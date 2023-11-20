
#include "oml_load.hpp"

#include <fstream>
#include <iostream>

#include "db_util.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

namespace duckdb {

void CreateSequence(ClientContext &context, Catalog &catalog, string schema) {
    auto seq_info = make_uniq<CreateSequenceInfo>();
    // CreateInfo
    seq_info->schema = schema;
    seq_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
    seq_info->temporary = false;
    // CreateSequenceInfo
    seq_info->name = "Power_Consumption_id_seq";
    seq_info->usage_count = UINT64_MAX;
    seq_info->increment = 1;
    seq_info->min_value = 0;
    seq_info->max_value = INT64_MAX;
    seq_info->start_value = 0;

    catalog.CreateSequence(context, *seq_info);
}

void CreateOmlLoadView(ClientContext &context, Catalog &catalog, string schema) {
    auto view_info = make_uniq<CreateViewInfo>();
    view_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
    view_info->temporary = false;
    view_info->view_name = "PC";

    string sql =
        "CREATE VIEW PC AS (SELECT nextval('power_consumption_id_seq') AS id, "
        "cast(time_sec AS real) + cast(time_usec AS real) AS ts, "
        "power, current, voltage "
        "FROM power_consumption);";

    view_info = view_info->FromCreateView(context, sql);
    view_info->schema = schema;

    catalog.CreateView(context, *view_info);
}

inline unique_ptr<FunctionData> OmlLoadBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
    // expected input types
    if (input.inputs.size() != 1 || input.inputs[0].type().id() != LogicalTypeId::VARCHAR) {
        throw BinderException("Power_Consumption_load requires a single VARCHAR argument");
    }

    // bind inputs
    auto result = make_uniq<BaseOMLData>();
    result->file = StringValue::Get(input.inputs[0]);
    result->finished_reading = false;
    result->catalog = "";  // default main-memory catalog is ""
    result->schema = "main";
    result->table = "Power_Consumption";

    // define types for data chunk
    vector<LogicalType> column_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                        LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::FLOAT,
                                        LogicalType::FLOAT, LogicalType::FLOAT};
    vector<string> column_names = {"experiment_id", "node_id", "node_id_seq", "time_sec", "time_usec", "power", "current", "voltage"};
    result->column_types = column_types;
    result->column_names = column_names;
    result->not_null_constraint = {false, false, false, true, true, true, true, true};

    // define output column names and types - should just output the amount of inserted tuples
    return_types = {LogicalType::INTEGER};
    names = {"amount of tuples inserted in 'Power_Consumption'"};

    return std::move(result);
}

inline void OmlLoad(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    // extract bind data
    auto &bind_data = data_p.bind_data->CastNoConst<BaseOMLData>();

    // return if we are finished reading
    if (bind_data.finished_reading)
        return;

    // open OML file
    std::ifstream file(bind_data.file);

    if (!file.is_open()) {
        throw InternalException("Could not open file");
    }

    // create table and obtain table catalog entry
    CreateTable(context, bind_data);
    auto &catalog = Catalog::GetCatalog(context, bind_data.catalog);
    auto &tbl_catalog = catalog.GetEntry<TableCatalogEntry>(context, bind_data.schema, bind_data.table);

    std::string line;  // buffer for a line

    // Skip the initial metadata
    for (int i = 0; i < 9; i++) {
        if (!std::getline(file, line)) {
            throw InternalException("File doesn't contain the expected metadata");
        }
    }

    idx_t row_count = 0;
    while (std::getline(file, line)) {
        // If the line does not have a '\r' character at the end, add it
        if (!line.empty() && line.back() != '\r') {
            line.push_back('\r');
        }
        std::istringstream iss(line);     // split line
        std::string field;                // buffer for a field
        std::vector<std::string> fields;  // buffer for vector of all fields from line
        // read individual space-separated fields ('field') from the line and append them to the fields vector.
        while (iss >> field) {
            fields.push_back(field);
        }

        if (fields.size() == 8) {
            // insert parsed data into vector
            vector<duckdb::Value> data = {Value(fields[0]), Value(fields[1]), Value(fields[2]),
                                          Value(fields[3]), Value(fields[4]), Value::FLOAT(std::stof(fields[5])),
                                          Value::FLOAT(std::stof(fields[6])), Value::FLOAT(std::stof(fields[7]))};
            // append row to table
            AppendData(context, bind_data, catalog, tbl_catalog, data);

            // increment row count
            row_count++;
        }
    }

    output.SetValue(0, 0, Value::INTEGER(row_count));
    output.SetCardinality(1);

    // create sequence and view of data.
    CreateSequence(context, catalog, bind_data.schema);
    CreateOmlLoadView(context, catalog, bind_data.schema);

    bind_data.finished_reading = true;
    file.close();
}

TableFunction GetOmlLoadTableFunction() {
    TableFunction oml_power_consumption_load = TableFunction("Power_Consumption_load", {LogicalType::VARCHAR}, OmlLoad, OmlLoadBind);
    return oml_power_consumption_load;
}

}  // namespace duckdb