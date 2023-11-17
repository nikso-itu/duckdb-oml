#define DUCKDB_EXTENSION_MAIN

#include "oml_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include <fstream>
#include <iostream>
#include <utility>

namespace duckdb {

struct oml_append_information {
    duckdb::unique_ptr<InternalAppender> appender;
};

template <typename Tuple, std::size_t... Is>
void append_oml_chunk(oml_append_information &info, const Tuple &data, std::index_sequence<Is...>) {
    // unpack a Tuple of heterogeneous values into the argument list, using a index sequence
    auto &append_info = info.appender;
    append_info->AppendRow(std::get<Is>(data)...);
}

template <typename... Ts>
void AppendData(ClientContext &context, BaseOMLData &bind_data, Catalog &catalog,
                TableCatalogEntry &tbl_catalog, const std::tuple<Ts...> &data) {
    auto append_info = make_uniq<oml_append_information>();
    append_info->appender = make_uniq<InternalAppender>(context, tbl_catalog);

    // append Tuple of data, and provide index sequence
    append_oml_chunk(*append_info, data, std::index_sequence_for<Ts...>{});

    // Flush any incomplete chunks
    append_info->appender->Flush();
    append_info->appender.reset();
}

void CreateTable(ClientContext &context, BaseOMLData &bind_data) {
    auto info = make_uniq<CreateTableInfo>();
    info->schema = bind_data.schema;
    info->table = bind_data.table;
    info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
    info->temporary = false;
    for (idx_t i = 0; i < bind_data.column_types.size(); i++) {
        info->columns.AddColumn(ColumnDefinition(bind_data.column_names[i], bind_data.column_types[i]));
        // if column has 'not null' constraint, set it
        if (bind_data.not_null_constraint.size() != 0 && bind_data.not_null_constraint[i])
            info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(i)));
    }
    auto &catalog = Catalog::GetCatalog(context, bind_data.catalog);
    catalog.CreateTable(context, std::move(info));
}

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

    string sql = "CREATE VIEW PC AS (SELECT nextval('power_consumption_id_seq') AS id, "
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
    result->catalog = ""; // default main-memory catalog is ""
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
    names = {"# tuples inserted in table 'Power_Consumption'"};

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

    std::string line; // buffer for a line

    // Skip the initial metadata
    for (int i = 0; i < 9; i++) {
        if (!std::getline(file, line)) {
            throw InternalException("File doesn't contain the expected metadata");
        }
    }

    idx_t row_count = 0;
    while (std::getline(file, line)) {
        std::istringstream iss(line);    // split line
        std::string field;               // buffer for a field
        std::vector<std::string> fields; // buffer for vector of all fields from line
        // // read individual space-separated fields ('field') from the line and append them to the fields vector.
        while (iss >> field) {
            fields.push_back(field);
        }

        if (fields.size() == 8) {
            // insert parsed data into tuple
            auto data = std::make_tuple(Value(fields[0]), Value(fields[1]), Value(fields[2]),
                                        Value(fields[3]), Value(fields[4]), Value::FLOAT(std::stof(fields[5])),
                                        Value::FLOAT(std::stof(fields[6])), Value::FLOAT(std::stof(fields[7])));
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

std::vector<std::string> split(const std::string &s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

inline unique_ptr<FunctionData> OmlGenBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
    // expected input types
    if (input.inputs.size() != 1 || input.inputs[0].type().id() != LogicalTypeId::VARCHAR) {
        throw BinderException("OmlGen requires a single VARCHAR argument");
    }

    // bind inputs
    auto result = make_uniq<BaseOMLData>();
    std::string filename = StringValue::Get(input.inputs[0]);
    result->file = filename;
    result->finished_reading = false;
    result->catalog = ""; // default main-memory catalog is ""
    result->schema = "main";
    
    std::ifstream file(filename);
    std::string line;
    std::string tableName;
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;

    for (int i = 0; i < 8; ++i) {
        std::getline(file, line);
        if (line.substr(0, 6) == "schema") {
            auto parts = split(line, ' ');
            if (parts[2] != "_experiment_metadata") {
                tableName = parts[2];
            }
            for (size_t j = 3; j < parts.size(); ++j) {
                auto columnParts = split(parts[j], ':');
                columnNames.push_back(columnParts[0]);
                // columnTypes.push_back(Value::'TYPE'(columnParts[1])); // Replace 'TYPE' with the appropriate conversion function
            }
        }
    }
    
    result->table = tableName;
    result->column_types = columnTypes;
    result->column_names = columnNames;
    result->not_null_constraint = {};

    // define output column names and types - should just output the amount of inserted tuples
    return_types = {LogicalType::INTEGER};
    names = {"# tuples inserted in table 'Power_Consumption'"};

    return std::move(result);
}

inline void OmlGen(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register OML load table function
    auto oml_power_consumption_load = TableFunction("Power_Consumption_load", {LogicalType::VARCHAR}, OmlLoad, OmlLoadBind);
    ExtensionUtil::RegisterFunction(instance, oml_power_consumption_load);

    // Register OML gen table function
    auto oml_gen = TableFunction("OmlGen", {LogicalType::VARCHAR}, OmlGen, OmlGenBind);
    ExtensionUtil::RegisterFunction(instance, oml_gen);
}

void OmlExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

std::string OmlExtension::Name() {
    return "oml";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void oml_init(duckdb::DatabaseInstance &db) {
    LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *oml_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
