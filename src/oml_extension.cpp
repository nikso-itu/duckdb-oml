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
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include <fstream>
#include <iostream>

namespace duckdb {

struct oml_append_information {
    duckdb::unique_ptr<InternalAppender> appender;
};

void append_oml_chunk(oml_append_information &info, DataChunk &data) {
    auto &append_info = info.appender;
    append_info->AppendDataChunk(data);
}

void AppendData(ClientContext &context, TableFunctionInput &data_p, DataChunk &data) {
    auto &bind_data = data_p.bind_data->CastNoConst<BaseOMLData>();
    auto &catalog = Catalog::GetCatalog(context, bind_data.catalog);

    auto &tbl_catalog = catalog.GetEntry<TableCatalogEntry>(context, bind_data.schema, bind_data.table);
    auto append_info = make_uniq<oml_append_information>();
    append_info->appender = make_uniq<InternalAppender>(context, tbl_catalog);

    append_oml_chunk(*append_info, data);

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
        if (bind_data.not_null_constraint[i])
            info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(i)));
    }
    auto &catalog = Catalog::GetCatalog(context, bind_data.catalog);
    catalog.CreateTable(context, std::move(info));
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

    // define output column names and types
    return_types = column_types;
    names = column_names;

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

    // create table if it doesn't exist
    CreateTable(context, bind_data);

    std::string line; // buffer for a line

    // Skip the initial metadata
    for (int i = 0; i < 9; i++) {
        if (!std::getline(file, line)) {
            throw InternalException("File doesn't contain the expected metadata");
        }
    }

    // initialize data chunk for saving parsed oml data
    DataChunk chunk;
    chunk.Initialize(context, bind_data.column_types);

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
            // insert field values into data chunk
            idx_t varchar_amount = 5;
            idx_t float_amount = 3;

            // insert VARCHAR's
            for (idx_t i = 0; i < varchar_amount; i++) {
                chunk.SetValue(i, row_count, Value(fields[i]));
                output.SetValue(i, row_count, Value(fields[i]));
            }

            // insert FLOAT's
            for (idx_t i = varchar_amount; i < (varchar_amount + float_amount); i++) {
                chunk.SetValue(i, row_count, Value::FLOAT(std::stof(fields[i])));
                output.SetValue(i, row_count, Value::FLOAT(std::stof(fields[i])));
            }

            // increment row count
            row_count++;
        }
    }

    chunk.SetCardinality(row_count);
    output.SetCardinality(row_count);

    // insert values into table
    AppendData(context, data_p, chunk);

    bind_data.finished_reading = true;
    file.close();
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register table function
    auto oml_power_consumption_load = TableFunction("Power_Consumption_load", {LogicalType::VARCHAR}, OmlLoad, OmlLoadBind);
    ExtensionUtil::RegisterFunction(instance, oml_power_consumption_load);
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
