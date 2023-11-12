#define DUCKDB_EXTENSION_MAIN

#include "oml_extension.hpp"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include <fstream>
#include <iostream>

namespace duckdb {

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

    // expected output schema (column types)
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                    LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::FLOAT,
                    LogicalType::FLOAT, LogicalType::FLOAT};
    names = {"experiment_id", "node_id", "node_id_seq", "time_sec", "time_usec", "power", "current", "voltage"};

    result->column_names = names;
    result->column_types = return_types;
    result->not_null_constraint = {false, false, false, true, true, true, true, true};

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
            // insert field values into output
            output.SetValue(0, row_count, Value(fields[0]));
            output.SetValue(1, row_count, Value(fields[1]));
            output.SetValue(2, row_count, Value(fields[2]));
            output.SetValue(3, row_count, Value(fields[3]));
            output.SetValue(4, row_count, Value(fields[4]));
            output.SetValue(5, row_count, Value::FLOAT(std::stof(fields[5])));
            output.SetValue(6, row_count, Value::FLOAT(std::stof(fields[6])));
            output.SetValue(7, row_count, Value::FLOAT(std::stof(fields[7])));

            // increment row count
            row_count++;

            // insert values into table
        }
    }

    output.SetCardinality(row_count);
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
