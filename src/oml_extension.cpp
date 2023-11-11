#define DUCKDB_EXTENSION_MAIN

#include "oml_extension.hpp"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <fstream>
#include <iostream>

namespace duckdb {

void CreateTable(ClientContext &context) {
    // create the Power_Consumption table if it doesn't already exist.
    const string create_table_query = "CREATE TABLE IF NOT EXISTS Power_Consumption ("
                                      "experiment_id VARCHAR,"
                                      "node_id VARCHAR,"
                                      "node_id_seq VARCHAR,"
                                      "time_sec VARCHAR NOT NULL,"
                                      "time_usec VARCHAR NOT NULL,"
                                      "power REAL NOT NULL,"
                                      "current REAL NOT NULL,"
                                      "voltage REAL NOT NULL);";

    unique_ptr<QueryResult> result = context.Query(create_table_query, true); // Halts here

    // fail of query was not successful
    if (!result) {
        throw InternalException(result->ToString());
    };
}

inline unique_ptr<FunctionData> OmlPowerConsumptionLoadBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
    // expected input types
    if (input.inputs.size() != 1 || input.inputs[0].type().id() != LogicalTypeId::VARCHAR) {
        throw BinderException("Power_Consumption_load requires a single VARCHAR argument");
    }

    // bind inputs
    auto result = make_uniq<BaseOMLData>();
    result->file = StringValue::Get(input.inputs[0]);
    result->finished_reading = false;

    // expected output schema (column types)
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                    LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::FLOAT,
                    LogicalType::FLOAT, LogicalType::FLOAT};
    names = {"experiment_id", "node_id", "node_id_seq", "time_sec", "time_usec", "power", "current", "voltage"};

    return std::move(result);
}

inline void OmlPowerConsumptionLoad(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    // extract bind data
    auto &bind_data = data_p.bind_data->CastNoConst<BaseOMLData>();

    // open OML file
    std::ifstream file(bind_data.file);

    if (!file.is_open()) {
        throw InternalException("Could not open file");
    }

    // create table if it doesn't exist
    // CreateTable(context);

    // parse the OML file
    std::string line;                // buffer for a line
    std::getline(file, line);        // read line and store in buffer
    std::istringstream iss(line);    // split line
    std::string field;               // buffer for a field
    std::vector<std::string> fields; // buffer for vector of all fields from line
    // // read individual space-separated fields ('field') from the line and append them to the fields vector.
    while (iss >> field) {
        fields.push_back(field);
    }
    // insert field values into output
    idx_t row_count = 0;
    if (!bind_data.finished_reading && fields.size() == 8) {
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

        output.SetCardinality(row_count);

        bind_data.finished_reading = true;
    }

    file.close();
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register table function
    auto oml_power_consumption_load = TableFunction("Power_Consumption_load", {LogicalType::VARCHAR}, OmlPowerConsumptionLoad, OmlPowerConsumptionLoadBind);
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
