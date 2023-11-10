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

void ParseOML(ClientContext &context, std::ifstream &file, DataChunk &output) {
    // parse line of OML
    std::string line;                // buffer for a line
    std::getline(file, line);        // read line and store in buffer
    std::istringstream iss(line);    // split line
    std::string field;               // buffer for a field
    std::vector<std::string> fields; // buffer for vector of all fields from line
    // read individual space-separated fields ('field') from the line and append them to the fields vector.
    while (iss >> field) {
        fields.push_back(field);
    }
    // insert field values into output
    if (fields.size() == 8) {
        output.SetValue(0, 0, duckdb::Value(fields[0]));
        output.SetValue(0, 1, duckdb::Value(fields[1]));
        output.SetValue(0, 2, duckdb::Value(fields[2]));
        output.SetValue(0, 3, duckdb::Value(fields[3]));
        output.SetValue(0, 4, duckdb::Value(fields[4]));
        output.SetValue(0, 5, duckdb::Value(std::stof(fields[5])));
        output.SetValue(0, 6, duckdb::Value(std::stof(fields[6])));
        output.SetValue(0, 7, duckdb::Value(std::stof(fields[7])));

        output.SetCardinality(output.size());

        // insert values into table
        char query[1024];
        std::sprintf(query, "INSERT INTO Power_Consumption VALUES ('%s', '%s', '%s', '%s', '%s', '%f', '%f', '%f')",
                     fields[0].c_str(), fields[1].c_str(), fields[2].c_str(), fields[3].c_str(), fields[4].c_str(),
                     std::stof(fields[5]), std::stof(fields[6]), std::stof(fields[7]));

        context.Query(query, false);
    }
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
    CreateTable(context);

    // parse the OML file
    ParseOML(context, file, output);
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
