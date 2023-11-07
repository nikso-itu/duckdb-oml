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

    context.Query(create_table_query, false);
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
        output.SetValue(0, 0, fields[0]);
        output.SetValue(0, 1, fields[1]);
        output.SetValue(0, 2, fields[2]);
        output.SetValue(0, 3, fields[3]);
        output.SetValue(0, 4, fields[4]);
        output.SetValue(0, 5, std::stof(fields[5]));
        output.SetValue(0, 6, std::stof(fields[6]));
        output.SetValue(0, 7, std::stof(fields[7]));

        // insert values into table
        char query[1024];
        std::sprintf(query, "INSERT INTO Power_Consumption VALUES ('%s', '%s', '%s', '%s', '%s', '%f', '%f', '%f')",
                     fields[0].c_str(), fields[1].c_str(), fields[2].c_str(), fields[3].c_str(), fields[4].c_str(),
                     std::stof(fields[5]), std::stof(fields[6]), std::stof(fields[7]));

        context.Query(query, false);
    }
}

inline void OmlPowerConsumptionLoad(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    // initialize the the DataChunk with the expected schema (column types)
    output.InitializeEmpty(vector<LogicalType>{
        LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
        LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::FLOAT,
        LogicalType::FLOAT, LogicalType::FLOAT});

    // extract filename and open OML file
    std::string filename = data_p.bind_data->Cast<string>();
    std::ifstream file(filename);

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
    auto oml_power_consumption_load = TableFunction("Power_Consumption_load", {LogicalType::VARCHAR}, OmlPowerConsumptionLoad);
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
