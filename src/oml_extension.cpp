#define DUCKDB_EXTENSION_MAIN

#include "oml_extension.hpp"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void OmlPowerConsumptionLoad(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    // extract filename
    string filename = data_p.bind_data->Cast<string>();

    // create table if it doesnt exist

    // parse the OML file
    // insert results into columns of output: output.data[column_id] = data
    // use SetCardinality on the output to specify the expected number of rows
    // initialize the the DataChunk with the expected schema (column types)
    // insert into the output chunk by accessing the vector:
    // output.data[column].setValue(row, value)

    output.InitializeEmpty(vector<LogicalType>{
        LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
        LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::FLOAT,
        LogicalType::FLOAT, LogicalType::FLOAT});
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register table function
    auto oml_power_consumption_load = TableFunction("Power_Consumption_load", {LogicalType::VARCHAR}, OmlPowerConsumptionLoad);
    ExtensionUtil::RegisterFunction(instance, oml_power_consumption_load);
}

void OmlExtension::Load(DuckDB &db) { LoadInternal(*db.instance); }
std::string OmlExtension::Name() { return "oml"; }

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
