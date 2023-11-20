#define DUCKDB_EXTENSION_MAIN

#include "oml_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "oml_gen.hpp"
#include "oml_load.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
    // Register OML load table function
    TableFunction oml_power_consumption_load = GetOmlLoadTableFunction();
    ExtensionUtil::RegisterFunction(instance, oml_power_consumption_load);

    // Register OML gen table function
    TableFunction oml_gen = GetOmlGenTableFunction();
    ExtensionUtil::RegisterFunction(instance, oml_gen);
}

void OmlExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

std::string OmlExtension::Name() {
    return "oml";
}

}  // namespace duckdb

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
