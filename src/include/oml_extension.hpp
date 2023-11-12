#pragma once

#include "duckdb.hpp"

namespace duckdb {

class OmlExtension : public Extension {
public:
    void Load(DuckDB &db) override;
    std::string Name() override;
};

struct BaseOMLData : public TableFunctionData {
    virtual ~BaseOMLData() {
    }
    // file path of the OML file to read
    string file;

    // whether the OML file is finished reading or not
    bool finished_reading;

    // catalog name
    string catalog;

    // schema name
    string schema;

    // table name
    string table;

    // table info
    vector<string> column_names;
    vector<LogicalType> column_types;
    vector<bool> not_null_constraint;
};

} // namespace duckdb
