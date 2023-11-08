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
};

} // namespace duckdb
