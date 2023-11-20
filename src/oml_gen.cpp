
#include "oml_gen.hpp"

#include <fstream>
#include <iostream>

#include "db_util.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

/* Convert a OML type to a DuckDB LogicalType */
LogicalType getLogicalType(const std::string &type) {
    if (type == "string") {
        return LogicalType::VARCHAR;
    } else if (type == "uint32") {
        return LogicalType::INTEGER;
    } else if (type == "int32") {
        return LogicalType::INTEGER;
    } else if (type == "double") {
        return LogicalType::FLOAT;
    }

    throw InternalException("OML type " + type + " not recognized.");
}

/* Take a string value 'parsed_val' parsed from OML, and convert it to duckdb::Value
   using the correct strategy, determined by 'type' */
duckdb::Value convertToValue(const std::string &parsed_val, LogicalType &type) {
    if (type == LogicalType::VARCHAR) {
        return Value(parsed_val);
    } else if (type == LogicalType::FLOAT) {
        return Value::FLOAT((std::stof(parsed_val)));
    } else if (type == LogicalType::INTEGER) {
        return Value::INTEGER((std::stoi(parsed_val)));
    }

    throw InternalException("LogicalType " + type.ToString() + " not recognized.");
}

/* Split a string 's' using a 'delimiter' and return a vector with the resulting strings */
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
    result->catalog = "";  // default main-memory catalog is ""
    result->schema = "main";

    std::ifstream file(filename);
    std::string line;
    std::string tableName;
    vector<string> columnNames;
    vector<LogicalType> columnTypes;

    // parse OML metadata to read table name, column names, and column types
    for (int i = 0; i < 8; ++i) {
        std::getline(file, line);
        // If the line does not have a '\r' character at the end, add it
        if (!line.empty() && line.back() != '\r') {
            line.push_back('\r');
        }
        if (line.substr(0, 6) == "schema") {
            auto parts = split(line, ' ');
            parts.pop_back();
            if (parts[2] != "_experiment_metadata") {
                // second schema contains table name
                tableName = parts[2];
            }  // rest of line contains 'name:type' pairs
            for (size_t j = 3; j < parts.size(); ++j) {
                auto columnParts = split(parts[j], ':');
                columnNames.push_back(columnParts[0]);
                columnTypes.push_back(getLogicalType(columnParts[1]));
            }
        }
    }

    result->table = tableName;
    result->column_types = columnTypes;
    result->column_names = columnNames;
    result->not_null_constraint = {};

    // define output column names and types - should just output the amount of inserted tuples
    return_types = {LogicalType::INTEGER};
    names = {"amount of tuples inserted in '" + tableName + "'"};

    file.close();
    return std::move(result);
}

inline void OmlGen(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
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

    std::string line;  // buffer for a line

    // Skip the initial metadata
    for (int i = 0; i < 9; i++) {
        if (!std::getline(file, line)) {
            throw InternalException("File doesn't contain the expected metadata");
        }
    }

    idx_t row_count = 0;
    idx_t column_amount = bind_data.column_types.size();
    while (std::getline(file, line)) {
        // If the line does not have a '\r' character at the end, add it
        if (!line.empty() && line.back() != '\r') {
            line.push_back('\r');
        }
        std::istringstream iss(line);     // split line
        std::string field;                // buffer for a field
        std::vector<std::string> fields;  // buffer for vector of all fields from line
        // read individual space-separated fields ('field') from the line and append them to the fields vector.
        while (iss >> field) {
            fields.push_back(field);
        }

        if (fields.size() == column_amount) {
            vector<duckdb::Value> data;
            for (idx_t i = 0; i < column_amount; i++) {
                // convert parsed value to duckdb::Value and insert it into data
                data.push_back(convertToValue(fields[i], bind_data.column_types[i]));
            }

            // append row to table
            AppendData(context, bind_data, catalog, tbl_catalog, data);

            // increment row count
            row_count++;
        }
    }

    output.SetValue(0, 0, Value::INTEGER(row_count));
    output.SetCardinality(1);
    bind_data.finished_reading = true;
    file.close();
}

TableFunction GetOmlGenTableFunction() {
    TableFunction oml_gen = TableFunction("OmlGen", {LogicalType::VARCHAR}, OmlGen, OmlGenBind);
    return oml_gen;
}

}  // namespace duckdb