
#include "db_util.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

struct oml_append_information {
    duckdb::unique_ptr<InternalAppender> appender;
};

void append_oml_chunk(oml_append_information &info, vector<duckdb::Value> &data) {
    // append each value from a vector, to the table
    auto &append_info = info.appender;
    append_info->BeginRow();
    for (const duckdb::Value value : data) {
        append_info->Append<duckdb::Value>(value);
    }
    append_info->EndRow();
}

void AppendData(ClientContext &context, BaseOMLData &bind_data, Catalog &catalog,
                TableCatalogEntry &tbl_catalog, vector<duckdb::Value> &data) {
    auto append_info = make_uniq<oml_append_information>();
    append_info->appender = make_uniq<InternalAppender>(context, tbl_catalog);

    // append data to table
    append_oml_chunk(*append_info, data);

    // flush any incomplete chunks
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
        if (bind_data.not_null_constraint.size() != 0 && bind_data.not_null_constraint[i])
            info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(i)));
    }
    auto &catalog = Catalog::GetCatalog(context, bind_data.catalog);
    catalog.CreateTable(context, std::move(info));
}

}  // namespace duckdb