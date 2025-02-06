#include "storage/iceberg_schema_entry.hpp"
#include "storage/iceberg_table_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
namespace duckdb {

IcebergSchemaEntry::IcebergSchemaEntry(Catalog &catalog, CreateSchemaInfo &info) : SchemaCatalogEntry(catalog, info) {
}


optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {

	/*auto &sqlite_transaction = GetSQLiteTransaction(transaction);
	auto &base_info = info.Base();
	auto table_name = base_info.table;
	if (base_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// CREATE OR REPLACE - drop any existing entries first (if any)
		TryDropEntry(transaction.GetContext(), CatalogType::TABLE_ENTRY, table_name);
	}

	sqlite_transaction.GetDB().Execute(GetCreateTableSQL(base_info));
	return GetEntry(transaction, CatalogType::TABLE_ENTRY, table_name);
    */
	throw BinderException("ICEBERG does not support creating tables");
   //return nullptr;

}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("SQLite databases do not support creating functions");
}


optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                          TableCatalogEntry &table) {
	throw BinderException("ICEBERG does not support creating index");
}


optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw BinderException("ICEBERG does not support creating index");
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw BinderException("ICEBERG does not support creating sequences");
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                  CreateTableFunctionInfo &info) {
	throw BinderException("ICEBERG does not support creating table functions");
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                 CreateCopyFunctionInfo &info) {
	throw BinderException("ICEBERG does not support creating copy functions");
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                   CreatePragmaFunctionInfo &info) {
	throw BinderException("ICEBERG does not support creating pragma functions");
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                              CreateCollationInfo &info) {
	throw BinderException("ICEBERG does not support creating collations");
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("ICEBERG does not support creating types");
}

void IcebergSchemaEntry::Alter(CatalogTransaction catalog_transaction, AlterInfo &info) {
	throw BinderException("ICEBERG does not support altering tables");
}

void IcebergSchemaEntry::Scan(ClientContext &context, CatalogType type,
                             const std::function<void(CatalogEntry &)> &callback) {

    auto transaction = GetCatalogTransaction(context);
    auto entry = GetEntry(transaction, type, "lineitem_iceberg");
	callback(*entry);
    auto entry2 = GetEntry(transaction, type, "lineitem_iceberg_2");
	callback(*entry2);
}
void IcebergSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw InternalException("Scan");
}

void IcebergSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::GetEntry(CatalogTransaction transaction, CatalogType type,
                                                       const string &name) {

    printf("ICEBERG GET ENTRY\n");
    CreateTableInfo info(*this, name);
    return make_uniq<IcebergTableEntry>(catalog, *this, info, false).get();
}

} // namespace duckdb