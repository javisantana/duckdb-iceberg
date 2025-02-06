#include "storage/iceberg_catalog.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/storage/database_size.hpp"
//#include "storage/sqlite_schema_entry.hpp"
//#include "storage/sqlite_transaction.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"


namespace duckdb {

IcebergCatalog::IcebergCatalog(AttachedDatabase &db_p)
    : Catalog(db_p), in_memory(false), active_in_memory(false) {
	if (InMemory()) {
		//in_memory_db = SQLiteDB::Open(path, options, true);
	}
}

IcebergCatalog::~IcebergCatalog() {
}

void IcebergCatalog::Initialize(bool load_builtin) {
    printf("ICEBERG CATALOG INITIALIZE\n");
	CreateSchemaInfo info;
	main_schema = make_uniq<IcebergSchemaEntry>(*this, info);
}

optional_ptr<CatalogEntry> IcebergCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw BinderException("SQLite databases do not support creating new schemas");
}

void IcebergCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
    printf("ICEBERG SCAN SCHEMAS: %d\n", main_schema->catalog.IsDuckCatalog());
    callback(*main_schema);
}

optional_ptr<SchemaCatalogEntry> IcebergCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name,
                                                          OnEntryNotFound if_not_found,
                                                          QueryErrorContext error_context) {
	if (schema_name == DEFAULT_SCHEMA || schema_name == INVALID_SCHEMA) {
		return main_schema.get();
	}
	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return nullptr;
	}
	throw BinderException("SQLite databases only have a single schema - \"%s\"", DEFAULT_SCHEMA);
}

bool IcebergCatalog::InMemory() {
	return in_memory;
}

string IcebergCatalog::GetDBPath() {
	return path;
}

/*
SQLiteDB *SQLiteCatalog::GetInMemoryDatabase() {
	if (!InMemory()) {
		throw InternalException("GetInMemoryDatabase() called on a non-in-memory database");
	}
	lock_guard<mutex> l(in_memory_lock);
	if (active_in_memory) {
		throw TransactionException("Only a single transaction can be active on an "
		                           "in-memory SQLite database at a time");
	}
	active_in_memory = true;
	return &in_memory_db;
}

void SQLiteCatalog::ReleaseInMemoryDatabase() {
	if (!InMemory()) {
		return;
	}
	lock_guard<mutex> l(in_memory_lock);
	if (!active_in_memory) {
		throw InternalException("ReleaseInMemoryDatabase called but there is no "
		                        "active transaction on an in-memory database");
	}
	active_in_memory = false;
}
*/

void IcebergCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw BinderException("SQLite databases do not support dropping schemas");
}

DatabaseSize IcebergCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize result;

	//auto &transaction = SQLiteTransaction::Get(context, *this);
	//auto &db = transaction.GetDB();
	result.total_blocks = 0;
	result.block_size = 0;
	result.free_blocks = 0;
	result.used_blocks = 0;
	result.bytes = 0;
	result.wal_size = idx_t(-1);
	return result;
}




} // namespace duckdb
