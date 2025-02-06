//===----------------------------------------------------------------------===//
//                         DuckDB
//
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "iceberg_schema_entry.hpp"

namespace duckdb {

class IcebergCatalog : public Catalog {
public:
	explicit IcebergCatalog(AttachedDatabase &db_p); //, const string &path, SQLiteOpenOptions options);
	~IcebergCatalog();

	string path;
	//SQLiteOpenOptions options;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "iceberg";
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> GetSchema(CatalogTransaction transaction, const string &schema_name,
	                                           OnEntryNotFound if_not_found,
	                                           QueryErrorContext error_context = QueryErrorContext()) override;

	//SQLiteSchemaEntry &GetMainSchema() { return *main_schema; }

	unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context, LogicalInsert &op,
	                                        unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
	                                               unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context, LogicalDelete &op,
	                                        unique_ptr<PhysicalOperator> plan) override {
		return plan; //make_uniq<PhysicalOperator>(PhysicalOperatorType::INVALID, op.types, 1);
                                            }
	unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context, LogicalUpdate &op,
	                                        unique_ptr<PhysicalOperator> plan) override {
		return plan;//make_uniq<PhysicalOperator>(PhysicalOperatorType::INVALID, op.types, 1);
	}
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override 
                                                {
                                                    return plan;
                                                }

                            

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	//! Whether or not this is an in-memory SQLite database
	bool InMemory() override;
	string GetDBPath() override;

	//! Returns a reference to the in-memory database (if any)
	//SQLiteDB *GetInMemoryDatabase();
	//! Release the in-memory database (if there is any)
	//void ReleaseInMemoryDatabase();

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

private:
	unique_ptr<IcebergSchemaEntry> main_schema;
	//! Whether or not the database is in-memory
	bool in_memory;
	//! In-memory database - if any
	//jSQLiteDB in_memory_db;
	//! The lock maintaing access to the in-memory database
	mutex in_memory_lock;
	//! Whether or not there is any active transaction on the in-memory database
	bool active_in_memory;
};

} // namespace duckdb
