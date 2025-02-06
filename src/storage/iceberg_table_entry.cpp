
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "storage/iceberg_table_entry.hpp"

#include "iceberg_functions.hpp"

namespace duckdb {




IcebergScanFunction::IcebergScanFunction()
    : TableFunction("icebeg_scan", {LogicalType::VARCHAR}, nullptr, nullptr, IcebergScanGlobalTableFunctionState::Init) {
	bind_replace = IcebergScanBindReplace;
	named_parameters["skip_schema_inference"] = LogicalType::BOOLEAN;
	named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	named_parameters["mode"] = LogicalType::VARCHAR;
	named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	named_parameters["version"] = LogicalType::VARCHAR;
	named_parameters["version_name_format"] = LogicalType::VARCHAR;
}

IcebergTableEntry::IcebergTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                   bool all_varchar)
    : TableCatalogEntry(catalog, schema, info), all_varchar(all_varchar) {
}

unique_ptr<BaseStatistics> IcebergTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void IcebergTableEntry::BindUpdateConstraints(Binder &, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                             ClientContext &) {
}

TableFunction IcebergTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto result = make_uniq<IcebergBindData>();
    /*
	for (auto &col : columns.Logical()) {
		result->names.push_back(col.GetName());
		result->types.push_back(col.GetType());
	}
	auto &sqlite_catalog = catalog.Cast<SQLiteCatalog>();
	result->file_name = sqlite_catalog.path;
	result->table_name = name;
	result->all_varchar = all_varchar;

	auto &transaction = Transaction::Get(context, catalog).Cast<SQLiteTransaction>();
	auto &db = transaction.GetDB();

	if (!db.GetRowIdInfo(name, result->row_id_info)) {
		result->rows_per_group = optional_idx();
	}
	if (!transaction.IsReadOnly() || sqlite_catalog.InMemory()) {
		// for in-memory databases or if we have transaction-local changes we can
		// only do a single-threaded scan set up the transaction's connection object
		// as the global db
		result->global_db = &db;
		result->rows_per_group = optional_idx();
	}*/
	result->table = this;

	bind_data = std::move(result);
	return static_cast<TableFunction>(IcebergScanFunction());
}


TableStorageInfo IcebergTableEntry::GetStorageInfo(ClientContext &context) {
    /*
	auto &transaction = Transaction::Get(context, catalog).Cast<SQLiteTransaction>();
	auto &db = transaction.GetDB();
	TableStorageInfo result;

	RowIdInfo info;
	if (!db.GetRowIdInfo(name, info)) {
		// probably
		result.cardinality = 10000;
	} else {
		result.cardinality = info.max_rowid.GetIndex() - info.min_rowid.GetIndex();
	}

	result.index_info = db.GetIndexInfo(name);
	return result;
    */
	TableStorageInfo result;
    return result;
}

} // namespace duckdb
