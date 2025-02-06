#include "duckdb.hpp"

#include "iceberg_storage.hpp"
#include "storage/iceberg_catalog.hpp"
#include "storage/iceberg_transaction_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

static unique_ptr<Catalog> IcebergAttach(StorageExtensionInfo *storage_info, ClientContext &context,
                                        AttachedDatabase &db, const string &name, AttachInfo &info,
                                        AccessMode access_mode) {
	/*SQLiteOpenOptions options;
	options.access_mode = access_mode;
	for (auto &entry : info.options) {
		if (StringUtil::CIEquals(entry.first, "busy_timeout")) {
			options.busy_timeout = entry.second.GetValue<uint64_t>();
		} else if (StringUtil::CIEquals(entry.first, "journal_mode")) {
			options.journal_mode = entry.second.ToString();
		}
	}*/
    printf("ICEBERG ATTACH\n");
	return make_uniq<IcebergCatalog>(db); //, info.path, std::move(options));
}

static unique_ptr<TransactionManager> IcebergCreateTransactionManager(StorageExtensionInfo *storage_info,
                                                                     AttachedDatabase &db, Catalog &catalog) {
	printf("ICEBERG CREATE TRANSACTION MANAGER\n");
	auto &iceberg_catalog = catalog.Cast<IcebergCatalog>();
	return make_uniq<IcebergTransactionManager>(db, iceberg_catalog);
}

IcebergStorageExtension::IcebergStorageExtension() {
	printf("ICEBERG STORAGE EXTENSION\n");
	attach = IcebergAttach;
	create_transaction_manager = IcebergCreateTransactionManager;
}

} // namespace duckdb