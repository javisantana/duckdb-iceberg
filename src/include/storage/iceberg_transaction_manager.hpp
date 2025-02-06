#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/iceberg_catalog.hpp"
//#include "storage/sqlite_transaction.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

class IcebergTransactionManager : public TransactionManager {
public:
	IcebergTransactionManager(AttachedDatabase &db_p, IcebergCatalog &iceberg_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	IcebergCatalog &iceberg_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<Transaction>> transactions;
};

} // namespace duckdb