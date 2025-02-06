
#include "storage/iceberg_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

IcebergTransactionManager::IcebergTransactionManager(AttachedDatabase &db_p, IcebergCatalog &iceberg_catalog)
    : TransactionManager(db_p), iceberg_catalog(iceberg_catalog) {
}

Transaction &IcebergTransactionManager::StartTransaction(ClientContext &context) {
    /*
	auto transaction = make_uniq<IcebergTransaction>(iceberg_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
    */
	auto transaction = make_uniq<Transaction>(*this, context);
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData IcebergTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	//auto &sqlite_transaction = transaction.Cast<SQLiteTransaction>();
	//transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void IcebergTransactionManager::RollbackTransaction(Transaction &transaction) {
	//auto &sqlite_transaction = transaction.Cast<SQLiteTransaction>();
	//sqlite_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void IcebergTransactionManager::Checkpoint(ClientContext &context, bool force) {
    /*
	auto &transaction = SQLiteTransaction::Get(context, db.GetCatalog());
	auto &db = transaction.GetDB();
	db.Execute("PRAGMA wal_checkpoint");
    */
}

} // namespace duckdb