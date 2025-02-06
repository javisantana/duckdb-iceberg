
#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"

namespace duckdb {

class IcebergInsert : public PhysicalOperator {
public:
	//! INSERT INTO
	IcebergInsert(LogicalOperator &op, TableCatalogEntry &table, physical_index_vector_t<idx_t> column_index_map);
	//! CREATE TABLE AS
	IcebergInsert(LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info);

	//! The table to insert into
	optional_ptr<TableCatalogEntry> table;
	//! Table schema, in case of CREATE TABLE AS
	optional_ptr<SchemaCatalogEntry> schema;
	//! Create table info, in case of CREATE TABLE AS
	unique_ptr<BoundCreateTableInfo> info;
	//! column_index_map
	physical_index_vector_t<idx_t> column_index_map;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};






} // namespace duckdb