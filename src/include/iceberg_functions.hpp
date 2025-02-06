//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {


struct IcebergScanGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<GlobalTableFunctionState>();
	}
};

unique_ptr<TableRef> IcebergScanBindReplace(ClientContext &context, TableFunctionBindInput &input);

class IcebergFunctions {
public:
	static vector<TableFunctionSet> GetTableFunctions();
	static vector<ScalarFunction> GetScalarFunctions();

private:
	static TableFunctionSet GetIcebergSnapshotsFunction();
	static TableFunctionSet GetIcebergScanFunction();
	static TableFunctionSet GetIcebergMetadataFunction();
};

} // namespace duckdb
