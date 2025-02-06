#define DUCKDB_EXTENSION_MAIN


#include "duckdb.hpp"
#include "iceberg_extension.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "iceberg_functions.hpp"
#include "yyjson.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "iceberg_storage.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {


#if 0
static void IcebergScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	printf("ICEBERG SCAN FUNCTION\n");
}

static unique_ptr<FunctionData> SqliteBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_uniq<SqliteBindData>();
	result->file_name = input.inputs[0].GetValue<string>();
	result->table_name = input.inputs[1].GetValue<string>();

	SQLiteDB db;
	SQLiteStatement stmt;
	SQLiteOpenOptions options;
	options.access_mode = AccessMode::READ_ONLY;
	db = SQLiteDB::Open(result->file_name, options);

	ColumnList columns;
	vector<unique_ptr<Constraint>> constraints;

	result->all_varchar = false;
	Value sqlite_all_varchar;
	if (context.TryGetCurrentSetting("sqlite_all_varchar", sqlite_all_varchar)) {
		result->all_varchar = BooleanValue::Get(sqlite_all_varchar);
	}
	db.GetTableInfo(result->table_name, columns, constraints, result->all_varchar);
	for (auto &column : columns.Logical()) {
		names.push_back(column.GetName());
		return_types.push_back(column.GetType());
	}

	if (names.empty()) {
		throw std::runtime_error("no columns for table " + result->table_name);
	}

	if (!db.GetRowIdInfo(result->table_name, result->row_id_info)) {
		result->rows_per_group = optional_idx();
	}

	result->names = names;
	result->types = return_types;

	return std::move(result);
}


class SqliteScanFunction : public TableFunction {
public:
	SqliteScanFunction();
	SqliteScanFunction::SqliteScanFunction()
		: TableFunction("sqlite_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR}, IcebergScan, SqliteBind,
						SqliteInitGlobalState, SqliteInitLocalState) {
		cardinality = SqliteCardinality;
		to_string = SqliteToString;
		get_bind_info = SqliteBindInfo;
		projection_pushdown = true;
	}

};

#endif 

struct AttachFunctionData : public TableFunctionData {
	AttachFunctionData() {
	}

	bool finished = false;
	bool overwrite = false;
	string path = "";
};

static unique_ptr<FunctionData> AttachBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_uniq<AttachFunctionData>();
	result->path = input.inputs[0].GetValue<string>();

	for (auto &kv : input.named_parameters) {
		if (kv.first == "overwrite") {
			result->overwrite = BooleanValue::Get(kv.second);
		}
	}

	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}



static void AttachFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<AttachFunctionData>();
	if (data.finished) {
		return;
	}
	printf("ICEBERG ATTACH FUNCTION: path: %s\n", data.path.c_str());
	auto dconn = Connection(context.db->GetDatabase(context));
	named_parameter_map_t options_map;
	options_map["allow_moved_paths"] = Value::BOOLEAN(true);
	dconn.TableFunction("iceberg_scan", {Value(data.path)}, options_map)->CreateView("icebeg_test", data.overwrite, false);

	/*SQLiteOpenOptions options;
	options.access_mode = AccessMode::READ_ONLY;
	SQLiteDB db = SQLiteDB::Open(data.file_name, options);
	auto dconn = Connection(context.db->GetDatabase(context));
	{
		auto tables = db.GetTables();
		for (auto &table_name : tables) {
			dconn.TableFunction("sqlite_scan", {Value(data.file_name), Value(table_name)})
			    ->CreateView(table_name, data.overwrite, false);
		}
	}
	{
		SQLiteStatement stmt = db.Prepare("SELECT sql FROM sqlite_master WHERE type='view'");
		while (stmt.Step()) {
			auto view_sql = stmt.GetValue<string>(0);
			dconn.Query(view_sql);
		}
	}*/
	data.finished = true;
}

class IcebergAttachFunction : public TableFunction {
public:
	IcebergAttachFunction()
		: TableFunction("iceberg_attach", {LogicalType::VARCHAR}, AttachFunction, AttachBind) {
		named_parameters["overwrite"] = LogicalType::BOOLEAN;
	}
};


static void LoadInternal(DatabaseInstance &instance) {
	printf("Starting LoadInternal\n");
	auto &config = DBConfig::GetConfig(instance);
	
	printf("Adding extension option\n");
	config.AddExtensionOption(
		"unsafe_enable_version_guessing",
		"Enable globbing the filesystem (if possible) to find the latest version metadata. This could result in reading an uncommitted version.",
		LogicalType::BOOLEAN,
		Value::BOOLEAN(false)
	);

	// Iceberg Table Functions
	printf("Registering table functions\n");
	for (auto &fun : IcebergFunctions::GetTableFunctions()) {
		printf("Registering table function\n");
		ExtensionUtil::RegisterFunction(instance, fun);
	}

	// Iceberg Scalar Functions
	printf("Registering scalar functions\n");
	for (auto &fun : IcebergFunctions::GetScalarFunctions()) {
		ExtensionUtil::RegisterFunction(instance, fun);
	}


	IcebergAttachFunction attach_func;
	ExtensionUtil::RegisterFunction(instance, attach_func);

	printf("LoadInternal complete\n");
}

void IcebergExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string IcebergExtension::Name() {
	return "iceberg";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void iceberg_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *iceberg_version() {
	return duckdb::DuckDB::LibraryVersion();
}

DUCKDB_EXTENSION_API void iceberg_storage_init(duckdb::DBConfig &config) {
	printf("ICEBERG SCANNER STORAGE INIT - start\n");
	try {
		printf("Creating storage extension\n");
		config.storage_extensions["iceberg"] = duckdb::make_uniq<duckdb::IcebergStorageExtension>();
		printf("Storage extension created successfully\n");
	} catch (const std::exception& e) {
		printf("Exception during storage init: %s\n", e.what());
		throw;
	} catch (...) {
		printf("Unknown exception during storage init\n");
		throw;
	}
	printf("ICEBERG SCANNER STORAGE INIT - complete\n");
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
