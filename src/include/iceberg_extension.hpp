#pragma once

#include "duckdb.hpp"

namespace duckdb {

class IcebergExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};


extern "C" {
DUCKDB_EXTENSION_API void iceberg_init(duckdb::DatabaseInstance &db);
DUCKDB_EXTENSION_API const char *iceberg_version();
DUCKDB_EXTENSION_API void iceberg_scanner_storage_init(duckdb::DBConfig &config);
}

} // namespace duckdb
