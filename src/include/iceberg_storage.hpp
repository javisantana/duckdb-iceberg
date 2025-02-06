
#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class IcebergStorageExtension : public StorageExtension {
public:
	IcebergStorageExtension();
};

} // namespace duckdb
