//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/warning_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

typedef void (*warning_callback_t)(const string &msg);

class WarningManager {
public:
	void SetWarningCallback(const warning_callback_t &callback_p) {
		callback = make_uniq<warning_callback_t>(callback_p);
	}
	void InvokeWarning(const string &msg) {
		if (callback) {
			(*callback)("WARNING: " + msg);
		}
	}

private:
	unique_ptr<warning_callback_t> callback;
};

} // namespace duckdb
