//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/error_function_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

enum class ErrorFunType : uint8_t {
	USER = 0,
	INTERNAL = 1,
	FATAL = 2,
	SIGNAL_SIGSEGV = 3,
	SIGNAL_SIGABRT = 4,
	SIGNAL_SIGBUS = 5
};

} // namespace duckdb
