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

enum class ErrorSeverityType : uint8_t { USER = 0, INTERNAL = 1, FATAL = 2, SEGMENTATION_VIOLATION = 3 };

} // namespace duckdb