#include "core_functions/scalar/debug_functions.hpp"

#include "duckdb/common/enum_util.hpp"

namespace duckdb {

inline void ChaosExceptionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &message_col = args.data[0];
	auto &exception_type_col = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, uint8_t>(message_col, exception_type_col, result, args.size(),
	                                                     [&](string_t message, string_t exception_type_str) -> uint8_t {
		                                                     auto exception_type = EnumUtil::FromString<ExceptionType>(
		                                                         exception_type_str.GetString());
		                                                     throw Exception(exception_type, message.GetString());
	                                                     });
}

ScalarFunction ChaosExceptionFun::GetFunction() {
	auto exception_fun =
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::UTINYINT, ChaosExceptionFunction);
	exception_fun.stability = FunctionStability::VOLATILE;
	BaseScalarFunction::SetReturnsError(exception_fun);
	return exception_fun;
}

} // namespace duckdb
