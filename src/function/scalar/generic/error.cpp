#include "duckdb/function/scalar/generic_functions.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/function/scalar/error_function_utils.hpp"

#include <signal.h>

namespace duckdb {

namespace {

static void InvalidInputErrorFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnifiedVectorFormat message_col;
	args.data[0].ToUnifiedFormat(args.size(), message_col);
	auto message_col_data = UnifiedVectorFormat::GetData<string_t>(message_col);

	for (idx_t i = 0; i < args.size(); i++) {
		auto message_idx = message_col.sel->get_index(i);
		if (!message_col.validity.RowIsValid(message_idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		throw InvalidInputException(message_col_data[message_idx].GetString());
	}
}

static void ErrorFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnifiedVectorFormat message_col;
	args.data[0].ToUnifiedFormat(args.size(), message_col);
	auto message_col_data = UnifiedVectorFormat::GetData<string_t>(message_col);

	UnifiedVectorFormat severity_col;
	args.data[1].ToUnifiedFormat(args.size(), severity_col);
	auto severity_col_data = UnifiedVectorFormat::GetData<string_t>(severity_col);

	for (idx_t i = 0; i < args.size(); i++) {
		auto message_idx = message_col.sel->get_index(i);
		auto severity_idx = severity_col.sel->get_index(i);
		if (!message_col.validity.RowIsValid(message_idx) || !severity_col.validity.RowIsValid(severity_idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto severity_str = severity_col_data[severity_idx].GetString();
		auto severity = EnumUtil::FromString<ErrorFunType>(severity_str);
		auto message = message_col_data[message_idx].GetString();
		switch (severity) {
		case ErrorFunType::USER:
			throw InvalidInputException(message);
		case ErrorFunType::INTERNAL:
			throw InternalException(message);
		case ErrorFunType::FATAL:
			throw FatalException(message);
		case ErrorFunType::SIGNAL_SIGSEGV:
			raise(SIGSEGV);
			break;
		case ErrorFunType::SIGNAL_SIGABRT:
			raise(SIGABRT);
			break;
		case ErrorFunType::SIGNAL_SIGBUS:
			raise(SIGBUS);
			break;
		}
	}
}

} // anonymous namespace

ScalarFunctionSet ErrorFun::GetFunctions() {
	ScalarFunctionSet error_fun_set("error");

	ScalarFunction invalid_input_error_fun({LogicalType::VARCHAR}, LogicalType::SQLNULL, InvalidInputErrorFunction);
	// Set the function with side effects to avoid the optimization.
	invalid_input_error_fun.stability = FunctionStability::VOLATILE;
	BaseScalarFunction::SetReturnsError(invalid_input_error_fun);
	error_fun_set.AddFunction(invalid_input_error_fun);

	ScalarFunction error_fun({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::SQLNULL, ErrorFunction);
	// Set the function with side effects to avoid the optimization.
	error_fun.stability = FunctionStability::VOLATILE;
	BaseScalarFunction::SetReturnsError(error_fun);
	error_fun_set.AddFunction(error_fun);

	return error_fun_set;
}

} // namespace duckdb
