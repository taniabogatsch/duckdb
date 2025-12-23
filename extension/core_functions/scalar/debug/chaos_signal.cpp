#include "core_functions/scalar/debug_functions.hpp"

#include <signal.h>

namespace duckdb {

enum class SignalType : uint8_t { SIGNAL_SIGSEGV = 0, SIGNAL_SIGABRT = 1, SIGNAL_SIGBUS = 2 };

static SignalType SignalTypeFromString(const string &signal_type_str) {
	if (StringUtil::CIEquals(signal_type_str, "SIGSEGV")) {
		return SignalType::SIGNAL_SIGSEGV;
	}
	if (StringUtil::CIEquals(signal_type_str, "SIGABRT")) {
		return SignalType::SIGNAL_SIGABRT;
	}
	if (StringUtil::CIEquals(signal_type_str, "SIGBUS")) {
		return SignalType::SIGNAL_SIGBUS;
	}
	throw InvalidInputException("Invalid signal type: %s. Valid signal types are: SIGSEGV, SIGABRT, and SIGBUS.",
	                            signal_type_str);
}

inline void ChaosSignalFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &signal_col = args.data[0];
	UnaryExecutor::Execute<string_t, uint8_t>(
	    signal_col, result, args.size(), [&](string_t signal_type_str) -> uint8_t {
#ifndef _WIN32
		    auto signal_type = SignalTypeFromString(signal_type_str.GetString());
		    switch (signal_type) {
		    case SignalType::SIGNAL_SIGSEGV:
			    raise(SIGSEGV);
		    case SignalType::SIGNAL_SIGABRT:
			    raise(SIGABRT);
		    case SignalType::SIGNAL_SIGBUS:
			    raise(SIGBUS);
		    }
		    throw InternalException("missing case for signal type: %s", signal_type_str.GetString());
#else
    throw Exception(ExceptionType::NOT_IMPLEMENTED, "signals are not supported for Windows");
#endif
	    });
}

ScalarFunction ChaosSignalFun::GetFunction() {
	auto signal_fun = ScalarFunction({LogicalType::VARCHAR}, LogicalType::UTINYINT, ChaosSignalFunction);
	signal_fun.stability = FunctionStability::VOLATILE;
	BaseScalarFunction::SetReturnsError(signal_fun);
	return signal_fun;
}

} // namespace duckdb
