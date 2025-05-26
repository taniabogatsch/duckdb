//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/prefix.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

class ConstPrefixHandle {
public:

	optional_idx GetMismatchWithKey(ART &art, const ARTKey &key, idx_t &depth);

private:
	SegmentHandle handle;
	const data_t *data_ptr;
	const Node *child_ptr;
};

} // namespace duckdb