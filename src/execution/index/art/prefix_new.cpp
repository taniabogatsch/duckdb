#include "duckdb/execution/index/art/prefix_new.hpp"

#include "duckdb/execution/index/art/art_key.hpp"

namespace duckdb {

optional_idx ConstPrefixHandle::GetMismatchWithKey(ART &art, const ARTKey &key, idx_t &depth) {
	for (idx_t i = 0; i < data_ptr[art.prefix_count]; i++) {
		if (data_ptr[i] != key[depth]) {
			return i;
		}
		depth++;
	}
	return optional_idx::Invalid();
}

} // namespace duckdb