//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/fixed_size_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/memory_safety.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/arena_allocator.hpp"

namespace duckdb {

template <class T, bool SAFE = true>
class fixed_size_vector {
public:
	fixed_size_vector() = delete;
	fixed_size_vector(ArenaAllocator &arena, idx_t capacity) : len(0), capacity(capacity) {
		ptr = reinterpret_cast<T *>(arena.AllocateAligned(capacity * sizeof(T)));
	}

	~fixed_size_vector() {
		for (idx_t i = 0; i < len; i++) {
			ptr[i].~T();
		}
	}

	//	fixed_size_vector(const fixed_size_vector &other) = delete;
	fixed_size_vector(fixed_size_vector &&other) noexcept : ptr(other.ptr), len(other.len), capacity(other.capacity) {
		other.ptr = nullptr;
	};

	//	fixed_size_vector& operator=(const fixed_size_vector &other) = delete;
	//	fixed_size_vector& operator=(fixed_size_vector &&other) = delete;

public:
	void push_back(const T &ref) {
		emplace_at(len++, ref);
	}
	void push_back(T &&ref) {
		emplace_at(len++, std::move(ref));
	}

	idx_t size() {
		return len;
	}

	using iterator = T *;
	using const_iterator = const T *;
	iterator begin() {
		return ptr;
	}
	const_iterator cbegin() {
		return ptr;
	}
	iterator end() {
		return ptr + len;
	}
	const_iterator cend() {
		return ptr + len;
	}

	T &operator[](idx_t index) {
		if (MemorySafety<SAFE>::ENABLED && index >= len) {
			throw InternalException("index out of range");
		}
		return ptr[index];
	}

	const T &operator[](idx_t index) const {
		if (MemorySafety<SAFE>::ENABLED && index >= len) {
			throw InternalException("index out of range");
		}
		return ptr[index];
	}

private:
	T *ptr;
	idx_t len;
	idx_t capacity;

private:
	template <class... ARGS>
	void emplace_at(idx_t index, ARGS &&... args) {
		if (MemorySafety<SAFE>::ENABLED && index >= len) {
			throw InternalException("index out of range");
		}
		new (ptr + index) T(std::forward<ARGS>(args)...);
	}
};

} // namespace duckdb
