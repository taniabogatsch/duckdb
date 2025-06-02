//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/index_pointer.hpp"

namespace duckdb {

// FIXME: we still need to get rid of all the Node references - instead, returning handles.

enum class NType : uint8_t {
	PREFIX = 1,
	LEAF = 2,
	NODE_4 = 3,
	NODE_16 = 4,
	NODE_48 = 5,
	NODE_256 = 6,
	LEAF_INLINED = 7,
	NODE_7_LEAF = 8,
	NODE_15_LEAF = 9,
	NODE_256_LEAF = 10,
};

enum class GateStatus : uint8_t {
	GATE_NOT_SET = 0,
	GATE_SET = 1,
};

class ART;
class Prefix;
class ARTKey;

//! The Node is the pointer class of the ART index.
//! It inherits from the IndexPointer, and adds ART-specific functionality.
class Node : public IndexPointer {
	friend class Prefix;

public:
	//! A gate sets the leftmost bit of the metadata, binary: 1000-0000.
	static constexpr uint8_t AND_GATE = 0x80;
	static constexpr idx_t AND_ROW_ID = 0x00FFFFFFFFFFFFFF;

public:
	//! Get a new pointer to a node and initialize it.
	static void New(ART &art, Node &node, const NType type);
	//! Free the node and its children.
	//! FIXME: remove recursion.
	static void Free(ART &art, Node &node);

	//! Get a reference to the allocator.
	static FixedSizeAllocator &GetAllocator(const ART &art, const NType type);
	//! Get the index of a node type's allocator.
	static uint8_t GetAllocatorIdx(const NType type);

	//! Replace the child at byte.
	void ReplaceChild(ART &art, const uint8_t byte, const Node child = Node()) const;
	//! Insert the child at byte.
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child = Node());
	//! Delete the child at byte.
	static void DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte, const GateStatus status,
	                        const ARTKey &row_id);

	//! Get the immutable child at byte.
	const unsafe_optional_ptr<Node> GetChild(ART &art, const uint8_t byte) const;
	//! Get the child at byte.
	unsafe_optional_ptr<Node> GetChildMutable(ART &art, const uint8_t byte) const;
	//! Get the first immutable child greater than or equal to the byte.
	const unsafe_optional_ptr<Node> GetNextChild(ART &art, uint8_t &byte) const;
	//! Returns true, if the byte exists, else false.
	bool HasByte(ART &art, uint8_t &byte) const;
	//! Get the first byte greater than or equal to the byte.
	bool GetNextByte(ART &art, uint8_t &byte) const;

	//! Returns the string representation of the node, if only_verify is false.
	//! Else, it traverses and verifies the node.
	string VerifyAndToString(ART &art, const bool only_verify) const;
	//! Counts each node type.
	void VerifyAllocations(ART &art, unordered_map<uint8_t, idx_t> &node_counts) const;

	//! Returns the node type for a count.
	static NType GetNodeType(const idx_t count);

	//! Transform the node storage to deprecated storage.
	static void TransformToDeprecated(ART &art, Node &node,
	                                  unsafe_unique_ptr<FixedSizeAllocator> &deprecated_prefix_allocator);

	//! Returns the node type.
	inline NType GetType() const {
		return NType(GetMetadata() & ~AND_GATE);
	}

	//! True, if the node is a Node4, Node16, Node48, or Node256.
	bool IsNode() const;
	//! True, if the node is a Node7Leaf, Node15Leaf, or Node256Leaf.
	bool IsLeafNode() const;
	//! True, if the node is any leaf.
	bool IsAnyLeaf() const;

	//! Get the row ID (8th to 63rd bit).
	inline row_t GetRowId() const {
		return UnsafeNumericCast<row_t>(Get() & AND_ROW_ID);
	}
	//! Set the row ID (8th to 63rd bit).
	inline void SetRowId(const row_t row_id) {
		Set((Get() & AND_METADATA) | UnsafeNumericCast<idx_t>(row_id));
	}

	//! Returns the gate status of a node.
	inline GateStatus GetGateStatus() const {
		return (GetMetadata() & AND_GATE) == 0 ? GateStatus::GATE_NOT_SET : GateStatus::GATE_SET;
	}
	//! Sets the gate status of a node.
	inline void SetGateStatus(const GateStatus status) {
		switch (status) {
		case GateStatus::GATE_SET:
			D_ASSERT(GetType() != NType::LEAF_INLINED);
			SetMetadata(GetMetadata() | AND_GATE);
			break;
		case GateStatus::GATE_NOT_SET:
			SetMetadata(GetMetadata() & ~AND_GATE);
			break;
		}
	}

	//! Assign operator.
	inline void operator=(const IndexPointer &ptr) {
		Set(ptr.Get());
	}

private:
	template <class NODE>
	static void TransformToDeprecatedInternal(ART &art, Node &node, unsafe_unique_ptr<FixedSizeAllocator> &deprecated_allocator) {
		auto allocator = Node::GetAllocator(art, node.GetType());
		auto handle = allocator.GetIfUsed(node);
		if (handle.Empty()) {
			NODE::Iterator(handle.GetRef<NODE>(), [&](Node &child) {
				Node::TransformToDeprecated(art, child, deprecated_allocator);
			});
		}
	}
};

//! NodeChildren holds the extracted bytes of a node, and their respective children.
//! The bytes and children are valid as long as the arena is valid,
//! even if the original node has been freed.
struct NodeChildren {
	NodeChildren() = delete;
	NodeChildren(array_ptr<uint8_t> bytes, array_ptr<Node> children) : bytes(bytes), children(children) {};

	array_ptr<uint8_t> bytes;
	array_ptr<Node> children;
};

template <class T>
class ConstNodeHandle {
public:
	ConstNodeHandle(ART &art, const Node node)
	    : handle(Node::GetAllocator(art, node.GetType()).Get(node)), n(handle.GetRef<T>()) {
	}

	ConstNodeHandle(const ConstNodeHandle &) = delete;
	ConstNodeHandle &operator=(const ConstNodeHandle &) = delete;

	ConstNodeHandle(ConstNodeHandle &&other) noexcept : handle(std::move(other.handle)), n(other.n) {
	}
	ConstNodeHandle &operator=(ConstNodeHandle &&other) noexcept = delete;

public:
	const T &Get() const {
		return n;
	}

private:
	SegmentHandle handle;
	const T &n;
};

template <class T>
class NodeHandle {
public:
	NodeHandle(ART &art, const Node node)
	    : handle(Node::GetAllocator(art, node.GetType()).Get(node)), n(handle.GetRef<T>()) {
		handle.MarkModified();
	}

	NodeHandle(const NodeHandle &) = delete;
	NodeHandle &operator=(const NodeHandle &) = delete;

	NodeHandle(NodeHandle &&other) noexcept : handle(std::move(other.handle)), n(other.n) {
	}
	NodeHandle &operator=(NodeHandle &&other) noexcept = delete;

public:
	T &Get() {
		return n;
	}

private:
	SegmentHandle handle;
	T &n;
};

} // namespace duckdb
