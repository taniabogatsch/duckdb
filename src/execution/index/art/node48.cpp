#include "duckdb/execution/index/art/node48.hpp"

#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {

NodeHandle<Node48> Node48::New(ART &art, Node &node) {
	node = Node::GetAllocator(art, NODE_48).New();
	node.SetMetadata(static_cast<uint8_t>(NODE_48));

	auto handle = NodeHandle<Node48>(art, node);
	auto &n = handle.Get();

	n.count = 0;
	for (uint16_t i = 0; i < Node256::CAPACITY; i++) {
		n.child_index[i] = EMPTY_MARKER;
	}
	for (uint8_t i = 0; i < CAPACITY; i++) {
		n.children[i].Clear();
	}

	return handle;
}

void Node48::Free(ART &art, Node &node) {
	auto handle = NodeHandle<Node48>(art, node);
	auto &n = handle.Get();

	if (!n.count) {
		return;
	}
	Iterator(n, [&](Node &child) { Node::Free(art, child); });
}

void Node48::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {
	auto handle = NodeHandle<Node48>(art, node);
	auto &n = handle.Get();

	// The node is full.
	// Grow to Node256.
	if (n.count == CAPACITY) {
		auto node48 = node;
		Node256::GrowNode48(art, node, node48);
		Node256::InsertChild(art, node, byte, child);
		return;
	}

	// Still space.
	// Insert the child.
	uint8_t child_pos = n.count;
	if (n.children[child_pos].HasMetadata()) {
		// Find an empty position in the node list.
		child_pos = 0;
		while (n.children[child_pos].HasMetadata()) {
			child_pos++;
		}
	}

	n.children[child_pos] = child;
	n.child_index[byte] = child_pos;
	n.count++;
}

void Node48::DeleteChild(ART &art, Node &node, const uint8_t byte) {
	auto handle = NodeHandle<Node48>(art, node);
	auto &n = handle.Get();

	// Free the child and decrease the count.
	Node::Free(art, n.children[n.child_index[byte]]);
	n.child_index[byte] = EMPTY_MARKER;
	n.count--;

	// Shrink to Node16.
	if (n.count < SHRINK_THRESHOLD) {
		auto node48 = node;
		Node16::ShrinkNode48(art, node, node48);
	}
}

void Node48::ReplaceChild(const uint8_t byte, const Node child) {
	D_ASSERT(count >= SHRINK_THRESHOLD);

	auto status = children[child_index[byte]].GetGateStatus();
	children[child_index[byte]] = child;
	if (status == GateStatus::GATE_SET && child.HasMetadata()) {
		children[child_index[byte]].SetGateStatus(status);
	}
}

void Node48::GrowNode16(ART &art, Node &node48, Node &node16) {
	auto n16_handle = NodeHandle<Node16>(art, node16);
	auto &n16 = n16_handle.Get();

	auto n48_handle = New(art, node48);
	auto &n48 = n48_handle.Get();
	node48.SetGateStatus(node16.GetGateStatus());

	n48.count = n16.count;
	for (uint16_t i = 0; i < Node256::CAPACITY; i++) {
		n48.child_index[i] = EMPTY_MARKER;
	}
	for (uint8_t i = 0; i < n16.count; i++) {
		n48.child_index[n16.key[i]] = i;
		n48.children[i] = n16.children[i];
	}
	for (uint8_t i = n16.count; i < CAPACITY; i++) {
		n48.children[i].Clear();
	}

	n16.count = 0;
	Node::Free(art, node16);
}

void Node48::ShrinkNode256(ART &art, Node &node48, Node &node256) {
	auto n48_handle = New(art, node48);
	auto &n48 = n48_handle.Get();
	node48.SetGateStatus(node256.GetGateStatus());

	auto n256_handle = NodeHandle<Node256>(art, node256);
	auto &n256 = n256_handle.Get();

	n48.count = 0;
	for (uint16_t i = 0; i < Node256::CAPACITY; i++) {
		if (!n256.children[i].HasMetadata()) {
			n48.child_index[i] = EMPTY_MARKER;
			continue;
		}
		n48.child_index[i] = n48.count;
		n48.children[n48.count] = n256.children[i];
		n48.count++;
	}
	for (uint8_t i = n48.count; i < CAPACITY; i++) {
		n48.children[i].Clear();
	}

	n256.count = 0;
	Node::Free(art, node256);
}

} // namespace duckdb
