// SPDX-License-Identifier: MPL-2.0

package memberlist

// NodeSelectionDelegate is an optional delegate that can be used to filter and prioritize
// nodes for gossip, and push/pull operations. This allows implementing custom routing logic,
// such as zone-aware or rack-aware gossiping.
//
// This delegate is not used for probes (health checks). When implementing zone-aware gossiping,
// probes can bypass this delegate.
type NodeSelectionDelegate interface {
	// SelectNodes is called with all candidate nodes to determine:
	// - selected: the nodes that should be included in the selection pool
	// - preferred: an optional single node that should be prioritized. During a gossip cycle,
	//              the preferred node is always selected first if provided. If no preferred
	//              node is returned, all gossip targets are chosen randomly from the selected nodes.
	//
	// The input slice may be modified in place and returned as the selected slice.
	SelectNodes(nodes []*Node) (selected []*Node, preferred *Node)
}
