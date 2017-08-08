package speculative

import (
	"fmt"
	"strings"
)

// Node represents a node in a tree.
type Node struct {
	Payload  interface{}
	Parent   *Node
	Children []*Node
}

// NewNode creates a new node with the payload and no children.
func NewNode(payload interface{}, parent *Node) *Node {
	return &Node{payload, parent, []*Node{}}
}

// AddChild adds a child to the current node.
func (node *Node) AddChild(payload interface{}) {
	newNode := Node{payload, node, []*Node{}}
	node.Children = append(node.Children, &newNode)
}

// AddChildren adds a list of nodes as children.
func (node *Node) AddChildren(children []*Node) {
	node.Children = append(node.Children, children...)
}

// FilterChildren returns the number of children satisfying certain conditions.
func (node *Node) FilterChildren(filter func(interface{}) bool) []*Node {
	filtered := make([]*Node, 0, len(node.Children))
	for _, child := range node.Children {
		if filter(child) {
			filtered = append(filtered, child)
		}
	}
	return filtered
}

// HasNoChildren returns whether or not the node has any child.
func (node *Node) HasNoChildren() bool {
	return len(node.Children) == 0
}

// Size returns the size of the tree.
func (node *Node) Size() int {
	size := 1
	for _, child := range node.Children {
		size += child.Size()
	}
	return size
}

func (node *Node) nonLeafSize() int {
	if len(node.Children) == 0 {
		return 0
	}
	size := 1
	for _, child := range node.Children {
		size += child.Size()
	}
	return size
}

func (node *Node) totalDegree() int {
	degrees := len(node.Children)
	for _, child := range node.Children {
		degrees += child.totalDegree()
	}
	return degrees
}

// AvgDegree returns the averge degree of the nodes in the tree.
func (node *Node) AvgDegree() float64 {
	nonLeafSize := node.nonLeafSize()
	if nonLeafSize == 0 {
		return 0
	}
	return float64(node.totalDegree()) / float64(nonLeafSize)
}

func (node *Node) toStringRecurr() []string {
	lines := make([]string, 0, len(node.Children)+1)
	lines = append(lines, fmt.Sprintf("%+v", node.Payload))
	for i := 0; i < len(node.Children); i++ {
		childFirstLinePrefix := ""
		childMoreLinesPrefix := ""
		if i < len(node.Children)-1 {
			childFirstLinePrefix = "├── "
			childMoreLinesPrefix = "│   "
		} else {
			childFirstLinePrefix = "└── "
			childMoreLinesPrefix = "    "
		}
		child := node.Children[i]
		childLines := child.toStringRecurr()
		lines = append(lines, childFirstLinePrefix+childLines[0])
		for j := 1; j < len(childLines); j++ {
			lines = append(lines, childMoreLinesPrefix+childLines[j])
		}
	}

	return lines
}

func (node *Node) toStringWithMaxDepthRecurr(maxDepth int, depth int) []string {
	if depth > maxDepth {
		return []string{""}
	}
	lines := make([]string, 0, len(node.Children)+1)
	lines = append(lines, fmt.Sprintf("%+v", node.Payload))
	for i := 0; i < len(node.Children); i++ {
		childFirstLinePrefix := ""
		childMoreLinesPrefix := ""
		if i < len(node.Children)-1 {
			childFirstLinePrefix = "├── "
			childMoreLinesPrefix = "│   "
		} else {
			childFirstLinePrefix = "└── "
			childMoreLinesPrefix = "    "
		}
		child := node.Children[i]
		childLines := child.toStringWithMaxDepthRecurr(maxDepth, depth+1)
		lines = append(lines, childFirstLinePrefix+childLines[0])
		for j := 1; j < len(childLines); j++ {
			lines = append(lines, childMoreLinesPrefix+childLines[j])
		}
	}

	return lines
}

// ToString returns a string representation of this tree.
func (node *Node) ToString() string {
	return strings.Join(node.toStringRecurr(), "\n")
}

// ToStringWithMaxDepth returns a string representation of this tree,
// up to the given depth
func (node *Node) ToStringWithMaxDepth(depth int) string {
	return strings.Join(node.toStringWithMaxDepthRecurr(depth, 0), "\n")
}

// UnorderedSet is a set implemented using hash map.
type UnorderedSet struct {
	elements map[interface{}]bool
}

// NewEmptyUnorderedSet returns an empty set.
func NewEmptyUnorderedSet() *UnorderedSet {
	return &UnorderedSet{make(map[interface{}]bool)}
}

// NewUnorderedSet returns a new set containing all elements in the list.
func NewUnorderedSet(list []interface{}) *UnorderedSet {
	set := UnorderedSet{make(map[interface{}]bool)}
	for _, ele := range list {
		set.elements[ele] = true
	}
	return &set
}

// Size returns the size of the set.
func (set *UnorderedSet) Size() int {
	return len(set.elements)
}

// Insert an element to the set.
func (set *UnorderedSet) Insert(element interface{}) {
	set.elements[element] = true
}

// Equal returns true if these two sets contain the same elements.
func (set *UnorderedSet) Equal(another *UnorderedSet) bool {
	if len(set.elements) != len(another.elements) {
		return false
	}
	for ele := range set.elements {
		if _, ok := another.elements[ele]; !ok {
			return false
		}
	}
	return true
}

// Elements returns the elements as a slice.
func (set *UnorderedSet) Elements() []interface{} {
	eleAsList := make([]interface{}, len(set.elements))
	i := 0
	for ele := range set.elements {
		eleAsList[i] = ele
		i++
	}
	return eleAsList
}

// ToString returns a string representation of the set.
func (set *UnorderedSet) ToString() string {
	return listToString(set.Elements())
}
