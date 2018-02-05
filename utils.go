package speculative

import (
	"bytes"
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

// LookBackLen is number of queries to look back during prediction.
const LookBackLen = 7

// QueryPath is a sequence of query IDs
type QueryPath [LookBackLen]int

// ToString returns a sting representation of the object
func (path QueryPath) ToString() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("[%v", path[0]))
	for _, val := range path[1:] {
		buffer.WriteString(fmt.Sprintf(",%v", val))
	}
	buffer.WriteString("]")
	return buffer.String()
}

// QueryQueue moves long a list of queries, keeping the most recent N ones.
type QueryQueue struct {
	queue [LookBackLen]int
	index int
}

// NewQueryQueue creates a new QueryQueue.
func NewQueryQueue() *QueryQueue {
	return &QueryQueue{[LookBackLen]int{}, 0}
}

// Advance the index
func (q *QueryQueue) Advance() {
	q.index = (q.index + 1) % LookBackLen
}

// MoveToNextQuery moves on to the next query.
func (q *QueryQueue) MoveToNextQuery(query int) {
	q.queue[q.index] = query
	q.Advance()
}

// GenPath generates a QueryPath from the current queue.
func (q *QueryQueue) GenPath() *QueryPath {
	path := &QueryPath{}
	for i := 0; i < LookBackLen; i++ {
		(*path)[i] = q.queue[q.index]
		q.Advance()
	}
	return path
}

// EdgeList contains a list of edges.
type EdgeList struct {
	Edges map[int]*Edge
}

// NewEdgeList creates a new EdgeList.
func NewEdgeList() *EdgeList {
	return &EdgeList{make(map[int]*Edge)}
}

// GetEdge gets the edges of the corresponding query, creating one
// if it does not exist.
func (el *EdgeList) GetEdge(queryID int) *Edge {
	edge := el.Edges[queryID]
	if edge == nil {
		edge = NewEdge(queryID)
		el.Edges[queryID] = edge
	}
	return edge
}

// FindBestPrediction returns the best prediction given a path.
func (el *EdgeList) FindBestPrediction(path *QueryPath) *Prediction {
	var bestMatch *Prediction
	bestMatch = nil
	for _, edge := range el.Edges {
		match := edge.FindBestMatchWithPath(path)
		if match == nil {
			continue
		}
		if bestMatch == nil || match.HitCount > bestMatch.HitCount {
			bestMatch = match
		}
	}
	return bestMatch
}

// ToString returns a string representation of the object
func (el *EdgeList) ToString() string {
	var buffer bytes.Buffer
	buffer.WriteString("[")
	for v, e := range el.Edges {
		buffer.WriteString(fmt.Sprintf(`{
	"vertex": %v,
	"edge": %v
},`, v, e.ToString()))
	}
	res := buffer.String()
	end := len(res) - 1
	if res[end] == '[' {
		end++
	}
	return res[:end] + "]"
}

// Edge is an edge in a graph.
type Edge struct {
	To          int
	Weight      int
	Predictions map[QueryPath][]*Prediction
}

// NewEdge creates a new edge.
func NewEdge(queryID int) *Edge {
	return &Edge{queryID, 0, make(map[QueryPath][]*Prediction)}
}

// IncWeight increments the weight of the edge.
func (e *Edge) IncWeight() {
	e.Weight++
}

// FindBestMatchWithPath returns the best prediction given the path.
func (e *Edge) FindBestMatchWithPath(path *QueryPath) *Prediction {
	predictions, _ := e.Predictions[*path]
	var best *Prediction
	best = nil
	for _, prediction := range predictions {
		if best == nil || prediction.HitCount > best.HitCount {
			best = prediction
		}
	}
	return best
}

// FindMatchingPredictions for the given query and its preceeding queries.
func (e *Edge) FindMatchingPredictions(query *Query, previousQueries []*Query, path *QueryPath) []*Prediction {
	if e.To != query.QueryID {
		return []*Prediction{}
	}
	predictions := e.Predictions[*path]
	if len(predictions) == 0 {
		return predictions
	}
	matches := make([]*Prediction, 0, len(predictions))
	for _, prediction := range predictions {
		if prediction.MatchesQuery(previousQueries, query) {
			matches = append(matches, prediction)
		}
	}
	return matches
}

// AddPredictions adds predictions for the given query under the given path.
func (e *Edge) AddPredictions(query *Query, path *QueryPath, predictions []*Prediction) {
	e.Predictions[*path] = append(e.Predictions[*path], predictions...)
}

func (e *Edge) predictionListToString(predictions []*Prediction) string {
	var buffer bytes.Buffer
	buffer.WriteString("[")
	if len(predictions) > 0 {
		buffer.WriteString(predictions[0].ToString())
		for _, prediction := range predictions[1:] {
			buffer.WriteString("," + prediction.ToString())
		}
	}
	buffer.WriteString("]")
	return buffer.String()
}

func (e *Edge) predictionMapToString() string {
	var buffer bytes.Buffer
	buffer.WriteString("[")
	for path, predictions := range e.Predictions {
		buffer.WriteString(fmt.Sprintf(`{
	"path": %v,
	"predictions": %v
},`, path.ToString(), e.predictionListToString(predictions)))
	}
	res := buffer.String()
	end := len(res) - 1
	if res[end] == '[' {
		end++
	}
	return res[:end] + "]"
}

// ToString returns a string representation of the object
func (e *Edge) ToString() string {
	return fmt.Sprintf(`{
	"to": %v,
	"weight": %v,
	"prediction_map": %v
}`, e.To, e.Weight, e.predictionMapToString())
}
