package speculative

import (
	"testing"
)

func TestNodeToString(t *testing.T) {
	root := NewNode(0, nil)
	firstLevel := []*Node{NewNode(1, root), NewNode(2, root), NewNode(3, root)}
	secondLevel1 := []*Node{NewNode(4, firstLevel[1]), NewNode(5, firstLevel[1])}
	secondLevel2 := []*Node{NewNode(6, firstLevel[2]), NewNode(7, firstLevel[2])}
	root.AddChildren(firstLevel)
	firstLevel[1].AddChildren(secondLevel1)
	firstLevel[2].AddChildren(secondLevel2)
	expectedString := `
0
├── 1
├── 2
│   ├── 4
│   └── 5
└── 3
    ├── 6
    └── 7`
	if "\n"+root.ToString() != expectedString {
		t.Fatalf("Expected %s\n, got \n%s\n", expectedString, root.ToString())
	}
}

func TestSetEqual(t *testing.T) {
	set1 := NewUnorderedSet([]interface{}{1, 2, 3, 4, 5})
	set2 := NewUnorderedSet([]interface{}{3, 5, 2, 1, 4})
	if !set1.Equal(set2) {
		t.Fail()
	}
}
