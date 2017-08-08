package speculative

import (
	"fmt"
	"testing"
)

type FakeQueryManager struct {
	queryID  int
	template string
}

func (manager *FakeQueryManager) GetQueryID(template string) int {
	return manager.queryID
}

func (manager *FakeQueryManager) GetTemplate(queryID int) string {
	if queryID == manager.queryID {
		return manager.template
	}
	return ""
}

func sliceOfSliceEqual(s1 [][]interface{}, s2 [][]interface{}) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i, eleS1 := range s1 {
		if !sliceEqual(eleS1, s2[i]) {
			fmt.Printf("sliceUnequal: %v, %v", eleS1, s2[i])
			return false
		}
	}
	return true
}

// func TestQueryGetSQL(test *testing.T) {
// 	manager := FakeQueryManager{0, "SELECT u_id FROM USERACCT WHERE " +
// 		"u_int = ?d AND u_float = ?d AND u_str = '?s' " +
// 		"u_ilist IN (?l) AND u_flist IN (?l) AND u_slist IN (?l)"}
// 	arguments := []interface{}{42, 42.42, "42", NewUnorderedSet([]interface{}{42, 43, 44}),
// 		NewUnorderedSet([]interface{}{42.42, 43.42, 44.42}), NewUnorderedSet([]interface{}{"42", "43", "44"})}
// 	query := Query{0, make([][]interface{}, 0), arguments, false, false}
// 	expectedSQL := "SELECT u_id FROM USERACCT WHERE u_int = 42 AND " +
// 		"u_float = 42.42 AND u_str = '42' u_ilist IN (42, 43, 44) " +
// 		"AND u_flist IN (42.42, 43.42, 44.42) AND u_slist IN ('44', '42', '43')"
// 	actualSQL := query.GetSQL(&manager)
// 	if actualSQL != expectedSQL {
// 		test.Fatalf("Expecting %s, got %s", expectedSQL, actualSQL)
// 	}
// }

func TestQueryParserParseString(test *testing.T) {
	sqlJSON := `{
		"sql": "SELECT u_id FROM USERACCT WHERE u_int = 42 AND u_float = 42.42 AND u_str = '42' u_ilist IN (42, 43, 44) AND u_flist IN (42.42, 43.42, 44.42) AND u_slist IN ('42', '43', '44') LIMIT 42",
		"results": [[42, "Is42"], [42]]
	}`
	expectedTemplate := "SELECT u_id FROM USERACCT WHERE u_int = ?d AND u_float = ?d AND u_str = '?s' u_ilist IN (?l) AND u_flist IN (?l) AND u_slist IN (?l) LIMIT 42"
	arguments := []interface{}{42.0, 42.42, "42", NewUnorderedSet([]interface{}{42.0, 43.0, 44.0}),
		NewUnorderedSet([]interface{}{42.42, 43.42, 44.42}), NewUnorderedSet([]interface{}{"42", "43", "44"})}
	manager := FakeQueryManager{0, expectedTemplate}
	queryParser := NewQueryParser(&manager)
	expectedQuery := &Query{0, [][]interface{}{[]interface{}{42.0, "Is42"}, []interface{}{42.0}}, arguments, true}
	actualQuery := queryParser.ParseQuery(sqlJSON)
	actualTemplate := manager.GetTemplate(actualQuery.QueryID)
	if actualTemplate != expectedTemplate {
		test.Fatalf("Expecting template %s, got %s", expectedTemplate, actualTemplate)
	}
	if !sliceEqual(expectedQuery.Arguments, actualQuery.Arguments) {
		test.Fatalf("Expecting arguments %v, got %v", expectedQuery.Arguments, actualQuery.Arguments)
	}
	if !sliceOfSliceEqual(expectedQuery.ResultSet, actualQuery.ResultSet) {
		test.Fatalf("Expecting results %v, got %v", expectedQuery.ResultSet, actualQuery.ResultSet)
	}
	if !actualQuery.IsSelect {
		test.Fatalf("IsSelect wrong")
	}
}
