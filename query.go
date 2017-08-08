package speculative

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Query represents a SQL query.
type Query struct {
	QueryID   int
	ResultSet [][]interface{}
	Arguments []interface{}
	IsSelect  bool
}

func listToString(list []interface{}) string {
	strs := make([]string, len(list))
	for i, ele := range list {
		strs[i] = fmt.Sprintf("%v", ele)
		switch ele.(type) {
		case string:
			strs[i] = "'" + strs[i] + "'"
		}
	}
	return strings.Join(strs, ", ")
}

func fillTemplate(queryID int, manager QueryManager, values []interface{}) string {
	template := manager.GetTemplate(queryID)
	for _, argument := range values {
		switch argument.(type) {
		case string:
			template = strings.Replace(template, "?s", fmt.Sprintf("%v", argument), 1)
		case int:
			template = strings.Replace(template, "?d", fmt.Sprintf("%v", argument), 1)
		case float64:
			template = strings.Replace(template, "?d", fmt.Sprintf("%v", argument), 1)
		case *UnorderedSet:
			set := argument.(*UnorderedSet)
			template = strings.Replace(template, "?l", set.ToString(), 1)
		}
	}
	return template
}

// Same returns true if the two queries are equal.
func (query *Query) Same(another *Query) bool {
	if another == nil {
		return false
	}
	return query.QueryID == another.QueryID &&
		sliceEqual(query.Arguments, another.Arguments)
}

// GetSQL returns the text representation of the SQL.
func (query *Query) GetSQL(querySet QueryManager) string {
	return fillTemplate(query.QueryID, querySet, query.Arguments)
}

// QueryManager stores all query ID and query template mapping.
type QueryManager interface {
	GetQueryID(template string) int
	GetTemplate(queryID int) string
}

// QuerySet represents a set of queries, providing both query ID and query template information.
type QuerySet struct {
	TemplateToID map[string]int
	IDToTemplate map[int]string
}

// NewQuerySet creates a new empty QuerySet
func NewQuerySet() *QuerySet {
	return &QuerySet{make(map[string]int), make(map[int]string)}
}

// GetQueryID returns the ID of a query template.
// It generates a new ID for a new query template.
func (querySet *QuerySet) GetQueryID(template string) int {
	if val, ok := querySet.TemplateToID[template]; ok {
		return val
	}

	queryID := len(querySet.IDToTemplate)
	querySet.IDToTemplate[queryID] = template
	querySet.TemplateToID[template] = queryID
	return queryID
}

// GetTemplate returns the query template given a query ID.
func (querySet *QuerySet) GetTemplate(queryID int) string {
	return querySet.IDToTemplate[queryID]
}

// QueryParser is used to parse SQL query text.
type QueryParser struct {
	queryManager    QueryManager
	argumentPattern *regexp.Regexp
	numStrPattern   *regexp.Regexp
	stringPattern   *regexp.Regexp
	numberPattern   *regexp.Regexp
	strListPattern  *regexp.Regexp
	numListPattern  *regexp.Regexp
	limitPattern    *regexp.Regexp
	offsetPattern   *regexp.Regexp
}

// NewQueryParser creates a new QueryParser object.
func NewQueryParser(queryManager QueryManager) *QueryParser {
	var queryParser QueryParser
	queryParser.queryManager = queryManager
	queryParser.argumentPattern = regexp.MustCompile(`('[^']*'|\b\d+(\.\d+)?\b| IN \([^)]+\))`)
	queryParser.numStrPattern = regexp.MustCompile(`('[^']*'|\b\d+(\.\d+)?\b)`)
	queryParser.stringPattern = regexp.MustCompile(`('[^']*')`)
	queryParser.numberPattern = regexp.MustCompile(`(\b\d+(\.\d+)?\b)`)
	queryParser.strListPattern = regexp.MustCompile(` IN \(([^)']+)\)`)
	queryParser.numListPattern = regexp.MustCompile(` IN \(([^)0-9]+)\)`)
	queryParser.limitPattern = regexp.MustCompile(`LIMIT \d+`)
	queryParser.offsetPattern = regexp.MustCompile(`OFFSET \d+`)
	return &queryParser
}

func (queryParser *QueryParser) convertArguments(args []string) []interface{} {
	numArgs := len(args)
	arguments := make([]interface{}, numArgs)
	for i, arg := range args {
		if strings.Contains(arg, " IN ") {
			listArgs := queryParser.numStrPattern.FindAllString(arg, -1)
			arguments[i] = NewUnorderedSet(queryParser.convertArguments(listArgs))
		} else if strings.Contains(arg, "'") {
			arguments[i] = arg[1 : len(arg)-1]
		} else {
			arguments[i], _ = strconv.ParseFloat(arg, 64)
		}
	}
	return arguments
}

func (queryParser *QueryParser) removeLimitsAndOffsets(sql *string) ([]string, []string) {
	limits := queryParser.limitPattern.FindAllString(*sql, -1)
	offsets := queryParser.offsetPattern.FindAllString(*sql, -1)
	*sql = queryParser.limitPattern.ReplaceAllString(*sql, "%%LIMIT_REPLACEMENT%%")
	*sql = queryParser.offsetPattern.ReplaceAllString(*sql, "%%OFFSET_REPLACEMENT%%")
	return limits, offsets
}

func (queryParser *QueryParser) restoreLimitsAndOffsets(sql *string, limits []string, offsets []string) {
	for _, limit := range limits {
		*sql = strings.Replace(*sql, "%%LIMIT_REPLACEMENT%%", limit, 1)
	}
	for _, offset := range offsets {
		*sql = strings.Replace(*sql, "%%OFFSET_REPLACEMENT%%", offset, 1)
	}
}

func (queryParser *QueryParser) toTemplate(sql string) string {
	template := strings.TrimSpace(sql)
	limits, offsets := queryParser.removeLimitsAndOffsets(&template)
	template = queryParser.stringPattern.ReplaceAllString(template, "'?s'")
	template = queryParser.numberPattern.ReplaceAllString(template, "?d")
	template = queryParser.strListPattern.ReplaceAllString(template, " IN (?l)")
	template = queryParser.numListPattern.ReplaceAllString(template, " IN (?l)")
	queryParser.restoreLimitsAndOffsets(&template, limits, offsets)
	return template
}

// ParseQuery parses a SQL query in text and returns a Query object for it.
func (queryParser *QueryParser) ParseQuery(text string) *Query {
	var queryJSON map[string]interface{}
	if err := json.Unmarshal([]byte(text), &queryJSON); err != nil {
		fmt.Printf("JSON input: '%s'\n", text)
		panic(err)
	}
	sql := queryJSON["sql"].(string)
	resLen := 0
	resultAsSlice, success := queryJSON["results"].([]interface{})
	if success {
		resLen = len(resultAsSlice)
	}
	results := make([][]interface{}, resLen)
	for i, row := range resultAsSlice {
		rowAsSlice, success := row.([]interface{})
		if !success {
			rowAsSlice = []interface{}{row}
		}
		results[i] = rowAsSlice
	}
	resultSet := results
	template := queryParser.toTemplate(sql)
	queryID := queryParser.queryManager.GetQueryID(template)
	queryParser.removeLimitsAndOffsets(&sql)
	args := queryParser.argumentPattern.FindAllString(sql, -1)
	arguments := queryParser.convertArguments(args)
	isSelect := strings.HasPrefix(strings.ToLower(strings.TrimSpace(sql)), "select")
	return &Query{queryID, resultSet, arguments, isSelect}
}
