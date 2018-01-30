package speculative

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	"log"

	sp "github.com/sensssz/spinner"
)

func min(num1 int, num2 int) int {
	if num1 <= num2 {
		return num1
	}
	return num2
}

func max(num1 int, num2 int) int {
	if num1 >= num2 {
		return num1
	}
	return num2
}

func nonNegative(num int) int {
	if num > 0 {
		return num
	}
	return 0
}

// Prediction represents a prediction on how *all* the parameters
// of a query are calculated.
type Prediction struct {
	QueryID  int
	ParamOps []Operation
	HitCount int
}

// NewPrediction creates a new Prediction object.
func NewPrediction(queryID int, parameters []Operation) *Prediction {
	return &Prediction{queryID, parameters, 0}
}

// Hit increases the HitCount of this prediction.
func (prediction *Prediction) Hit() {
	prediction.HitCount++
}

// MatchesQuery true if the current prediction perfectly matches the given query.
func (prediction *Prediction) MatchesQuery(trx []*Query, query *Query) bool {
	if prediction.QueryID != query.QueryID {
		return false
	}
	for i := 0; i < len(query.Arguments); i++ {
		if !prediction.ParamOps[i].MatchesValue(trx, query.Arguments[i]) {
			return false
		}
	}
	return true
}

// ToString returns s JSON representation of the object
func (prediction *Prediction) ToString() string {
	return fmt.Sprintf(`{
	"query": %d,
	"hit": %d,
	"ops": %s
}`, prediction.QueryID, prediction.HitCount, operationsToString(prediction.ParamOps))
}

// PredictionTrees contains all the trees for prediction
type PredictionTrees struct {
	trees map[int]*Node
}

// NewPredictionTrees creates a new prediciton tree.
func NewPredictionTrees() *PredictionTrees {
	return &PredictionTrees{make(map[int]*Node)}
}

// Predictor does prediction using the prediction trees.
type Predictor struct {
	gm          *GraphModel
	current     int
	queries     []*Query
	queryQueue  *QueryQueue
	queryParser *QueryParser
	manager     QueryManager
}

// PredictNextSQL returns the most possible next query in SQL form.
func (pt *Predictor) PredictNextSQL() string {
	query := pt.PredictNextQuery()
	if query == nil {
		return ""
	}
	return fillTemplate(query.QueryID, pt.manager, query.Arguments)
}

// PredictNextQuery returns the most possible next query.
func (pt *Predictor) PredictNextQuery() *Query {
	if len(pt.queries) == 0 {
		return nil
	}
	path := pt.queryQueue.GenPath()
	bestMatch := pt.gm.GetEdgeList(pt.current).FindBestPrediction(path)
	if bestMatch == nil {
		return nil
	}
	arguments := make([]interface{}, len(bestMatch.ParamOps))
	for i, paramOp := range bestMatch.ParamOps {
		arguments[i] = paramOp.GetValue(pt.queries)
	}
	return &Query{bestMatch.QueryID, [][]interface{}{}, arguments, true}
}

// MoveToNext query.
func (pt *Predictor) MoveToNext(query *Query) {
	pt.current = query.QueryID
	pt.queries = append(pt.queries, query)
	if len(pt.queries) > LookBackLen {
		pt.queries = pt.queries[1:]
	}
	pt.queryQueue.MoveToNextQuery(query.QueryID)
}

// GraphModel is a graph-based model for prediction.
type GraphModel struct {
	vertexEdges map[int]*EdgeList
}

// NewGraphModel creates a new GraphModel.
func NewGraphModel() *GraphModel {
	return &GraphModel{make(map[int]*EdgeList)}
}

// GetEdgeList returns the EdgeList for this query.
func (gm *GraphModel) GetEdgeList(queryID int) *EdgeList {
	edgeList := gm.vertexEdges[queryID]
	if edgeList == nil {
		edgeList = NewEdgeList()
		gm.vertexEdges[queryID] = edgeList
	}
	return edgeList
}

// Print this model.
func (gm *GraphModel) Print() {
	fmt.Fprintf(os.Stderr, "Graph Model:\n")
	for queryID, edgeList := range gm.vertexEdges {
		fmt.Fprintf(os.Stderr, "%v: [\n", queryID)
		for toQuery, edge := range edgeList.Edges {
			fmt.Fprintf(os.Stderr, "  %v: { %v\n", toQuery, edge.Weight)
			for path, predictions := range edge.Predictions {
				fmt.Fprintf(os.Stderr, "    %v: [%v]\n", path, len(predictions))
			}
			fmt.Fprintf(os.Stderr, "  },\n")
		}
		fmt.Fprintf(os.Stderr, "],\n")
	}
}

// ToString returns a string representation of the model
func (gm *GraphModel) ToString() string {
	var buffer bytes.Buffer
	buffer.WriteString("[")
	for v, el := range gm.vertexEdges {
		buffer.WriteString(fmt.Sprintf(`{
	"vertex": %v,
	"edgelist": %v
},`, v, el.ToString()))
	}
	res := buffer.String()
	end := len(res) - 1
	if res[end] == '[' {
		end++
	}
	res = res[:end] + "]"
	var j interface{}
	if err := json.Unmarshal([]byte(res), &j); err != nil {
		panic(err)
	}
	bytes, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(bytes) + "\n"
}

// NewPredictor creates predictor using the this prediction tree
func (gm *GraphModel) NewPredictor(manager QueryManager) *Predictor {
	return &Predictor{gm, 0, []*Query{}, NewQueryQueue(), NewQueryParser(manager), manager}
}

// ModelBuilder takes in a workload trace and generates a prediciton
// model from it.
type ModelBuilder struct {
	QuerySet             *QuerySet
	Queries              []*Query
	QueryQueue           *QueryQueue
	Current              *Query
	NumOpsAllQueries     [][]Operand
	StrOpsAllQueries     [][]Operand
	NumListOpsAllQueries [][]Operand
	StrListOpsAllQueries [][]Operand
}

// NewModelBuilder creates a new ModelBuilder
func NewModelBuilder(path string) *ModelBuilder {
	builder := &ModelBuilder{NewQuerySet(), []*Query{}, NewQueryQueue(), nil, [][]Operand{}, [][]Operand{}, [][]Operand{}, [][]Operand{}}
	builder.parseQueriesFromFile(path)
	return builder
}

// NewModelBuilderFromContent creates a new ModelBuilder using the given queries
func NewModelBuilderFromContent(queries string) *ModelBuilder {
	builder := &ModelBuilder{NewQuerySet(), []*Query{}, NewQueryQueue(), nil, [][]Operand{}, [][]Operand{}, [][]Operand{}, [][]Operand{}}
	builder.parseQueries(queries)
	return builder
}

// ParseQueries parses all queries from the workload trace.
func (builder *ModelBuilder) parseQueriesFromFile(path string) {
	if queryFile, err := os.Open(path); err == nil {
		defer queryFile.Close()

		scanner := bufio.NewScanner(queryFile)
		queryParser := NewQueryParser(builder.QuerySet)

		spinner := sp.NewSpinnerWithProgress(19, "Parsing query %d...", -1)
		const maxCapacity = 1024 * 1024 * 1024
		buf := make([]byte, maxCapacity)
		scanner.Buffer(buf, maxCapacity)
		spinner.SetCompletionMessage("All queries parsed.")
		spinner.Start()
		i := 0
		for scanner.Scan() {
			spinner.UpdateProgress(i)
			i++
			line := scanner.Text()
			if len(line) <= 1 {
				continue
			}
			query := queryParser.ParseQuery(line)
			if query != nil {
				builder.Queries = append(builder.Queries, query)
			}
		}
		spinner.Stop()
	} else {
		log.Fatal(err)
	}
}

func (builder *ModelBuilder) parseQueries(queries string) {
	queryParser := NewQueryParser(builder.QuerySet)
	lines := strings.Split(queries, "\n")
	for _, line := range lines {
		query := queryParser.ParseQuery(line)
		if query != nil {
			builder.Queries = append(builder.Queries, query)
		}
	}
}

func (builder *ModelBuilder) enumerateConstOperand(query *Query) {
	numOps := make([]Operand, 0, len(query.Arguments))
	strOps := make([]Operand, 0, len(query.Arguments))
	for _, arg := range query.Arguments {
		op := ConstOperand{arg}
		switch arg.(type) {
		case string:
			strOps = append(strOps, &op)
		case float64:
			numOps = append(numOps, &op)
		}
	}
	builder.NumOpsAllQueries = append(builder.NumOpsAllQueries, numOps)
	builder.StrOpsAllQueries = append(builder.StrOpsAllQueries, strOps)
	if len(builder.NumOpsAllQueries) != len(builder.StrOpsAllQueries) ||
		len(builder.NumOpsAllQueries) != len(builder.NumListOpsAllQueries)+1 ||
		len(builder.StrOpsAllQueries) != len(builder.StrListOpsAllQueries)+1 {
		panic(fmt.Sprintf("%v, %v, %v, %v", len(builder.NumOpsAllQueries), len(builder.StrOpsAllQueries), len(builder.NumListOpsAllQueries), len(builder.StrListOpsAllQueries)))
	}
}

func (builder *ModelBuilder) enumerateResultOperand(query *Query, numOps *[]Operand, strOps *[]Operand) {
	if len(query.ResultSet) != 1 {
		return
	}
	for j, cell := range query.ResultSet[0] {
		op := QueryResultOperand{query.QueryID, 0, 0, j}
		switch cell.(type) {
		case string:
			*strOps = append(*strOps, &op)
		case float64:
			*numOps = append(*numOps, &op)
		}
	}
}

func (builder *ModelBuilder) enumerateAggregationOperand(query *Query, numOps *[]Operand, aggregators []Aggregator) {
	if len(query.ResultSet) == 0 {
		return
	}
	for i, cell := range query.ResultSet[0] {
		switch cell.(type) {
		case float64:
			break
		default:
			continue
		}
		for _, aggregator := range aggregators {
			aggregation := AggregationOperand{0, aggregator, i}
			*numOps = append(*numOps, &aggregation)
		}
	}
}

func (builder *ModelBuilder) enumerateArgumentOperand(query *Query, numOps *[]Operand, strOps *[]Operand) {
	for i, arg := range query.Arguments {
		op := QueryArgumentOperand{query.QueryID, 0, i}
		switch arg.(type) {
		case string:
			*strOps = append(*strOps, &op)
		case float64:
			*numOps = append(*numOps, &op)
		}
	}
}

func (builder *ModelBuilder) enumerateArgumentListOperand(query *Query, numLists *[]Operand, strLists *[]Operand) {
	for i, arg := range query.Arguments {
		if set, ok := arg.(*UnorderedSet); ok {
			if set.Size() == 0 {
				continue
			}
			op := ArgumentListOperand{query.QueryID, 0, i}
			switch set.Elements()[0].(type) {
			case string:
				*strLists = append(*strLists, &op)
			case float64:
				*numLists = append(*numLists, &op)
			}
		}
	}
}

func (builder *ModelBuilder) getColumnType(query *Query, columnIndex int) reflect.Kind {
	var kind reflect.Kind
	for _, row := range query.ResultSet {
		if row[columnIndex] != nil {
			kind = reflect.TypeOf(row[columnIndex]).Kind()
			break
		}
	}
	return kind
}

func (builder *ModelBuilder) enumerateColumnListOperand(query *Query, numLists *[]Operand, strLists *[]Operand) {
	if len(query.ResultSet) == 0 {
		return
	}
	firstRow := query.ResultSet[0]
	for i := 0; i < len(firstRow); i++ {
		kind := builder.getColumnType(query, i)
		op := ColumnListOperand{query.QueryID, 0, i}
		switch kind {
		case reflect.String:
			*strLists = append(*strLists, &op)
		case reflect.Float64:
			*numLists = append(*numLists, &op)
		}
	}
}

func (builder *ModelBuilder) enumerateAllOperands(query *Query) {
	numOps := []Operand{}
	strOps := []Operand{}
	numListOps := []Operand{}
	strListOps := []Operand{}

	builder.enumerateResultOperand(query, &numOps, &strOps)
	builder.enumerateArgumentOperand(query, &numOps, &strOps)
	// builder.enumerateAggregationOperand(query, Aggregators, &numOps)
	builder.enumerateArgumentListOperand(query, &numListOps, &strListOps)
	builder.enumerateColumnListOperand(query, &numListOps, &strListOps)

	// For numOps and strOps, the slice for the query at queryIndex has been inserted at enumerateConstOperands
	numQueries := len(builder.NumListOpsAllQueries)
	builder.NumOpsAllQueries[numQueries] = append(builder.NumOpsAllQueries[numQueries], numOps...)
	builder.StrOpsAllQueries[numQueries] = append(builder.StrOpsAllQueries[numQueries], numOps...)
	builder.NumListOpsAllQueries = append(builder.NumListOpsAllQueries, numListOps)
	builder.StrListOpsAllQueries = append(builder.StrListOpsAllQueries, strListOps)
	if numQueries == LookBackLen {
		builder.NumOpsAllQueries = builder.NumOpsAllQueries[1:]
		builder.StrOpsAllQueries = builder.StrOpsAllQueries[1:]
		builder.NumListOpsAllQueries = builder.NumListOpsAllQueries[1:]
		builder.StrListOpsAllQueries = builder.StrListOpsAllQueries[1:]
	}
	if len(builder.NumOpsAllQueries) > LookBackLen ||
		len(builder.StrOpsAllQueries) > LookBackLen ||
		len(builder.NumListOpsAllQueries) > LookBackLen ||
		len(builder.StrListOpsAllQueries) > LookBackLen {
		panic(fmt.Sprintf("%v, %v, %v, %v", len(builder.NumOpsAllQueries), len(builder.StrOpsAllQueries), len(builder.NumListOpsAllQueries), len(builder.StrListOpsAllQueries)))
	}
}

// Search for unary operations that matches the columnIndex-th parameter of the queryIndex-th query.
func (builder *ModelBuilder) searchForUnaryOps(queries []*Query, operands [][]Operand, columnIndex int) []Operation {
	unaryOperations := make([]Operation, 0, len(operands))
	if len(operands) > LookBackLen+1 || len(queries) > LookBackLen || !(len(operands) == len(queries)+1 || len(operands) == len(queries)) {
		panic(fmt.Sprintf("len(operands): %v, len(queries): %v\n", len(operands), len(queries)))
	}
	// fmt.Printf("%+v\n", operands)
	for i := len(operands) - 1; i >= 0; i-- {
		for j := 0; j < len(operands[i]); j++ {
			operand := operands[i][j].Copy()
			operand.SetQueryIndex(i)
			// fmt.Printf("%d,%d:%+v\n", i, j, operand)
			if valueEqual(operand.GetValue(queries), builder.Current.Arguments[columnIndex]) {
				unaryOperations = append(unaryOperations, UnaryOperation{operand})
			}
		}
	}
	if len(unaryOperations) == 0 {
		unaryOperations = append(unaryOperations, RandomOperation{})
	}
	return unaryOperations
}

func (builder *ModelBuilder) enumeratePredictionsFromParaOps(paraOps [][]Operation, queryID int) []*Prediction {
	var numCombis int64
	numCombis = 1
	for _, para := range paraOps {
		numCombis *= int64(len(para))
		if numCombis == 0 {
			break
		}
	}
	predictions := make([]*Prediction, 0, numCombis)
	if numCombis > 0 {
		currentCombi := []Operation{}
		builder.operationCombinations(paraOps, queryID, 0, currentCombi, &predictions)
	}
	return predictions
}

func (builder *ModelBuilder) operationCombinations(paraOps [][]Operation, queryID int, paraIndex int, currentCombi []Operation, allPredictions *[]*Prediction) {
	if paraIndex >= len(paraOps) {
		*allPredictions = append(*allPredictions, NewPrediction(queryID, currentCombi))
		return
	}
	for i := 0; i < len(paraOps[paraIndex]); i++ {
		builder.operationCombinations(paraOps, queryID, paraIndex+1, append(currentCombi, paraOps[paraIndex][i]), allPredictions)
	}
}

func (builder *ModelBuilder) enumeratePredictionsForCurrentQuery(queries []*Query) []*Prediction {
	query := builder.Current
	opsForArgs := make([][]Operation, len(query.Arguments))
	for i, arg := range query.Arguments {
		var candidateOps [][]Operand
		switch arg.(type) {
		case float64:
			candidateOps = builder.NumOpsAllQueries
		case string:
			candidateOps = builder.StrOpsAllQueries
		case *UnorderedSet:
			if len(arg.(*UnorderedSet).Elements()) == 0 {
				break
			}
			switch arg.(*UnorderedSet).Elements()[0].(type) {
			case float64:
				candidateOps = builder.NumListOpsAllQueries
			case string:
				candidateOps = builder.StrListOpsAllQueries
			}
		}
		ops := builder.searchForUnaryOps(queries, candidateOps, i)
		opsForArgs[i] = append(opsForArgs[i], ops...)
	}
	predictions := builder.enumeratePredictionsFromParaOps(opsForArgs, query.QueryID)
	return predictions
}

// UpdateGraphModel updates the GraphModel with the new query.
func (builder *ModelBuilder) UpdateGraphModel(lastNQueries []*Query, query *Query, gm *GraphModel) {
	if builder.Current == nil {
		builder.Current = query
		builder.QueryQueue.MoveToNextQuery(query.QueryID)
		return
	}
	edge := gm.GetEdgeList(builder.Current.QueryID).GetEdge(query.QueryID)
	edge.IncWeight()
	path := builder.QueryQueue.GenPath()
	builder.Current = query
	matches := edge.FindMatchingPredictions(query, lastNQueries, path)
	if len(matches) == 0 {
		matches = builder.enumeratePredictionsForCurrentQuery(lastNQueries)
		edge.AddPredictions(query, path, matches)
	}
	for _, prediction := range matches {
		prediction.Hit()
	}
	builder.QueryQueue.MoveToNextQuery(query.QueryID)
}

// TrainModel trains the model using the given number of queries.
func (builder *ModelBuilder) TrainModel(gm *GraphModel, numForTraining int) {
	spinner := sp.NewSpinnerWithProgress(19, "Updating model with query %d...", -1)
	spinner.Start()
	for i, query := range builder.Queries[:numForTraining] {
		spinner.UpdateProgress(i)
		lastNQueries := builder.Queries[nonNegative(i-LookBackLen):i]
		builder.enumerateConstOperand(query)
		builder.UpdateGraphModel(lastNQueries, query, gm)
		builder.enumerateAllOperands(query)
	}
	spinner.Stop()
}
