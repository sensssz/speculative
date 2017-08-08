package speculative

import (
	"bufio"
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
	IsRandom bool
}

// NewPrediction creates a new Prediction object.
func NewPrediction(queryID int, parameters []Operation) *Prediction {
	prediction := Prediction{queryID, parameters, 0, false}
	for _, param := range parameters {
		switch param.(type) {
		case RandomOperation:
			prediction.IsRandom = true
			break
		}
	}
	return &prediction
}

// NewRandomPrediction creates a random prediction
func NewRandomPrediction(queryID int, numOps int) *Prediction {
	ops := make([]Operation, numOps)
	for i := 0; i < numOps; i++ {
		ops[i] = RandomOperation{}
	}
	return &Prediction{queryID, ops, 0, true}
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
	if prediction.IsRandom {
		return true
	}
	for i := 0; i < len(query.Arguments); i++ {
		if !prediction.ParamOps[i].MatchesValue(trx, query.Arguments[i]) {
			return false
		}
	}
	return true
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
	pt          *PredictionTrees
	newTrx      bool
	currentNode *Node
	currentTrx  []*Query
	queryParser *QueryParser
	manager     QueryManager
}

// PrintCurrentTree prints out the tree in a pretty format.
func (pt *Predictor) PrintCurrentTree() {
	node := pt.currentNode
	for node.Parent != nil {
		node = node.Parent
	}
	fmt.Println(node.ToString())
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
	if pt.currentNode == nil ||
		len(pt.currentNode.Children) == 0 {
		pt.currentNode = nil
		return nil
	}
	queryFrequencies := make(map[int]int)
	mostFrequentQuery := -1
	for _, child := range pt.currentNode.Children {
		queryID := child.Payload.(*Prediction).QueryID
		queryFrequencies[queryID]++
		if queryFrequencies[queryID] > queryFrequencies[mostFrequentQuery] {
			mostFrequentQuery = queryID
		}
	}
	if !strings.HasPrefix(pt.manager.GetTemplate(mostFrequentQuery), "SELECT") {
		return nil
	}
	var maxHitChild *Node
	maxHitChild = nil
	for _, child := range pt.currentNode.Children {
		if !child.Payload.(*Prediction).IsRandom &&
			(maxHitChild == nil ||
				maxHitChild.Payload.(*Prediction).QueryID != mostFrequentQuery ||
				maxHitChild.Payload.(*Prediction).HitCount < child.Payload.(*Prediction).HitCount) {
			maxHitChild = child
		}
	}
	if maxHitChild == nil {
		maxHitChild = pt.currentNode.Children[0]
		for _, child := range pt.currentNode.Children {
			if maxHitChild.Payload.(*Prediction).QueryID != mostFrequentQuery ||
				maxHitChild.Payload.(*Prediction).HitCount < child.Payload.(*Prediction).HitCount {
				maxHitChild = child
			}
		}
	}
	prediction := maxHitChild.Payload.(*Prediction)
	if prediction.IsRandom {
		return nil
	}
	arguments := make([]interface{}, len(prediction.ParamOps))
	for i, paramOp := range prediction.ParamOps {
		arguments[i] = paramOp.GetValue(pt.currentTrx)
	}
	return &Query{prediction.QueryID, [][]interface{}{}, arguments, true}
}

// MoveToNext query.
func (pt *Predictor) MoveToNext(query *Query) {
	sql := query.GetSQL(pt.manager)
	if sql == "BEGIN" || sql == "COMMIT" {
		pt.currentTrx = []*Query{}
		pt.currentNode = nil
		pt.newTrx = true
	}
	if pt.currentNode == nil && pt.newTrx {
		pt.newTrx = false
		pt.currentTrx = append(pt.currentTrx, query)
		pt.currentNode = pt.pt.GetTreeWithRoot(query.QueryID, len(query.Arguments))
		return
	}
	if pt.currentNode == nil {
		return
	}
	pt.currentTrx = append(pt.currentTrx, query)
	children := pt.currentNode.Children
	pt.currentNode = nil
	for _, child := range children {
		prediction := child.Payload.(*Prediction)
		if !prediction.MatchesQuery(pt.currentTrx, query) {
			continue
		}
		if pt.currentNode == nil ||
			prediction.HitCount > pt.currentNode.Payload.(*Prediction).HitCount {
			pt.currentNode = child
		}
	}
}

// PrintCurrntTrx prints the query templates of the current transaction.
func (pt *Predictor) PrintCurrntTrx() {
	for _, query := range pt.currentTrx {
		fmt.Printf("%d, %s\n", query.QueryID, pt.manager.GetTemplate(query.QueryID))
	}
}

// EndTransaction ends the current transaction
func (pt *Predictor) EndTransaction() {
	pt.currentNode = nil
	pt.newTrx = true
	pt.currentTrx = []*Query{}
}

// NewPredictor creates predictor using the this prediction tree
func (pt *PredictionTrees) NewPredictor(manager QueryManager) *Predictor {
	return &Predictor{pt, true, nil, []*Query{}, NewQueryParser(manager), manager}
}

// GetTreeWithRoot returns the tree with the given query as root
func (pt *PredictionTrees) GetTreeWithRoot(queryID int, numOps int) *Node {
	tree := pt.trees[queryID]
	if tree == nil {
		tree = NewNode(NewRandomPrediction(queryID, numOps), nil)
		pt.trees[queryID] = tree
	}
	return tree
}

// ModelBuilder takes in a workload trace and generates a prediciton
// model from it.
type ModelBuilder struct {
	QuerySet     *QuerySet
	Queries      []*Query
	Transactions [][]*Query
	Clusters     [][][]*Query
}

// NewModelBuilder creates a new ModelBuilder
func NewModelBuilder(path string) *ModelBuilder {
	builder := &ModelBuilder{NewQuerySet(), []*Query{}, [][]*Query{}, [][][]*Query{}}
	builder.parseQueriesFromFile(path)
	builder.splitTransactions(true)
	builder.clusterTransactions()
	return builder
}

// NewModelBuilderFromContent creates a new ModelBuilder using the given queries
func NewModelBuilderFromContent(queries string) *ModelBuilder {
	builder := &ModelBuilder{NewQuerySet(), []*Query{}, [][]*Query{}, [][][]*Query{}}
	builder.parseQueries(queries)
	builder.splitTransactions(true)
	builder.clusterTransactions()
	return builder
}

// ParseQueries parses all queries from the workload trace.
func (builder *ModelBuilder) parseQueriesFromFile(path string) {
	if queryFile, err := os.Open(path); err == nil {
		defer queryFile.Close()

		scanner := bufio.NewScanner(queryFile)
		queryParser := NewQueryParser(builder.QuerySet)

		spinner := sp.NewSpinnerWithProgress(19, "Parsing query %d...", -1)
		//adjust the capacity to your need (max characters in line)
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
			builder.Queries = append(builder.Queries, queryParser.ParseQuery(line))
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
		builder.Queries = append(builder.Queries, queryParser.ParseQuery(line))
	}
}

func (builder *ModelBuilder) queryIs(query *Query, sql string) bool {
	return query.GetSQL(builder.QuerySet) == sql
}

func (builder *ModelBuilder) trxEnds(query *Query, startsWithBegin bool) bool {
	return (startsWithBegin && builder.queryIs(query, "COMMIT")) ||
		(!startsWithBegin && builder.queryIs(query, "BEGIN"))
}

// moveToNextQuery returns true if the pointers are successfully moved to the next query, and false it reaches the end of the queries.
func (builder *ModelBuilder) moveToNextQuery(queryIndex *int, startsWithBegin *bool) bool {
	query := builder.Queries[*queryIndex]
	if builder.queryIs(query, "COMMIT") {
		(*queryIndex)++
		if *queryIndex >= len(builder.Queries) {
			return false
		}
		*startsWithBegin = builder.queryIs(builder.Queries[*queryIndex], "BEGIN")
		if *startsWithBegin {
			(*queryIndex)++
		}
	} else {
		*startsWithBegin = true
		(*queryIndex)++
	}
	return true
}

// If clusterSingle is ture, all consecutive single query transactions will be viewed as one single transaction.
func (builder *ModelBuilder) splitTransactions(clusterSingle bool) {
	currentTrx := []*Query{}
	startsWithBegin := builder.queryIs(builder.Queries[0], "BEGIN")
	queryIndex := 0
	if startsWithBegin {
		queryIndex++
	}
	for queryIndex < len(builder.Queries) {
		query := builder.Queries[queryIndex]
		if builder.trxEnds(query, startsWithBegin) {
			if len(currentTrx) > 0 && (startsWithBegin || clusterSingle) {
				builder.Transactions = append(builder.Transactions, currentTrx)
			}
			currentTrx = make([]*Query, 0)
			if !builder.moveToNextQuery(&queryIndex, &startsWithBegin) {
				break
			}
		} else {
			currentTrx = append(currentTrx, query)
			queryIndex++
		}
	}
	if len(currentTrx) > 0 {
		builder.Transactions = append(builder.Transactions, currentTrx)
	}
}

func (builder *ModelBuilder) trxToString(trx []*Query) string {
	idStrings := make([]string, len(trx))
	for i, query := range trx {
		idStrings[i] = string(query.QueryID)
	}
	return strings.Join(idStrings, ",")
}

func (builder *ModelBuilder) clusterTransactions() {
	clusters := make(map[string][][]*Query)
	for _, trx := range builder.Transactions {
		trxID := builder.trxToString(trx)
		clusters[trxID] = append(clusters[trxID], trx)
	}
	builder.Clusters = make([][][]*Query, len(clusters))
	index := 0
	for _, cluster := range clusters {
		builder.Clusters[index] = cluster
		index++
	}
}

func (builder *ModelBuilder) enumerateConstOperand(query *Query, numOpsAllQueries *[][]Operand, strOpsAllQueries *[][]Operand) {
	numOps := make([]Operand, 0, len(query.Arguments))
	strOps := make([]Operand, 0, len(query.Arguments))
	for _, arg := range query.Arguments {
		op := ConstOperand{arg}
		switch arg.(type) {
		case string:
			strOps = append(strOps, op)
		case float64:
			numOps = append(numOps, op)
		}
	}
	*numOpsAllQueries = append(*numOpsAllQueries, numOps)
	*strOpsAllQueries = append(*strOpsAllQueries, strOps)
}

func (builder *ModelBuilder) enumerateResultOperand(queryIndex int, query *Query, numOps *[]Operand, strOps *[]Operand) {
	if len(query.ResultSet) != 1 {
		return
	}
	for j, cell := range query.ResultSet[0] {
		op := QueryResultOperand{query.QueryID, queryIndex, 0, j}
		switch cell.(type) {
		case string:
			*strOps = append(*strOps, op)
		case float64:
			*numOps = append(*numOps, op)
		}
	}
}

func (builder *ModelBuilder) enumerateAggregationOperand(queryIndex int, query *Query, numOps *[]Operand, aggregators []Aggregator) {
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
			aggregation := AggregationOperand{queryIndex, aggregator, i}
			*numOps = append(*numOps, aggregation)
		}
	}
}

func (builder *ModelBuilder) enumerateArgumentOperand(queryIndex int, query *Query, numOps *[]Operand, strOps *[]Operand) {
	for i, arg := range query.Arguments {
		op := QueryArgumentOperand{query.QueryID, queryIndex, i}
		switch arg.(type) {
		case string:
			*strOps = append(*strOps, op)
		case float64:
			*numOps = append(*numOps, op)
		}
	}
}

func (builder *ModelBuilder) enumerateArgumentListOperand(queryIndex int, query *Query, numLists *[]Operand, strLists *[]Operand) {
	for i, arg := range query.Arguments {
		if set, ok := arg.(*UnorderedSet); ok {
			if set.Size() == 0 {
				continue
			}
			op := ArgumentListOperand{query.QueryID, queryIndex, i}
			switch set.Elements()[0].(type) {
			case string:
				*strLists = append(*strLists, op)
			case float64:
				*numLists = append(*numLists, op)
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

func (builder *ModelBuilder) enumerateColumnListOperand(queryIndex int, query *Query, numLists *[]Operand, strLists *[]Operand) {
	if len(query.ResultSet) == 0 {
		return
	}
	firstRow := query.ResultSet[0]
	for i := 0; i < len(firstRow); i++ {
		kind := builder.getColumnType(query, i)
		op := ColumnListOperand{query.QueryID, queryIndex, i}
		switch kind {
		case reflect.String:
			*strLists = append(*strLists, op)
		case reflect.Float64:
			*numLists = append(*numLists, op)
		}
	}
}

func (builder *ModelBuilder) enumerateAllOperands(queryIndex int, query *Query, numOpsAllQueries *[][]Operand, strOpsAllQueries *[][]Operand, numListOpsAllQueries *[][]Operand, strListOpsAllQueries *[][]Operand) {
	numOps := []Operand{}
	strOps := []Operand{}
	numListOps := []Operand{}
	strListOps := []Operand{}

	builder.enumerateResultOperand(queryIndex, query, &numOps, &strOps)
	builder.enumerateArgumentOperand(queryIndex, query, &numOps, &strOps)
	// builder.enumerateAggregationOperand(queryIndex, query, Aggregators, &numOps)
	builder.enumerateArgumentListOperand(queryIndex, query, &numListOps, &strListOps)
	builder.enumerateColumnListOperand(queryIndex, query, &numListOps, &strListOps)

	// For numOps and strOps, the slice for the query at queryIndex has been inserted at enumerateConstOperands
	(*numOpsAllQueries)[queryIndex] = append((*numOpsAllQueries)[queryIndex], numOps...)
	(*strOpsAllQueries)[queryIndex] = append((*strOpsAllQueries)[queryIndex], strOps...)
	*numListOpsAllQueries = append(*numListOpsAllQueries, numListOps)
	*strListOpsAllQueries = append(*strListOpsAllQueries, strListOps)
}

// Search for unary operations that matches the columnIndex-th parameter of the queryIndex-th query.
func (builder *ModelBuilder) searchForUnaryOps(transactions [][]*Query, operands [][]Operand, queryIndex int, columnIndex int) []Operation {
	unaryOperations := make([]Operation, 0, len(operands))
	for i := len(operands) - 1; i >= 0; i-- {
		for j := 0; j < len(operands[i]); j++ {
			operand := operands[i][j]
			matches := true
			for trxIndex := 0; trxIndex < len(transactions); trxIndex++ {
				targetQuery := transactions[trxIndex][queryIndex]
				if !valueEqual(operand.GetValue(transactions[trxIndex]), targetQuery.Arguments[columnIndex]) {
					matches = false
					break
				}
			}
			if matches {
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

func (builder *ModelBuilder) collapseArgOperand(parent *Node, parentLevel int, op Operand) Operand {
	if parent == nil {
		return op
	}
	targetQueryIndex := 0
	targetArgIndex := 0
	switch op.(type) {
	case QueryArgumentOperand:
		argOp := op.(QueryArgumentOperand)
		targetQueryIndex = argOp.QueryIndex
		targetArgIndex = argOp.ArgIndex
	case ArgumentListOperand:
		argOp := op.(ArgumentListOperand)
		targetQueryIndex = argOp.QueryIndex
		targetArgIndex = argOp.ArgIndex
	}
	for parentLevel > targetQueryIndex {
		parent = parent.Parent
		parentLevel--
		if parent == nil {
			return op
		}
	}
	prediction := parent.Payload.(*Prediction)
	argOperation := prediction.ParamOps[targetArgIndex]
	if _, ok := argOperation.(RandomOperation); ok {
		// Prediction for the target arg is random, not need to collapse.
		return op
	}
	argOperand := argOperation.(UnaryOperation).Operand
	if _, ok := argOperand.(QueryArgumentOperand); ok {
		return builder.collapseArgOperand(parent, parentLevel, argOperand)
	}
	if _, ok := argOperand.(ArgumentListOperand); ok {
		return builder.collapseArgOperand(parent, parentLevel, argOperand)
	}
	return argOperand
}

func (builder *ModelBuilder) collapseOperands(parent *Node, parentLevel int, operands [][]Operand) [][]Operand {
	for i := len(operands) - 1; i >= 0; i-- {
		ops := operands[i]
		for i, op := range ops {
			switch op.(type) {
			case QueryArgumentOperand:
				collapsedOp := builder.collapseArgOperand(parent, parentLevel, op)
				ops[i] = collapsedOp
			case ArgumentListOperand:
				collapsedOp := builder.collapseArgOperand(parent, parentLevel, op)
				ops[i] = collapsedOp
			}
		}
	}

	// Deduplicate ops
	opExistsence := make(map[Operand]bool)
	deduplicatedOps := make([][]Operand, len(operands))
	for i := len(operands) - 1; i >= 0; i-- {
		ops := operands[i]
		deduplicated := make([]Operand, 0, len(ops))
		for _, op := range ops {
			if !opExistsence[op] {
				opExistsence[op] = true
				deduplicated = append(deduplicated, op)
			}
		}
		deduplicatedOps[i] = deduplicated
	}

	return deduplicatedOps
}

func (builder *ModelBuilder) enumeratePredictionsForQuery(parent *Node, transactions [][]*Query, queryIndex int, numOps [][]Operand, strOps [][]Operand, numListOps [][]Operand, strListOps [][]Operand) []*Node {
	query := transactions[0][queryIndex]
	numOps = builder.collapseOperands(parent, queryIndex-1, numOps)
	strOps = builder.collapseOperands(parent, queryIndex-1, strOps)
	numListOps = builder.collapseOperands(parent, queryIndex-1, numListOps)
	strListOps = builder.collapseOperands(parent, queryIndex-1, strListOps)
	opsForArgs := make([][]Operation, len(query.Arguments))
	for i, arg := range query.Arguments {
		var candidateOps [][]Operand
		switch arg.(type) {
		case float64:
			candidateOps = numOps
		case string:
			candidateOps = strOps
		case *UnorderedSet:
			if len(arg.(*UnorderedSet).Elements()) == 0 {
				break
			}
			switch arg.(*UnorderedSet).Elements()[0].(type) {
			case float64:
				candidateOps = numListOps
			case string:
				candidateOps = strListOps
			}
		}
		ops := builder.searchForUnaryOps(transactions, candidateOps, queryIndex, i)
		opsForArgs[i] = append(opsForArgs[i], ops...)
	}
	predictions := builder.enumeratePredictionsFromParaOps(opsForArgs, query.QueryID)
	if len(predictions) == 0 {
		predictions = append(predictions, NewRandomPrediction(query.QueryID, len(query.Arguments)))
	}
	nodes := make([]*Node, len(predictions))
	for i, prediction := range predictions {
		nodes[i] = NewNode(prediction, parent)
	}
	return nodes
}

// UpdateModel updates the model using the supplied transactions.
func (builder *ModelBuilder) UpdateModel(transactions [][]*Query, pt *PredictionTrees) {
	exampleTrx := transactions[0]
	numTrx := min(10, len(transactions))
	firstTen := transactions[:numTrx]
	numOpsAllQueries := [][]Operand{}
	strOpsAllQueries := [][]Operand{}
	numListOpsAllQueries := [][]Operand{}
	strListOpsAllQueries := [][]Operand{}
	root := pt.GetTreeWithRoot(exampleTrx[0].QueryID, len(exampleTrx[0].Arguments))
	currentLevel := []*Node{root}
	nextLevel := []*Node{}
	builder.enumerateConstOperand(exampleTrx[0], &numOpsAllQueries, &strOpsAllQueries)
	builder.enumerateAllOperands(0, exampleTrx[0], &numOpsAllQueries, &strOpsAllQueries, &numListOpsAllQueries, &strListOpsAllQueries)
	for index, query := range exampleTrx[1:] {
		i := index + 1
		builder.enumerateConstOperand(query, &numOpsAllQueries, &strOpsAllQueries)
		for _, node := range currentLevel {
			predictionsForThisQuery := node.FilterChildren(func(payload interface{}) bool {
				if prediction, ok := payload.(*Prediction); ok {
					return prediction.QueryID == query.QueryID
				}
				return false
			})
			if len(predictionsForThisQuery) == 0 {
				numOpsLen := len(numOpsAllQueries)
				strOpsLen := len(strOpsAllQueries)
				numListOpsLen := len(numListOpsAllQueries)
				strListOpsLen := len(strListOpsAllQueries)
				lastN := 7
				numOpsLastN := numOpsAllQueries[nonNegative(numOpsLen-lastN):numOpsLen]
				strOpsLastN := strOpsAllQueries[nonNegative(strOpsLen-lastN):strOpsLen]
				numListOpsLastN := numListOpsAllQueries[nonNegative(numListOpsLen-lastN):numListOpsLen]
				strListOpsLastN := strListOpsAllQueries[nonNegative(strListOpsLen-lastN):strListOpsLen]
				predictionsForThisQuery = builder.enumeratePredictionsForQuery(node, firstTen, i, numOpsLastN, strOpsLastN, numListOpsLastN, strListOpsLastN)
				node.AddChildren(predictionsForThisQuery)
			}
			matchedPredictions := make([]*Node, 0, len(predictionsForThisQuery))
			for _, node := range predictionsForThisQuery {
				hits := false
				prediction := node.Payload.(*Prediction)
				for _, trx := range transactions {
					if prediction.MatchesQuery(trx, trx[i]) {
						hits = true
						prediction.Hit()
					}
				}
				if hits {
					matchedPredictions = append(matchedPredictions, node)
				}
			}
			if len(matchedPredictions) == 0 {
				newChild := []*Node{NewNode(NewRandomPrediction(i, len(query.Arguments)), node)}
				node.AddChildren(newChild)
				matchedPredictions = append(matchedPredictions, newChild[0])
			}
			nextLevel = append(nextLevel, matchedPredictions...)
			if len(nextLevel) > 10000 {
				break
			}
		}
		currentLevel = nextLevel
		nextLevel = []*Node{}
		builder.enumerateAllOperands(i, exampleTrx[i], &numOpsAllQueries, &strOpsAllQueries, &numListOpsAllQueries, &strListOpsAllQueries)
	}
}
