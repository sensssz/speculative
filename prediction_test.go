package speculative

import (
	"testing"
)

func operandsEqual(ops1 []Operand, ops2 []Operand) bool {
	if len(ops1) != len(ops2) {
		return false
	}
	for i := 0; i < len(ops1); i++ {
		if !ops1[i].Equal(ops2[i]) {
			return false
		}
	}
	return true
}

func TestEnumerateConstOperands(test *testing.T) {
	sqlJSON := `{"sql":"SELECT tag_filters.* FROM tag_filters  WHERE tag_filters.user_id = 2 AND tag_filter.name = 'Google' AND tag_filters.tag_id IN (1, 2, 3, 4, 5) AND tag_filters.content IN ('a', 'b', 'c')","results":[[1,"2017-01-23T19:36:58.000Z","2017-01-23T19:36:58.000Z",2,1],[2,"2017-01-23T19:36:58.000Z","2017-01-23T19:36:58.000Z",2,2],[3,"2017-01-23T19:36:58.000Z","2017-01-23T19:36:58.000Z",2,3]]}`
	builder := NewModelBuilderFromContent(sqlJSON)
	expectedNumOperands := [][]Operand{[]Operand{&ConstOperand{2.0}}}
	expectedStrOperands := [][]Operand{[]Operand{&ConstOperand{"Google"}}}
	builder.enumerateConstOperand(builder.Queries[0])
	actualNumOperands := builder.NumOpsAllQueries
	actualStrOperands := builder.StrOpsAllQueries
	if !operandsEqual(expectedNumOperands[0], actualNumOperands[0]) {
		test.Fatalf("NumOperands wrong. Expected %v, got %v", expectedNumOperands, actualNumOperands)
	}
	if !operandsEqual(expectedStrOperands[0], actualStrOperands[0]) {
		test.Fatalf("StrOperands wrong")
	}
}

func TestEnumerateResultOperand(test *testing.T) {
	sqlJSON := `{"sql":"SELECT tag_filters.* FROM tag_filters  WHERE tag_filters.user_id = 2 AND tag_filter.name = 'Google' AND tag_filters.tag_id IN (1, 2, 3, 4, 5) AND tag_filters.content IN ('a', 'b', 'c')","results":[[1,"2017-01-23T19:36:58.000Z","2017-01-23T19:36:58.000Z",2,1]]}`
	builder := NewModelBuilderFromContent(sqlJSON)
	expectedNumOperands := []Operand{&QueryResultOperand{1, 0, 0, 0}, &QueryResultOperand{1, 0, 0, 3}, &QueryResultOperand{1, 0, 0, 4}}
	expectedStrOperands := []Operand{&QueryResultOperand{1, 0, 0, 1}, &QueryResultOperand{1, 0, 0, 2}}
	actualNumOperands := []Operand{}
	actualStrOperands := []Operand{}
	builder.enumerateResultOperand(builder.Queries[0], &actualNumOperands, &actualStrOperands)
	if !operandsEqual(expectedNumOperands, actualNumOperands) {
		test.Fatalf("NumOperands wrong. Expected %v, got %v", expectedNumOperands, actualNumOperands)
	}
	if !operandsEqual(expectedStrOperands, actualStrOperands) {
		test.Fatalf("StrOperands wrong")
	}
}

func TestEnumerateArgumentOperand(test *testing.T) {
	sqlJSON := `{"sql":"SELECT tag_filters.* FROM tag_filters  WHERE tag_filters.user_id = 2 AND tag_filter.name = 'Google' AND tag_filters.tag_id IN (1, 2, 3, 4, 5) AND tag_filters.content IN ('a', 'b', 'c')","results":[[1,"2017-01-23T19:36:58.000Z","2017-01-23T19:36:58.000Z",2,1],[2,"2017-01-23T19:36:58.000Z","2017-01-23T19:36:58.000Z",2,2],[3,"2017-01-23T19:36:58.000Z","2017-01-23T19:36:58.000Z",2,3]]}`
	builder := NewModelBuilderFromContent(sqlJSON)
	expectedNumOperands := []Operand{&QueryArgumentOperand{1, 0, 0}}
	expectedStrOperands := []Operand{&QueryArgumentOperand{1, 0, 1}}
	actualNumOperands := []Operand{}
	actualStrOperands := []Operand{}
	builder.enumerateArgumentOperand(builder.Queries[0], &actualNumOperands, &actualStrOperands)
	if !operandsEqual(expectedNumOperands, actualNumOperands) {
		test.Fatalf("NumOperands wrong. Expected %v, got %v", expectedNumOperands, actualNumOperands)
	}
	if !operandsEqual(expectedStrOperands, actualStrOperands) {
		test.Fatalf("StrOperands wrong")
	}
}

func TestEnumerateArgumentListOperand(test *testing.T) {
	sqlJSON := `{"sql":"SELECT tag_filters.* FROM tag_filters  WHERE tag_filters.user_id = 2 AND tag_filter.name = 'Google' AND tag_filters.tag_id IN (1, 2, 3, 4, 5) AND tag_filters.content IN ('a', 'b', 'c')","results":[[1,"2017-01-23T19:36:58.000Z","2017-01-23T19:36:58.000Z",2,1],[2,"2017-01-23T19:36:58.000Z","2017-01-23T19:36:58.000Z",2,2],[3,"2017-01-23T19:36:58.000Z","2017-01-23T19:36:58.000Z",2,3]]}`
	builder := NewModelBuilderFromContent(sqlJSON)
	expectedNumOperands := []Operand{&ArgumentListOperand{1, 0, 2}}
	expectedStrOperands := []Operand{&ArgumentListOperand{1, 0, 3}}
	actualNumOperands := []Operand{}
	actualStrOperands := []Operand{}
	builder.enumerateArgumentListOperand(builder.Queries[0], &actualNumOperands, &actualStrOperands)
	if !operandsEqual(expectedNumOperands, actualNumOperands) {
		test.Fatalf("NumOperands wrong. Expected %v, got %v", expectedNumOperands, actualNumOperands)
	}
	if !operandsEqual(expectedStrOperands, actualStrOperands) {
		test.Fatalf("StrOperands wrong")
	}
}

func TestEnumerateColumnListOperand(test *testing.T) {
	sqlJSON := `{"sql":"SELECT tag_filters.* FROM tag_filters  WHERE tag_filters.user_id = 2 AND tag_filter.name = 'Google' AND tag_filters.tag_id IN (1, 2, 3, 4, 5) AND tag_filters.content IN ('a', 'b', 'c')","results":[[1,"2017-01-23T19:36:58.000Z","2017-01-23T19:36:58.000Z",2,1],[2,"2017-01-23T19:36:58.000Z","2017-01-23T19:36:58.000Z",2,2],[3,"2017-01-23T19:36:58.000Z","2017-01-23T19:36:58.000Z",2,3]]}`
	builder := NewModelBuilderFromContent(sqlJSON)
	expectedNumOperands := []Operand{&ColumnListOperand{1, 0, 0}, &ColumnListOperand{1, 0, 3}, &ColumnListOperand{1, 0, 4}}
	expectedStrOperands := []Operand{&ColumnListOperand{1, 0, 1}, &ColumnListOperand{1, 0, 2}}
	actualNumOperands := []Operand{}
	actualStrOperands := []Operand{}
	builder.enumerateColumnListOperand(builder.Queries[0], &actualNumOperands, &actualStrOperands)
	if !operandsEqual(expectedNumOperands, actualNumOperands) {
		test.Fatalf("NumOperands wrong. Expected %v, got %v", expectedNumOperands, actualNumOperands)
	}
	if !operandsEqual(expectedStrOperands, actualStrOperands) {
		test.Fatalf("StrOperands wrong")
	}
}

// func TestSearchForUnary(test *testing.T) {
// 	modelBuilder := NewModelBuilder("test/small_workload_trace")
// 	targetCluster := [][]*Query{}
// 	for _, cluster := range modelBuilder.Clusters {
// 		if modelBuilder.QuerySet.GetTemplate(cluster[0][0].QueryID) == "SELECT  `tags`.* FROM `tags`  WHERE `tags`.`tag` = '?s'  ORDER BY `tags`.`id` ASC LIMIT 1" && len(cluster) > 10 {
// 			targetCluster = cluster
// 			break
// 		}
// 	}
// 	numOpsAllQueries := [][]Operand{}
// 	strOpsAllQueries := [][]Operand{}
// 	numListOpsAllQueries := [][]Operand{}
// 	strListOpsAllQueries := [][]Operand{}
// 	for i := 0; i < 4; i++ {
// 		modelBuilder.enumerateConstOperand(targetCluster[0][i])
// 		modelBuilder.enumerateAllOperands(targetCluster[0][i])
// 	}
// 	unaries := modelBuilder.searchForUnaryOps(targetCluster, numListOpsAllQueries, 4, 0)
// 	expectedUnaries := []Operation{UnaryOperation{&ColumnListOperand{targetCluster[0][2].QueryID, 2, 0}}}
// 	if !reflect.DeepEqual(expectedUnaries, unaries) {
// 		test.Fatalf("Expecting %+v, got %+v\n", expectedUnaries, unaries)
// 	}
// }

// func predictionEqual(prediction1 *Prediction, prediction2 *Prediction) bool {
// 	if prediction1.QueryID != prediction2.QueryID ||
// 		len(prediction1.ParamOps) != len(prediction2.ParamOps) {
// 		return false
// 	}
// 	if !reflect.DeepEqual(prediction1.ParamOps, prediction2.ParamOps) {
// 		return false
// 	}
// 	return true
// }

// func nodesEqual(nodes1 []*Node, nodes2 []*Node) bool {
// 	if len(nodes1) != len(nodes2) {
// 		return false
// 	}
// 	for i := 0; i < len(nodes1); i++ {
// 		pre1 := nodes1[i].Payload.(*Prediction)
// 		pre2 := nodes2[i].Payload.(*Prediction)
// 		if !predictionEqual(pre1, pre2) {
// 			return false
// 		}
// 	}
// 	return true
// }

// func TestEnumeratePredictionsForQuery(test *testing.T) {
// 	modelBuilder := NewModelBuilder("test/small_workload_trace")
// 	targetCluster := [][]*Query{}
// 	for _, cluster := range modelBuilder.Clusters {
// 		if modelBuilder.QuerySet.GetTemplate(cluster[0][0].QueryID) == "SELECT  `tags`.* FROM `tags`  WHERE `tags`.`tag` = '?s'  ORDER BY `tags`.`id` ASC LIMIT 1" && len(cluster) > 10 {
// 			targetCluster = cluster
// 			break
// 		}
// 	}
// 	numOpsAllQueries := [][]Operand{}
// 	strOpsAllQueries := [][]Operand{}
// 	numListOpsAllQueries := [][]Operand{}
// 	strListOpsAllQueries := [][]Operand{}
// 	for i := 0; i < 4; i++ {
// 		modelBuilder.enumerateConstOperand(targetCluster[0][i], &numOpsAllQueries, &strOpsAllQueries)
// 		modelBuilder.enumerateAllOperands(i, targetCluster[0][i], &strOpsAllQueries, &numOpsAllQueries, &strListOpsAllQueries, &numListOpsAllQueries)
// 	}
// 	unaries := modelBuilder.searchForUnaryOps(targetCluster, numListOpsAllQueries, 4, 0)
// 	nodes := modelBuilder.enumeratePredictionsForQuery(nil, targetCluster, 4, numOpsAllQueries, strOpsAllQueries, numListOpsAllQueries, strListOpsAllQueries)
// 	expectedNodes := []*Node{NewNode(NewPrediction(targetCluster[0][4].QueryID, unaries), nil)}
// 	for _, node := range nodes {
// 		for _, trx := range targetCluster {
// 			if !node.Payload.(*Prediction).MatchesQuery(trx, trx[4]) {
// 				test.Fail()
// 			}
// 		}
// 	}
// 	if !nodesEqual(expectedNodes, nodes) {
// 		test.Fail()
// 	}
// }
