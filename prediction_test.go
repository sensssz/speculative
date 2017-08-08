package speculative

import "testing"

import "fmt"
import "reflect"

func TestSplitTransactions(t *testing.T) {
	modelBuilder := NewModelBuilder("test/small_workload_trace")
	for _, cluster := range modelBuilder.Clusters {
		if modelBuilder.QuerySet.GetTemplate(cluster[0][0].QueryID) != "SELECT  `tags`.* FROM `tags`  WHERE `tags`.`tag` = '?s'  ORDER BY `tags`.`id` ASC LIMIT ?d" {
			continue
		}
		// for _, trx := range cluster {
		// 	for _, query := range trx {
		// 		fmt.Println(modelBuilder.QuerySet.GetTemplate(query.QueryID))
		// 	}
		// 	fmt.Println("")
		// }
		// fmt.Printf("\n\n")
	}
}

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
	expectedNumOperands := [][]Operand{[]Operand{ConstOperand{2.0}}}
	expectedStrOperands := [][]Operand{[]Operand{ConstOperand{"Google"}}}
	actualNumOperands := [][]Operand{}
	actualStrOperands := [][]Operand{}
	builder.enumerateConstOperand(builder.Queries[0], &actualNumOperands, &actualStrOperands)
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
	expectedNumOperands := []Operand{QueryResultOperand{0, 0, 0, 0}, QueryResultOperand{0, 0, 0, 3}, QueryResultOperand{0, 0, 0, 4}}
	expectedStrOperands := []Operand{QueryResultOperand{0, 0, 0, 1}, QueryResultOperand{0, 0, 0, 2}}
	actualNumOperands := []Operand{}
	actualStrOperands := []Operand{}
	builder.enumerateResultOperand(0, builder.Queries[0], &actualNumOperands, &actualStrOperands)
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
	expectedNumOperands := []Operand{QueryArgumentOperand{0, 0, 0}}
	expectedStrOperands := []Operand{QueryArgumentOperand{0, 0, 1}}
	actualNumOperands := []Operand{}
	actualStrOperands := []Operand{}
	builder.enumerateArgumentOperand(0, builder.Queries[0], &actualNumOperands, &actualStrOperands)
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
	expectedNumOperands := []Operand{ArgumentListOperand{0, 0, 2}}
	expectedStrOperands := []Operand{ArgumentListOperand{0, 0, 3}}
	actualNumOperands := []Operand{}
	actualStrOperands := []Operand{}
	builder.enumerateArgumentListOperand(0, builder.Queries[0], &actualNumOperands, &actualStrOperands)
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
	expectedNumOperands := []Operand{ColumnListOperand{0, 0, 0}, ColumnListOperand{0, 0, 3}, ColumnListOperand{0, 0, 4}}
	expectedStrOperands := []Operand{ColumnListOperand{0, 0, 1}, ColumnListOperand{0, 0, 2}}
	actualNumOperands := []Operand{}
	actualStrOperands := []Operand{}
	builder.enumerateColumnListOperand(0, builder.Queries[0], &actualNumOperands, &actualStrOperands)
	if !operandsEqual(expectedNumOperands, actualNumOperands) {
		test.Fatalf("NumOperands wrong. Expected %v, got %v", expectedNumOperands, actualNumOperands)
	}
	if !operandsEqual(expectedStrOperands, actualStrOperands) {
		test.Fatalf("StrOperands wrong")
	}
}

func TestSearchForUnary(test *testing.T) {
	modelBuilder := NewModelBuilder("test/small_workload_trace")
	targetCluster := [][]*Query{}
	for _, cluster := range modelBuilder.Clusters {
		if modelBuilder.QuerySet.GetTemplate(cluster[0][0].QueryID) == "SELECT  `tags`.* FROM `tags`  WHERE `tags`.`tag` = '?s'  ORDER BY `tags`.`id` ASC LIMIT 1" && len(cluster) > 10 {
			targetCluster = cluster
			break
		}
	}
	numOpsAllQueries := [][]Operand{}
	strOpsAllQueries := [][]Operand{}
	numListOpsAllQueries := [][]Operand{}
	strListOpsAllQueries := [][]Operand{}
	for i := 0; i < 4; i++ {
		modelBuilder.enumerateConstOperand(targetCluster[0][i], &numOpsAllQueries, &strOpsAllQueries)
		modelBuilder.enumerateAllOperands(i, targetCluster[0][i], &numOpsAllQueries, &strOpsAllQueries, &numListOpsAllQueries, &strListOpsAllQueries)
	}
	unaries := modelBuilder.searchForUnaryOps(targetCluster, numListOpsAllQueries, 4, 0)
	expectedUnaries := []Operation{UnaryOperation{ColumnListOperand{targetCluster[0][2].QueryID, 2, 0}}}
	if !reflect.DeepEqual(expectedUnaries, unaries) {
		test.Fatalf("Expecting %+v, got %+v\n", expectedUnaries, unaries)
	}
}

func TestCollapseOperands(test *testing.T) {
	sqlJSON := `{"sql":"SELECT * FROM users WHERE id = 0","results":[[1], [2], [3]]}
	{"sql":"SELECT * FROM tags WHERE id IN (1, 2, 3)","results":[[1]]}
	{"sql":"SELECT * FROM tag_filters WHERE id IN (1, 2, 3)","results":[[1]]}`
	modelBuilder := NewModelBuilderFromContent(sqlJSON)
	const0 := ConstOperand{0}
	constsList := []Operand{const0, const0}
	columnListOp := ColumnListOperand{0, 0, 0}
	argListOp1 := ArgumentListOperand{0, 1, 0}
	argListOp2 := ArgumentListOperand{0, 2, 0}
	numListOps := []Operand{columnListOp, argListOp1, argListOp2}
	root := NewNode(NewPrediction(0, []Operation{UnaryOperation{const0}}), nil)
	firstLevel := NewNode(NewPrediction(1, []Operation{UnaryOperation{columnListOp}}), root)
	secondLevel1 := NewNode(NewPrediction(2, []Operation{UnaryOperation{argListOp1}}), firstLevel)
	secondLevel2 := NewNode(NewPrediction(2, []Operation{UnaryOperation{columnListOp}}), firstLevel)
	root.AddChildren([]*Node{firstLevel})
	firstLevel.AddChildren([]*Node{secondLevel1, secondLevel2})
	collapsed := modelBuilder.collapseOperands(secondLevel1, 2, [][]Operand{constsList, numListOps})
	if !reflect.DeepEqual([][]Operand{[]Operand{const0}, []Operand{columnListOp}}, collapsed) {
		test.Fail()
	}
}

func predictionEqual(prediction1 *Prediction, prediction2 *Prediction) bool {
	if prediction1.QueryID != prediction2.QueryID ||
		len(prediction1.ParamOps) != len(prediction2.ParamOps) {
		return false
	}
	if !reflect.DeepEqual(prediction1.ParamOps, prediction2.ParamOps) {
		return false
	}
	return true
}

func nodesEqual(nodes1 []*Node, nodes2 []*Node) bool {
	if len(nodes1) != len(nodes2) {
		return false
	}
	for i := 0; i < len(nodes1); i++ {
		pre1 := nodes1[i].Payload.(*Prediction)
		pre2 := nodes2[i].Payload.(*Prediction)
		if !predictionEqual(pre1, pre2) {
			return false
		}
	}
	return true
}

func TestEnumeratePredictionsForQuery(test *testing.T) {
	modelBuilder := NewModelBuilder("test/small_workload_trace")
	targetCluster := [][]*Query{}
	for _, cluster := range modelBuilder.Clusters {
		if modelBuilder.QuerySet.GetTemplate(cluster[0][0].QueryID) == "SELECT  `tags`.* FROM `tags`  WHERE `tags`.`tag` = '?s'  ORDER BY `tags`.`id` ASC LIMIT 1" && len(cluster) > 10 {
			targetCluster = cluster
			break
		}
	}
	numOpsAllQueries := [][]Operand{}
	strOpsAllQueries := [][]Operand{}
	numListOpsAllQueries := [][]Operand{}
	strListOpsAllQueries := [][]Operand{}
	for i := 0; i < 4; i++ {
		modelBuilder.enumerateConstOperand(targetCluster[0][i], &numOpsAllQueries, &strOpsAllQueries)
		modelBuilder.enumerateAllOperands(i, targetCluster[0][i], &strOpsAllQueries, &numOpsAllQueries, &strListOpsAllQueries, &numListOpsAllQueries)
	}
	unaries := modelBuilder.searchForUnaryOps(targetCluster, numListOpsAllQueries, 4, 0)
	nodes := modelBuilder.enumeratePredictionsForQuery(nil, targetCluster, 4, numOpsAllQueries, strOpsAllQueries, numListOpsAllQueries, strListOpsAllQueries)
	expectedNodes := []*Node{NewNode(NewPrediction(targetCluster[0][4].QueryID, unaries), nil)}
	for _, node := range nodes {
		for _, trx := range targetCluster {
			if !node.Payload.(*Prediction).MatchesQuery(trx, trx[4]) {
				test.Fail()
			}
		}
	}
	if !nodesEqual(expectedNodes, nodes) {
		test.Fail()
	}
}

func PredictionTreeEqual(tree1 *Node, tree2 *Node) bool {
	prediction1 := tree1.Payload.(*Prediction)
	prediction2 := tree2.Payload.(*Prediction)
	if len(tree1.Children) != len(tree2.Children) ||
		!predictionEqual(prediction1, prediction2) {
		return false
	}
	for i := 0; i < len(tree1.Children); i++ {
		if !PredictionTreeEqual(tree1.Children[i], tree2.Children[i]) {
			return false
		}
	}
	return true
}

func TestBuildOrUpdateTrees(t *testing.T) {
	modelBuilder := NewModelBuilder("test/bug.log")
	targetCluster := [][]*Query{}
	pt := NewPredictionTrees()
	fmt.Println(modelBuilder.QuerySet.GetTemplate(modelBuilder.Clusters[0][0][3].QueryID))
	for i, cluster := range modelBuilder.Clusters {
		fmt.Printf("Cluster %d\n", i)
		modelBuilder.UpdateModel(cluster, pt)
		// for _, tree := range pt.trees {
		// fmt.Printf("Tree size: %d, degree %v\n", tree.Size(), tree.AvgDegree())
		// fmt.Println(tree.ToString())
		// }
		if len(cluster[0]) > 3 &&
			modelBuilder.QuerySet.GetTemplate(cluster[0][3].QueryID) == "SELECT COUNT(*) FROM `messages`  WHERE `messages`.`recipient_user_id` = ?d AND `messages`.`has_been_read` = ?d AND `messages`.`deleted_by_recipient` = ?d" {
			targetCluster = cluster
			break
		}
	}
	// targetCluster = modelBuilder.Clusters[0]
	// fmt.Printf("Cluster size: %d\n", len(targetCluster))
	// for _, query := range targetCluster[0] {
	// 	fmt.Printf("%d: %s\n", query.QueryID, query.GetSQL(modelBuilder.QuerySet))
	// }
	// fmt.Println(pt.GetTreeWithRoot(targetCluster[0][0].QueryID, 0).ToString())
	predictor := pt.NewPredictor(modelBuilder.QuerySet)
	trx := targetCluster[0]
	predictor.MoveToNext(trx[0])
	for _, query := range trx[1:] {
		prediction := predictor.PredictNextQuery()
		fmt.Println(prediction)
		predictor.MoveToNext(query)
	}
}

func TestBuildOrUpdateSingleTree(t *testing.T) {
	modelBuilder := NewModelBuilder("test/single_tree")
	pt := NewPredictionTrees()
	for i, cluster := range modelBuilder.Clusters {
		fmt.Printf("Cluster %d\n", i)
		modelBuilder.UpdateModel(cluster, pt)
	}
	targetCluster := modelBuilder.Clusters[1]
	fmt.Println(pt.GetTreeWithRoot(targetCluster[0][0].QueryID, 0).ToString())

	predictor := pt.NewPredictor(modelBuilder.QuerySet)
	trx := targetCluster[0]
	predictor.MoveToNext(trx[0])
	for _, query := range trx[1:] {
		prediction := predictor.PredictNextQuery()
		fmt.Printf("%+v\n", prediction)
		predictor.MoveToNext(query)
	}
}
