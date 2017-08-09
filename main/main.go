package main

import (
	"fmt"

	sqp "github.com/sensssz/speculative"
	sp "github.com/sensssz/spinner"
)

func queriesToString(queries []*sqp.Query) string {
	res := "["
	for i, query := range queries {
		if i == len(queries)-1 {
			res += fmt.Sprintf("Query{%d, %+v}", query.QueryID, query.Arguments)
		} else {
			res += fmt.Sprintf("Query{%d, %+v}, ", query.QueryID, query.Arguments)
		}
	}
	res += "]"
	return res
}

func nonNegative(num int) int {
	if num > 0 {
		return num
	}
	return 0
}

func main() {
	postfix := ".lobsters"
	modelBuilder := sqp.NewModelBuilder("/home/jiamin/sql_log/sql" + postfix)
	var total int64
	var match int64
	var wrongPrediction int64
	var unpredictale int64
	total = 0
	match = 0
	wrongPrediction = 0
	unpredictale = 0
	gm := sqp.NewGraphModel()
	queries := modelBuilder.Queries
	thirtyPercent := int(float64(len(queries)) * 0.3)
	modelBuilder.TrainModel(gm, thirtyPercent)
	gm.Print()
	predictor := gm.NewPredictor(modelBuilder.QuerySet)
	spinner := sp.NewSpinnerWithProgress(19, "Performaning prediction for query %d...", -1)
	spinner.Start()
	total = len(queries) - thirtyPercent
	for i, query := range queries[thirtyPercent:] {
		spinner.UpdateProgress(i)
		prediction := predictor.PredictNextQuery()
		if query.Same(prediction) {
			match++
		} else if prediction != nil {
			wrongPrediction++
		} else {
			unpredictale++
		}
		predictor.MoveToNext(query)
	}
	spinner.Stop()
	fmt.Printf("Hit count: %v\n", match)
	fmt.Printf("Unpredictable: %v\n", unpredictale)
	fmt.Printf("Wrong prediction: %v\n", wrongPrediction)
	fmt.Printf("Total queries: %v\n", total)
}
