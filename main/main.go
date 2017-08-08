package main

import (
	"fmt"
	"sort"

	"bufio"

	"os"

	"strings"

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

type Pair struct {
	ClusterID int
	Frequency int
	Matches   int
	Total     int
}
type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Frequency < p[j].Frequency }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func main() {
	modelBuilder := sqp.NewModelBuilder("/home/jiamin/sql.log")
	var total int64
	var totalSelect int64
	var userID int64
	var match int64
	var numTrx int64
	var wrongPrediction int64
	var unpredictale int64
	total = 0
	totalSelect = 0
	userID = 0
	match = 0
	wrongPrediction = 0
	unpredictale = 0
	pt := sqp.NewPredictionTrees()
	spinner := sp.NewSpinnerWithProgress(19, "Creating model for cluster %d", -1)
	spinner.Start()
	for i, cluster := range modelBuilder.Clusters {
		spinner.UpdateProgress(i)
		if len(cluster[0]) < 10 {
			continue
		}
		thirtyPercent := int(float64(len(cluster)) * 0.3)
		if thirtyPercent > 1 {
			modelBuilder.UpdateModel(cluster[:thirtyPercent], pt)
		}
	}
	spinner.Stop()
	predictor := pt.NewPredictor(modelBuilder.QuerySet)
	spinner = sp.NewSpinnerWithProgress(19, "Performaning preduction for cluster %d...", -1)
	spinner.Start()
	clusterInfoFile, _ := os.Create("clusterInfo")
	defer clusterInfoFile.Close()
	fileWriter := bufio.NewWriter(clusterInfoFile)
	pl := make(PairList, 0, len(modelBuilder.Clusters))
	for i, cluster := range modelBuilder.Clusters {
		spinner.UpdateProgress(i)
		if len(cluster[0]) < 10 {
			continue
		}
		thirtyPercent := int(float64(len(cluster)) * 0.3)
		if thirtyPercent <= 1 {
			continue
		}
		matchOfTrx := 0
		totalSelectOfTrx := 0
		for j, trx := range cluster[thirtyPercent+1:] {
			if trx[0].IsSelect {
				totalSelect++
				if j == 0 {
					totalSelectOfTrx++
				}
			}
			numTrx++
			total++
			predictor.MoveToNext(trx[0])
			for _, query := range trx[1:] {
				total++
				if query.IsSelect {
					totalSelect++
					if j == 0 {
						totalSelectOfTrx++
					}
				}
				actualSQL := query.GetSQL(modelBuilder.QuerySet)
				if strings.Contains(actualSQL, "`user`.`id` = ") ||
					strings.Contains(actualSQL, ".`user_id` = ") {
					userID++
				}
				prediction := predictor.PredictNextQuery()
				if query.Same(prediction) {
					match++
					if j == 0 {
						matchOfTrx++
					}
				} else if prediction != nil {
					wrongPrediction++
					// predictedSQL := prediction.GetSQL(modelBuilder.QuerySet)
					// fileWriter.WriteString(fmt.Sprintf("Expecting:\n\t%s\ngot:\n\t%s\n\n", actualSQL, predictedSQL))
				} else {
					unpredictale++
				}
				predictor.MoveToNext(query)
			}
			predictor.EndTransaction()
		}
		pl = append(pl, Pair{i, len(cluster) - thirtyPercent, matchOfTrx, totalSelectOfTrx})
	}
	sort.Sort(sort.Reverse(pl))
	predictableClusters := 0
	totalPredictableTrx := 0
	for _, pair := range pl {
		percent := float64(100*pair.Frequency) / float64(numTrx)
		fileWriter.WriteString(fmt.Sprintf("Cluster %d: %d  %.2f%%  %d/%d\n", pair.ClusterID, pair.Frequency, percent, pair.Matches, pair.Total))
		if float64(pair.Matches)/float64(pair.Total) >= 0.8 {
			predictableClusters++
			totalPredictableTrx += len(modelBuilder.Clusters[pair.ClusterID])
		}
		for _, query := range modelBuilder.Clusters[pair.ClusterID][0] {
			fileWriter.WriteString(query.GetSQL(modelBuilder.QuerySet) + "\n")
		}
		fileWriter.WriteString("\n")
	}
	spinner.Stop()
	fileWriter.WriteString(fmt.Sprintf("Hit count: %v\n", match))
	fileWriter.WriteString(fmt.Sprintf("Unpredictable: %v\n", unpredictale))
	fileWriter.WriteString(fmt.Sprintf("Wrong prediction: %v\n", wrongPrediction))
	fileWriter.WriteString(fmt.Sprintf("Total select: %v\n", totalSelect))
	fileWriter.WriteString(fmt.Sprintf("Total queries: %v\n", total))
	fileWriter.WriteString(fmt.Sprintf("Num trx: %d\n", numTrx))
	fileWriter.WriteString(fmt.Sprintf("Average trx size: %d\n", total/numTrx))
	fileWriter.WriteString(fmt.Sprintf("Total number of predictable transactions: %d\n", totalPredictableTrx))
	fileWriter.WriteString(fmt.Sprintf("Total number of transaction types: %d\n", len(modelBuilder.Clusters)))
	fileWriter.WriteString(fmt.Sprintf("Number of transaction types with more than 80%% predictability: %d\n", predictableClusters))
	fileWriter.Flush()
	fmt.Printf("Hit count: %v\n", match)
	fmt.Printf("Unpredictable: %v\n", unpredictale)
	fmt.Printf("Wrong prediction: %v\n", wrongPrediction)
	fmt.Printf("Total select: %v\n", totalSelect)
	fmt.Printf("Total queries: %v\n", total)
	fmt.Printf("Num trx: %d\n", numTrx)
	fmt.Printf("Average trx size: %d\n", total/numTrx)
	fmt.Printf("Total number of predictable transactions: %d\n", totalPredictableTrx)
	fmt.Printf("Total number of transaction types: %d\n", len(modelBuilder.Clusters))
	fmt.Printf("Number of transaction types with more than 80%% predictability: %d\n", predictableClusters)
}
