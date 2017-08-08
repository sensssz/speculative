package speculative

import (
	"fmt"
	"math"
	"reflect"
)

func floatEqual(num1 float64, num2 float64) bool {
	return math.Abs(num1-num2) < 0.00001
}

func interfaceEqual(i1 interface{}, i2 interface{}) bool {
	if reflect.TypeOf(i1) != reflect.TypeOf(i2) {
		return false
	}
	switch i1.(type) {
	case float64:
		if !floatEqual(i1.(float64), i2.(float64)) {
			return false
		}
	case *UnorderedSet:
		return i1.(*UnorderedSet).Equal(i2.(*UnorderedSet))
	case UnorderedSet:
		set1 := i1.(UnorderedSet)
		set2 := i2.(UnorderedSet)
		return set1.Equal(&set2)
	case []interface{}:
		if !sliceEqual(i1.([]interface{}), i2.([]interface{})) {
			return false
		}
	}
	return i1 == i2
}

func sliceEqual(s1 []interface{}, s2 []interface{}) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i, eleS1 := range s1 {
		eleS2 := s2[i]
		if !interfaceEqual(eleS1, eleS2) {
			return false
		}
	}
	return true
}

func valueEqual(val1 interface{}, val2 interface{}) bool {
	return interfaceEqual(val1, val2)
}

func queriesToString(queries []*Query) string {
	res := "["
	for i, query := range queries {
		if i == len(queries)-1 {
			res += fmt.Sprintf("Query{%d}", query.QueryID)
		} else {
			res += fmt.Sprintf("Query{%d}, ", query.QueryID)
		}
	}
	res += "]"
	return res
}

// Operand represents an operand involved in an operation.
type Operand interface {
	GetValue(trx []*Query) interface{}
	ToString() string
	Equal(operand Operand) bool
}

// ConstOperand represents a constant value.
type ConstOperand struct {
	Value interface{}
}

// GetValue returns the constant value.
func (op ConstOperand) GetValue(trx []*Query) interface{} {
	return op.Value
}

// ToString returns a string representation of this operand.
func (op ConstOperand) ToString() string {
	return fmt.Sprintf("%v", op.Value)
}

// Equal returns whether the two operands are equal.
func (op ConstOperand) Equal(operand Operand) bool {
	operandActual, ok := operand.(ConstOperand)
	if !ok {
		return false
	}
	return op == operandActual
}

// QueryResultOperand results an operand whose value comes
// from the result of a query.
type QueryResultOperand struct {
	QueryID     int
	QueryIndex  int
	RowIndex    int
	ColumnIndex int
}

// GetValue returns the value represented by this operand.
func (op QueryResultOperand) GetValue(trx []*Query) interface{} {
	if len(trx) <= op.QueryIndex {
		fmt.Printf("%+v, %+v\n", op, queriesToString(trx))
	}
	if op.QueryID != trx[op.QueryIndex].QueryID {
		fmt.Printf("%+v, %+v\n", op, queriesToString(trx))
		panic("Incorrect query")
	}
	queryResult := trx[op.QueryIndex].ResultSet
	if len(queryResult) <= op.RowIndex {
		return nil
	}
	return queryResult[op.RowIndex][op.ColumnIndex]
}

// ToString returns a string representation of this operand.
func (op QueryResultOperand) ToString() string {
	return fmt.Sprintf("Query%d[%d,%d]", op.QueryIndex, op.RowIndex, op.ColumnIndex)
}

// Equal returns whether the two operands are equal.
func (op QueryResultOperand) Equal(operand Operand) bool {
	operandActual, ok := operand.(QueryResultOperand)
	if !ok {
		return false
	}
	return op == operandActual
}

// QueryArgumentOperand represents an operand whose value comes
// from the argument list of a query.
type QueryArgumentOperand struct {
	QueryID    int
	QueryIndex int
	ArgIndex   int
}

// GetValue returns the value represented by this operand.
func (op QueryArgumentOperand) GetValue(trx []*Query) interface{} {
	if len(trx) <= op.QueryIndex {
		fmt.Printf("%+v, %+v\n", op, queriesToString(trx))
	}
	if op.QueryID != trx[op.QueryIndex].QueryID {
		fmt.Printf("%+v, %+v\n", op, queriesToString(trx))
		panic("Incorrect query")
	}
	arguments := trx[op.QueryIndex].Arguments
	return arguments[op.ArgIndex]
}

// ToString returns a string representation of this operand.
func (op QueryArgumentOperand) ToString() string {
	return fmt.Sprintf("Query%d(%d)", op.QueryIndex, op.ArgIndex)
}

// Equal returns whether the two operands are equal.
func (op QueryArgumentOperand) Equal(operand Operand) bool {
	operandActual, ok := operand.(QueryArgumentOperand)
	if !ok {
		return false
	}
	return op == operandActual
}

// Aggregator represents aggregation functions on float slice
type Aggregator func([]float64) float64

// AggregationOperand represents an operand whose value is the
// result of an aggregation on an float column of a query's result.
type AggregationOperand struct {
	QueryIndex  int
	Aggregation Aggregator
	ColumnIndex int
}

// GetValue returns the value represented by this operand.
func (op AggregationOperand) GetValue(trx []*Query) interface{} {
	queryResult := trx[op.QueryIndex].ResultSet
	column := make([]float64, 0, len(queryResult))
	for _, row := range queryResult {
		val := row[op.ColumnIndex]
		if val != nil {
			column = append(column, val.(float64))
		}
	}
	if len(column) > 0 {
		return op.Aggregation(column)
	}
	return 0.0
}

// ToString returns a string representation of this operand.
func (op AggregationOperand) ToString() string {
	return fmt.Sprintf("Query%d.aggregate(%d)", op.QueryIndex, op.ColumnIndex)
}

// Equal returns whether the two operands are equal.
func (op AggregationOperand) Equal(operand Operand) bool {
	operandActual, ok := operand.(AggregationOperand)
	if !ok {
		return false
	}
	return op.QueryIndex == operandActual.QueryIndex &&
		op.ColumnIndex == operandActual.ColumnIndex
}

// ArgumentListOperand represents an operand whose value is a list
// but comes from an argument of a query.
type ArgumentListOperand struct {
	QueryID    int
	QueryIndex int
	ArgIndex   int
}

// GetValue returns the value represented by this operand.
func (op ArgumentListOperand) GetValue(trx []*Query) interface{} {
	if len(trx) <= op.QueryIndex {
		fmt.Printf("%+v, %+v\n", op, queriesToString(trx))
	}
	if op.QueryID != trx[op.QueryIndex].QueryID {
		fmt.Printf("%+v, %+v\n", op, queriesToString(trx))
		panic("Incorrect query")
	}
	query := trx[op.QueryIndex]
	return query.Arguments[op.ArgIndex].(*UnorderedSet)
}

// ToString returns a string representation of this operand.
func (op ArgumentListOperand) ToString() string {
	return fmt.Sprintf("Query%d(%dl)", op.QueryIndex, op.ArgIndex)
}

// Equal returns whether the two operands are equal.
func (op ArgumentListOperand) Equal(operand Operand) bool {
	operandActual, ok := operand.(ArgumentListOperand)
	if !ok {
		return false
	}
	return op == operandActual
}

// ColumnListOperand represents an operand whose value is a list,
// which comes from a certain column of a query's result.
type ColumnListOperand struct {
	QueryID     int
	QueryIndex  int
	ColumnIndex int
}

// GetValue returns the value represented by this operand.
func (op ColumnListOperand) GetValue(trx []*Query) interface{} {
	if len(trx) <= op.QueryIndex {
		fmt.Printf("%+v, %+v\n", op, queriesToString(trx))
	}
	if op.QueryID != trx[op.QueryIndex].QueryID {
		fmt.Printf("%+v, %+v\n", op, queriesToString(trx))
		panic("Incorrect query")
	}
	queryResult := trx[op.QueryIndex].ResultSet
	column := NewEmptyUnorderedSet()
	for _, row := range queryResult {
		if op.ColumnIndex >= len(row) {
			fmt.Printf("%+v: %+v\n", trx[op.QueryIndex], op)
		}
		val := row[op.ColumnIndex]
		if val != nil {
			column.Insert(val)
		}
	}
	return column
}

// ToString returns a string representation of this operand.
func (op ColumnListOperand) ToString() string {
	return fmt.Sprintf("Query%d[%dl]", op.QueryIndex, op.ColumnIndex)
}

// Equal returns whether the two operands are equal.
func (op ColumnListOperand) Equal(operand Operand) bool {
	operandActual, ok := operand.(ColumnListOperand)
	if !ok {
		return false
	}
	return op == operandActual
}

// Operation represents an operation involving zero, one or more operands.
type Operation interface {
	GetValue(trx []*Query) interface{}
	MatchesValue(trx []*Query, value interface{}) bool
	ToString() string
}

// RandomOperation represents an operation, whose value matches anything.
type RandomOperation struct {
}

// GetValue returns the value of this operation.
func (op RandomOperation) GetValue(trx []*Query) interface{} {
	return nil
}

// MatchesValue is true for RandomOperation and any given value.
func (op RandomOperation) MatchesValue(trx []*Query, value interface{}) bool {
	return true
}

// ToString returns a string representation of this operation.
func (op RandomOperation) ToString() string {
	return ""
}

// UnaryOperation represents an unary operation.
type UnaryOperation struct {
	Operand Operand
}

// GetValue returns the value of this operation.
func (op UnaryOperation) GetValue(trx []*Query) interface{} {
	return op.Operand.GetValue(trx)
}

// MatchesValue returns whether the value of this operation matches
// the given value.
func (op UnaryOperation) MatchesValue(trx []*Query, value interface{}) bool {
	return valueEqual(op.Operand.GetValue(trx), value)
}

// ToString returns a string representation of this operation.
func (op UnaryOperation) ToString() string {
	return op.Operand.ToString()
}

// BinaryOperator represents a binary operator.
type BinaryOperator interface {
	Operate(leftOperand interface{}, rightOperand interface{}) float64
	IsSymmetrical() bool
	Name() string
}

// BinaryOperation represents an unary operation.
type BinaryOperation struct {
	Operator     BinaryOperator
	LeftOperand  Operand
	RightOperand Operand
}

// GetValue returns the value of this operation.
func (op BinaryOperation) GetValue(trx []*Query) interface{} {
	return op.Operator.Operate(op.LeftOperand.GetValue(trx), op.RightOperand.GetValue(trx))
}

// MatchesValue returns whether the value of this operation matches
// the given value.
func (op BinaryOperation) MatchesValue(trx []*Query, value interface{}) bool {
	val := op.Operator.Operate(op.LeftOperand.GetValue(trx), op.RightOperand.GetValue(trx))
	if reflect.TypeOf(val) == reflect.TypeOf(value) {
		return val == value
	}
	return false
}

// ToString returns a string representation of this operation.
func (op BinaryOperation) ToString() string {
	return op.LeftOperand.ToString() + " " + op.Operator.Name() + " " + op.RightOperand.ToString()
}

// Adder is able to add two numbers.
type Adder struct{}

// Operate for Adder adds two numbers, at least one of them being a float.
func (adder Adder) Operate(leftOperand interface{}, rightOperand interface{}) float64 {
	return leftOperand.(float64) + rightOperand.(float64)
}

// IsSymmetrical returns true for Adder.
func (adder Adder) IsSymmetrical() bool {
	return true
}

// Name of Adder
func (adder Adder) Name() string {
	return "+"
}

// Subtractor is able to substract two numbers.
type Subtractor struct{}

// Operate for Subtractor substracts two numbers, at least one of them being a float.
func (subtractor Subtractor) Operate(leftOperand interface{}, rightOperand interface{}) float64 {
	return leftOperand.(float64) - rightOperand.(float64)
}

// IsSymmetrical returns false for Subtractor.
func (subtractor Subtractor) IsSymmetrical() bool {
	return false
}

// Name of Subtractor
func (subtractor Subtractor) Name() string {
	return "-"
}

// Multiplier is able to multiply two numbers.
type Multiplier struct{}

// Operate for Multiplier multiplies two numbers, at least one of them being a float.
func (multiplier Multiplier) Operate(leftOperand interface{}, rightOperand interface{}) float64 {
	return leftOperand.(float64) * rightOperand.(float64)
}

// IsSymmetrical returns true for Multiplier.
func (multiplier Multiplier) IsSymmetrical() bool {
	return true
}

// Name of Multiplier
func (multiplier Multiplier) Name() string {
	return "*"
}

// Divider is able to divide two numbers.
type Divider struct{}

// Operate for Divider divides two numbers, at least one of them being a float.
func (divider Divider) Operate(leftOperand interface{}, rightOperand interface{}) float64 {
	return leftOperand.(float64) / rightOperand.(float64)
}

// IsSymmetrical returns false for Divider.
func (divider Divider) IsSymmetrical() bool {
	return true
}

// Name of Divider
func (divider Divider) Name() string {
	return "/"
}

// Moduloer is able to calculate the mod of two numbers.
type Moduloer struct{}

// Operate for Moduloer mods two numbers, at least one of them being a float.
func (moduloer Moduloer) Operate(leftOperand interface{}, rightOperand interface{}) float64 {
	return float64(math.NaN())
}

// IsSymmetrical returns false for Moduloer.
func (moduloer Moduloer) IsSymmetrical() bool {
	return true
}

// Name of Moduloer
func (moduloer Moduloer) Name() string {
	return "%"
}

// Sum returns the sum of the numbers
func Sum(nums []float64) float64 {
	var sum float64
	for _, num := range nums {
		sum += num
	}
	return sum
}

// Avg returns the average of the numbers
func Avg(nums []float64) float64 {
	var sum float64
	for _, num := range nums {
		sum += num
	}
	return sum / float64(len(nums))
}

// Len returns the numbers of the numbers
func Len(nums []float64) float64 {
	return float64(len(nums))
}

// Max returns the max of the numbers
func Max(nums []float64) float64 {
	max := nums[0]
	for _, num := range nums {
		if num > max {
			max = num
		}
	}
	return max
}

// Min returns the min of the numbers
func Min(nums []float64) float64 {
	min := nums[0]
	for _, num := range nums {
		if num < min {
			min = num
		}
	}
	return min
}

// BinaryOperators contains all supported BinaryOperator
var BinaryOperators = []BinaryOperator{Adder{}, Subtractor{}, Multiplier{}, Divider{}, Moduloer{}}

// Aggregators contains all supported Aggregator
var Aggregators = []Aggregator{Sum, Avg, Len, Max, Min}
