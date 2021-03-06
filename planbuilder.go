/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package replicator

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/nicholaskh/opendts/log"
	"github.com/nicholaskh/opendts/evalengine"
	"github.com/nicholaskh/opendts/mysql"
	"github.com/nicholaskh/opendts/proto"
	"github.com/nicholaskh/opendts/sqlparser"
	"github.com/nicholaskh/opendts/sqltypes"
)

// Plan represents the plan for a table.
type Plan struct {
	Table *Table

	// ColExprs is the list of column expressions to be sent
	// in the stream.
	ColExprs []ColExpr

	// Filters is the list of filters to be applied to the columns
	// of the table.
	Filters []Filter
}

// Opcode enumerates the operators supported in a where clause
type Opcode int

const (
	// Equal is used to filter an integer column on a specific value
	Equal = Opcode(iota)
)

// Filter contains opcodes for filtering.
type Filter struct {
	Opcode Opcode
	ColNum int
	Value  sqltypes.Value
}

// ColExpr represents a column expression.
type ColExpr struct {
	// ColNum specifies the source column value.
	ColNum int

	Field *proto.Field

	FixedValue sqltypes.Value
}

// Table contains the metadata for a table.
type Table struct {
	Name   string
	Fields []*proto.Field
}

// fields returns the fields for the plan.
func (plan *Plan) fields() []*proto.Field {
	fields := make([]*proto.Field, len(plan.ColExprs))
	for i, ce := range plan.ColExprs {
		fields[i] = ce.Field
	}
	return fields
}

// filter filters the row against the plan. It returns false if the row did not match.
// If the row matched, it returns the columns to be sent.
func (plan *Plan) filter(values []sqltypes.Value) (bool, []sqltypes.Value, error) {
	for _, filter := range plan.Filters {
		switch filter.Opcode {
		case Equal:
			result, err := evalengine.NullsafeCompare(values[filter.ColNum], filter.Value)
			if err != nil {
				return false, nil, err
			}
			if result != 0 {
				return false, nil, nil
			}
		}
	}

	result := make([]sqltypes.Value, len(plan.ColExprs))
	for i, colExpr := range plan.ColExprs {
		if colExpr.ColNum == -1 {
			result[i] = colExpr.FixedValue
			continue
		}
		if colExpr.ColNum >= len(values) {
			return false, nil, fmt.Errorf("index out of range, colExpr.ColNum: %d, len(values): %d", colExpr.ColNum, len(values))
		}
		result[i] = values[colExpr.ColNum]
	}
	return true, result, nil
}

func mustSendStmt(query mysql.Query, dbnames map[string]struct{}) bool {
	if _, ok := dbnames[query.Database]; !ok && query.Database != "" {
		return false
	}
	return true
}

func mustSendDDL(query mysql.Query, filter *proto.Filter) (*proto.Rule, bool) {
	// TODO ??????SQL parser??????????????????
	ast, err := sqlparser.Parse(query.SQL)
	// If there was a parsing error, we send it through. Hopefully,
	// recipient can handle it.
	if err != nil {
		return nil, true
	}
	switch stmt := ast.(type) {
	case sqlparser.DBDDLStatement:
		return nil, false
	case sqlparser.DDLStatement:
		var rule *proto.Rule
		if !stmt.GetTable().IsEmpty() {
			rule = tableMatches(query.Database, stmt.GetTable().Name.String(), filter)
			return rule, rule != nil
		}
		for _, table := range stmt.GetFromTables() {
			if rule = tableMatches(query.Database, table.Name.String(), filter); rule != nil {
				return rule, true
			}
		}
		for _, table := range stmt.GetToTables() {
			if rule := tableMatches(query.Database, table.Name.String(), filter); rule != nil {
				return rule, true
			}
		}
		return nil, false
	}
	return nil, true
}

// ruleMatches is similar to tableMatches and buildPlan defined in vstreamer/planbuilder.go.
func ruleMatches(dbname, tableName string, filter *proto.Filter) (*proto.Rule, error) {
	tables := make([]string, 0, 4)
	tables = append(tables, tableName)
	if table := ghoSourceTable(tableName); len(table) != len(tableName)-4 {
		tables = append(tables, table)
	}
	if table := ghcSourceTable(tableName); len(table) != len(tableName)-4 {
		tables = append(tables, table)
	}
	if table := delSourceTable(tableName); len(table) != len(tableName)-4 {
		tables = append(tables, table)
	}
	for _, rule := range filter.Rules {
		if dbname != rule.Dbname {
			continue
		}
		for _, table := range tables {
			switch rule.Type {
			case proto.RuleTypeRegexp:
				result, err := regexp.MatchString(rule.Regexp, table)
				if err != nil {
					return nil, err
				}
				if !result {
					continue
				}
				return rule, nil
			case proto.RuleTypeTables:
				if _, ok := rule.Tables[table]; ok {
					return rule, nil
				}
			case proto.RuleTypeTable:
				if table == rule.Table {
					return rule, nil
				}
			}
		}
	}
	return nil, nil
}

// tableMatches is similar to buildPlan below and MatchTable in vreplication/table_plan_builder.go.
func tableMatches(dbname, tableName string, filter *proto.Filter) *proto.Rule {
	rule, _ := ruleMatches(dbname, tableName, filter)
	return rule
}

// use for gh-ost
func ghoSourceTable(tableName string) string {
	return strings.TrimSuffix(strings.TrimPrefix(tableName, "_"), "_gho")
}

func ghcSourceTable(tableName string) string {
	return strings.TrimSuffix(strings.TrimPrefix(tableName, "_"), "_ghc")
}

func delSourceTable(tableName string) string {
	return strings.TrimSuffix(strings.TrimPrefix(tableName, "_"), "_del")
}

func buildPlan(ti *Table, filter *proto.Filter) (*Plan, error) {
	for _, rule := range filter.Rules {
		switch rule.Type {
		case proto.RuleTypeRegexp:
			result, err := regexp.MatchString(rule.Regexp, ti.Name)
			if err != nil {
				return nil, err
			}
			if !result {
				continue
			}
			return buildCommonPlan(ti)
		case proto.RuleTypeTables:
			if _, ok := rule.Tables[ti.Name]; ok {
				return buildCommonPlan(ti)
			}
		case proto.RuleTypeTable:
			if rule.Table == ti.Name || rule.Table == ghoSourceTable(ti.Name) || rule.Table == ghcSourceTable(ti.Name) {
				return buildTablePlan(ti, rule.Filter)
			}
		}
	}
	return nil, nil
}

// buildREPlan handles cases where Match has a regular expression.
// If so, the Filter can be an empty string or a keyrange, like "-80".
func buildCommonPlan(ti *Table) (*Plan, error) {
	plan := &Plan{
		Table: ti,
	}
	plan.ColExprs = make([]ColExpr, len(ti.Fields))
	for i, col := range ti.Fields {
		plan.ColExprs[i].ColNum = i
		plan.ColExprs[i].Field = col
	}

	return plan, nil
}

// BuildTablePlan handles cases where a specific table name is specified.
// The filter must be a select statement.
func buildTablePlan(ti *Table, query string) (*Plan, error) {
	sel, fromTable, err := analyzeSelect(query)
	if err != nil {
		log.Warn("%s", err.Error())
		return nil, err
	}
	if fromTable.String() != ti.Name && fromTable.String() != ghoSourceTable(ti.Name) && fromTable.String() != ghcSourceTable(ti.Name) {
		log.Warn("unsupported: select expression table %v does not match the table entry name %s", sqlparser.String(fromTable), ti.Name)
		return nil, fmt.Errorf("unsupported: select expression table %v does not match the table entry name %s", sqlparser.String(fromTable), ti.Name)
	}

	plan := &Plan{
		Table: ti,
	}
	if err := plan.analyzeWhere(sel.Where); err != nil {
		log.Warn("%s", err.Error())
		return nil, err
	}
	if err := plan.analyzeExprs(sel.SelectExprs); err != nil {
		log.Warn("%s", err.Error())
		return nil, err
	}

	if sel.Where == nil {
		return plan, nil
	}

	return plan, nil
}

func analyzeSelect(query string) (sel *sqlparser.Select, fromTable sqlparser.TableIdent, err error) {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, fromTable, err
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, fromTable, fmt.Errorf("unsupported: %v", sqlparser.String(statement))
	}
	if len(sel.From) > 1 {
		return nil, fromTable, fmt.Errorf("unsupported: %v", sqlparser.String(sel))
	}
	node, ok := sel.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fromTable, fmt.Errorf("unsupported: %v", sqlparser.String(sel))
	}
	fromTable = sqlparser.GetTableName(node.Expr)
	if fromTable.IsEmpty() {
		return nil, fromTable, fmt.Errorf("unsupported: %v", sqlparser.String(sel))
	}
	return sel, fromTable, nil
}

func (plan *Plan) analyzeWhere(where *sqlparser.Where) error {
	if where == nil {
		return nil
	}
	exprs := splitAndExpression(nil, where.Expr)
	for _, expr := range exprs {
		switch expr := expr.(type) {
		case *sqlparser.ComparisonExpr:
			qualifiedName, ok := expr.Left.(*sqlparser.ColName)
			if !ok {
				return fmt.Errorf("unexpected: %v", sqlparser.String(expr))
			}
			if !qualifiedName.Qualifier.IsEmpty() {
				return fmt.Errorf("unsupported qualifier for column: %v", sqlparser.String(qualifiedName))
			}
			colnum, err := findColumn(plan.Table, qualifiedName.Name)
			if err != nil {
				return err
			}
			val, ok := expr.Right.(*sqlparser.Literal)
			if !ok {
				return fmt.Errorf("unexpected: %v", sqlparser.String(expr))
			}
			// NOTE StrVal is varbinary, we do not support varchar since we would have to implement all collation types
			if val.Type != sqlparser.IntVal && val.Type != sqlparser.StrVal {
				return fmt.Errorf("unexpected: %v", sqlparser.String(expr))
			}
			pv, err := sqlparser.NewPlanValue(val)
			if err != nil {
				return err
			}
			resolved, err := pv.ResolveValue(nil)
			if err != nil {
				return err
			}
			plan.Filters = append(plan.Filters, Filter{
				Opcode: Equal,
				ColNum: colnum,
				Value:  resolved,
			})
		default:
			return fmt.Errorf("unsupported constraint: %v", sqlparser.String(expr))
		}
	}
	return nil
}

// splitAndExpression breaks up the Expr into AND-separated conditions
// and appends them to filters, which can be shuffled and recombined
// as needed.
func splitAndExpression(filters []sqlparser.Expr, node sqlparser.Expr) []sqlparser.Expr {
	if node == nil {
		return filters
	}
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		filters = splitAndExpression(filters, node.Left)
		return splitAndExpression(filters, node.Right)
	}
	return append(filters, node)
}

func (plan *Plan) analyzeExprs(selExprs sqlparser.SelectExprs) error {
	if _, ok := selExprs[0].(*sqlparser.StarExpr); !ok {
		for _, expr := range selExprs {
			cExpr, err := plan.analyzeExpr(expr)
			if err != nil {
				return err
			}
			plan.ColExprs = append(plan.ColExprs, cExpr)
		}
	} else {
		if len(selExprs) != 1 {
			return fmt.Errorf("unsupported: %v", sqlparser.String(selExprs))
		}
		plan.ColExprs = make([]ColExpr, len(plan.Table.Fields))
		for i, col := range plan.Table.Fields {
			plan.ColExprs[i].ColNum = i
			plan.ColExprs[i].Field = col
		}
	}
	return nil
}

func (plan *Plan) analyzeExpr(selExpr sqlparser.SelectExpr) (cExpr ColExpr, err error) {
	aliased, ok := selExpr.(*sqlparser.AliasedExpr)
	if !ok {
		return ColExpr{}, fmt.Errorf("unsupported: %v", sqlparser.String(selExpr))
	}
	switch inner := aliased.Expr.(type) {
	case *sqlparser.ColName:
		if !inner.Qualifier.IsEmpty() {
			return ColExpr{}, fmt.Errorf("unsupported qualifier for column: %v", sqlparser.String(inner))
		}
		colnum, err := findColumn(plan.Table, inner.Name)
		if err != nil {
			return ColExpr{}, err
		}
		as := aliased.As
		if as.IsEmpty() {
			as = sqlparser.NewColIdent(sqlparser.String(aliased.Expr))
		}
		return ColExpr{
			ColNum: colnum,
			Field:  plan.Table.Fields[colnum],
		}, nil
	case *sqlparser.Literal:
		//allow only intval 1
		if inner.Type != sqlparser.IntVal {
			return ColExpr{}, fmt.Errorf("only integer literals are supported")
		}
		num, err := strconv.ParseInt(string(inner.Val), 0, 64)
		if err != nil {
			return ColExpr{}, err
		}
		if num != 1 {
			return ColExpr{}, fmt.Errorf("only the integer literal 1 is supported")
		}
		return ColExpr{
			Field: &proto.Field{
				Name: "1",
				Type: proto.Type_INT64,
			},
			ColNum:     -1,
			FixedValue: sqltypes.NewInt64(num),
		}, nil
	default:
		log.Trace("Unsupported expression: %v", inner)
		return ColExpr{}, fmt.Errorf("unsupported: %v", sqlparser.String(aliased.Expr))
	}
}

func selString(expr sqlparser.SelectExpr) (string, error) {
	aexpr, ok := expr.(*sqlparser.AliasedExpr)
	if !ok {
		return "", fmt.Errorf("unsupported: %v", sqlparser.String(expr))
	}
	val, ok := aexpr.Expr.(*sqlparser.Literal)
	if !ok {
		return "", fmt.Errorf("unsupported: %v", sqlparser.String(expr))
	}
	return string(val.Val), nil
}

func findColumn(ti *Table, name sqlparser.ColIdent) (int, error) {
	for i, col := range ti.Fields {
		if name.EqualString(col.Name) {
			return i, nil
		}
	}
	return 0, fmt.Errorf("column %s not found in table %s", sqlparser.String(name), ti.Name)
}
