package replicator

import (
	"fmt"
	"sort"
	"strings"

	"git.100tal.com/wangxiao_jichujiagou_common/dts/proto"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/sqlparser"
	"git.100tal.com/wangxiao_jichujiagou_common/dts/sqltypes"
)

// This file contains just the builders for ReplicatorPlan and TablePlan.
// ReplicatorPlan and TablePlan are in replicator_plan.go.
// TODO(sougou): reorganize this in a better fashion.

// ExcludeStr is the filter value for excluding tables that match a rule.
// TODO(sougou): support this on vstreamer side also.
const ExcludeStr = "exclude"

// tablePlanBuilder contains the metadata needed for building a TablePlan.
type tablePlanBuilder struct {
	targetDbname sqlparser.TableIdent
	dbname       sqlparser.TableIdent
	name         sqlparser.TableIdent
	sendSelect   *sqlparser.Select
	// selColumns keeps track of the columns we want to pull from source.
	// If Lastpk is set, we compare this list against the table's pk and
	// add missing references.
	selColumns map[string]bool
	colExprs   []*colExpr
	onInsert   insertType
	pkCols     []*colExpr
	lastpk     *sqltypes.Result
	pkInfos    []*PrimaryKeyInfo
}

// colExpr describes the processing to be performed to
// compute the value of one column of the target table.
type colExpr struct {
	colName sqlparser.ColIdent
	colType proto.Type
	// operation==opExpr: full expression is set
	// operation==opCount: nothing is set.
	// operation==opSum: for 'sum(a)', expr is set to 'a'.
	operation operation
	// expr stores the expected field name from vstreamer and dictates
	// the generated bindvar names, like a_col or b_col.
	expr sqlparser.Expr
	// references contains all the column names referenced in the expression.
	references map[string]bool

	isGrouped  bool
	isPK       bool
	dataType   string
	columnType string
}

// operation is the opcode for the colExpr.
type operation int

// The following values are the various colExpr opcodes.
const (
	opExpr = operation(iota)
	opCount
	opSum
)

// insertType describes the type of insert statement to generate.
// Please refer to TestBuildPlayerPlan for examples.
type insertType int

// The following values are the various insert types.
const (
	// insertNormal is for normal selects without a group by, like
	// "select a+b as c from t".
	insertNormal = insertType(iota)
	// insertOnDup is for the more traditional grouped expressions, like
	// "select a, b, count(*) as c from t group by a". For statements
	// like these, "insert.. on duplicate key" statements will be generated
	// causing "b" to be updated to the latest value (last value wins).
	insertOnDup
	// insertIgnore is for special grouped expressions where all columns are
	// in the group by, like "select a, b, c from t group by a, b, c".
	// This generates "insert ignore" statements (first value wins).
	insertIgnore
)

// buildReplicatorPlan builds a ReplicatorPlan for the tables that match the filter.
// The filter is matched against the target schema. For every table matched,
// a table-specific rule is built to be sent to the source. We don't send the
// original rule to the source because it may not match the same tables as the
// target.
// pkInfoMap specifies the list of primary key columns for each table.
// copyState is a map of tables that have not been fully copied yet.
// If a table is not present in copyState, then it has been fully copied. If so,
// all replication events are applied. The table still has to match a Filter.Rule.
// If it has a non-nil entry, then the value is the last primary key (lastpk)
// that was copied.  If so, only replication events < lastpk are applied.
// If the entry is nil, then copying of the table has not started yet. If so,
// no events are applied.
// The TablePlan built is a partial plan. The full plan for a table is built
// when we receive field information from events or rows sent by the source.
// buildExecutionPlan is the function that builds the full plan.
func buildReplicatorPlan(filter *proto.Filter, pkInfoMap map[string]map[string][]*PrimaryKeyInfo, copyState map[string]map[string]*sqltypes.Result) (*ReplicatorPlan, error) {
	plan := &ReplicatorPlan{
		VStreamFilter: &proto.Filter{},
		TargetTables:  make(map[string]map[string]*TablePlan),
		TablePlans:    make(map[string]map[string]*TablePlan),
		PKInfoMap:     pkInfoMap,
	}
	for dbname, pkInfoTable := range pkInfoMap {
		targetTables := make(map[string]*TablePlan)
		tablePlans := make(map[string]*TablePlan)
		for tableName, _ := range pkInfoTable {
			lastpk, ok := copyState[dbname][tableName]
			if ok && lastpk == nil {
				// Don't replicate uncopied tables.
				continue
			}
			rule, err := ruleMatches(dbname, tableName, filter)
			if err != nil {
				return nil, err
			}
			if rule == nil {
				continue
			}
			tablePlan, err := buildReplicatorTablePlan(dbname, tableName, rule.Filter, rule.TargetDbname, pkInfoMap, lastpk)
			if err != nil {
				return nil, err
			}
			if tablePlan == nil {
				// Table was excluded.
				continue
			}
			if _, ok := plan.TablePlans[tablePlan.Dbname]; ok {
				if dup, ok := plan.TablePlans[tablePlan.Dbname][tablePlan.TargetName]; ok {
					return nil, fmt.Errorf("more than one target for source table %s: %s and %s", tablePlan.SendRule.Table, dup.TargetName, tableName)
				}
			}
			plan.VStreamFilter.Rules = append(plan.VStreamFilter.Rules, tablePlan.SendRule)
			targetTables[tableName] = tablePlan
			tablePlans[tablePlan.SendRule.Table] = tablePlan
		}
		plan.TargetTables[dbname] = targetTables
		plan.TablePlans[dbname] = tablePlans
	}
	return plan, nil
}

func buildReplicatorTablePlan(dbname, tableName, filter, targetDbname string, pkInfoMap map[string]map[string][]*PrimaryKeyInfo, lastpk *sqltypes.Result) (*TablePlan, error) {
	query := filter
	// generate equivalent select statement if filter is empty or a keyrange.
	switch {
	case filter == "":
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("select * from %v", sqlparser.NewTableIdent(tableName))
		query = buf.String()
	case filter == ExcludeStr:
		return nil, nil
	}
	sel, _, err := analyzeSelectFrom(query)
	if err != nil {
		return nil, err
	}
	sendRule := &proto.Rule{
		TargetDbname: targetDbname,
		Dbname:       dbname,
		Table:        tableName,
	}

	if expr, ok := sel.SelectExprs[0].(*sqlparser.StarExpr); ok {
		// If it's a "select *", we return a partial plan, and complete
		// it when we get back field info from the stream.
		if len(sel.SelectExprs) != 1 {
			return nil, fmt.Errorf("unexpected: %v", sqlparser.String(sel))
		}
		if !expr.TableName.IsEmpty() {
			return nil, fmt.Errorf("unsupported qualifier for '*' expression: %v", sqlparser.String(expr))
		}
		sendRule.Filter = query
		tablePlan := &TablePlan{
			TargetDbname: targetDbname,
			Dbname:       dbname,
			TargetName:   tableName,
			SendRule:     sendRule,
			Lastpk:       lastpk,
			DbTopic:      dbname,
			TbTopic:      dbname + "_" + tableName,
		}
		return tablePlan, nil
	}

	tpb := &tablePlanBuilder{
		targetDbname: sqlparser.NewTableIdent(targetDbname),
		dbname:       sqlparser.NewTableIdent(dbname),
		name:         sqlparser.NewTableIdent(tableName),
		sendSelect: &sqlparser.Select{
			From:  sel.From,
			Where: sel.Where,
		},
		selColumns: make(map[string]bool),
		lastpk:     lastpk,
		pkInfos:    pkInfoMap[dbname][tableName],
	}

	if err := tpb.analyzeExprs(sel.SelectExprs); err != nil {
		return nil, err
	}
	// It's possible that the target table does not materialize all
	// the primary keys of the source table. In such situations,
	// we still have to be able to validate the incoming event
	// against the current lastpk. For this, we have to request
	// the missing columns so we can compare against those values.
	// If there is no lastpk to validate against, then we don't
	// care.
	if tpb.lastpk != nil {
		for _, f := range tpb.lastpk.Fields {
			tpb.addCol(sqlparser.NewColIdent(f.Name))
		}
	}
	if err := tpb.analyzeGroupBy(sel.GroupBy); err != nil {
		return nil, err
	}
	if err := tpb.analyzePK(pkInfoMap); err != nil {
		return nil, err
	}

	// if there are no columns being selected the select expression can be empty, so we "select 1" so we have a valid
	// select to get a row back
	if len(tpb.sendSelect.SelectExprs) == 0 {
		tpb.sendSelect.SelectExprs = sqlparser.SelectExprs([]sqlparser.SelectExpr{
			&sqlparser.AliasedExpr{
				Expr: sqlparser.NewIntLiteral([]byte{'1'}),
			},
		})
	}
	sendRule.Filter = sqlparser.String(tpb.sendSelect)

	tablePlan := tpb.generate()
	tablePlan.SendRule = sendRule
	return tablePlan, nil
}

func (tpb *tablePlanBuilder) generate() *TablePlan {
	refmap := make(map[string]bool)
	for _, cexpr := range tpb.pkCols {
		for k := range cexpr.references {
			refmap[k] = true
		}
	}
	if tpb.lastpk != nil {
		for _, f := range tpb.lastpk.Fields {
			refmap[f.Name] = true
		}
	}
	pkrefs := make([]string, 0, len(refmap))
	for k := range refmap {
		pkrefs = append(pkrefs, k)
	}
	sort.Strings(pkrefs)

	bvf := &bindvarFormatter{}

	return &TablePlan{
		Dbname:           tpb.dbname.String(),
		TargetName:       tpb.name.String(),
		TargetDbname:     tpb.targetDbname.String(),
		DbTopic:          tpb.dbname.String(),
		TbTopic:          tpb.dbname.String() + "_" + tpb.name.String(),
		Lastpk:           tpb.lastpk,
		BulkInsertFront:  tpb.generateInsertPart(sqlparser.NewTrackedBuffer(bvf.formatter)),
		BulkInsertValues: tpb.generateValuesPart(sqlparser.NewTrackedBuffer(bvf.formatter), bvf),
		BulkInsertOnDup:  tpb.generateOnDupPart(sqlparser.NewTrackedBuffer(bvf.formatter)),
		Insert:           tpb.generateInsertStatement(),
		Update:           tpb.generateUpdateStatement(),
		Delete:           tpb.generateDeleteStatement(),
		PKReferences:     pkrefs,
	}
}

func analyzeSelectFrom(query string) (sel *sqlparser.Select, from string, err error) {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, "", err
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, "", fmt.Errorf("unexpected: %v", sqlparser.String(statement))
	}
	if sel.Distinct {
		return nil, "", fmt.Errorf("unexpected: %v", sqlparser.String(sel))
	}
	if len(sel.From) > 1 {
		return nil, "", fmt.Errorf("unexpected: %v", sqlparser.String(sel))
	}
	node, ok := sel.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, "", fmt.Errorf("unexpected: %v", sqlparser.String(sel))
	}
	fromTable := sqlparser.GetTableName(node.Expr)
	if fromTable.IsEmpty() {
		return nil, "", fmt.Errorf("unexpected: %v", sqlparser.String(sel))
	}
	return sel, fromTable.String(), nil
}

func (tpb *tablePlanBuilder) analyzeExprs(selExprs sqlparser.SelectExprs) error {
	for _, selExpr := range selExprs {
		cexpr, err := tpb.analyzeExpr(selExpr)
		if err != nil {
			return err
		}
		tpb.colExprs = append(tpb.colExprs, cexpr)
	}
	return nil
}

func (tpb *tablePlanBuilder) analyzeExpr(selExpr sqlparser.SelectExpr) (*colExpr, error) {
	aliased, ok := selExpr.(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(selExpr))
	}
	as := aliased.As
	if as.IsEmpty() {
		// Require all non-trivial expressions to have an alias.
		if colAs, ok := aliased.Expr.(*sqlparser.ColName); ok && colAs.Qualifier.IsEmpty() {
			as = colAs.Name
		} else {
			return nil, fmt.Errorf("expression needs an alias: %v", sqlparser.String(aliased))
		}
	}
	cexpr := &colExpr{
		colName:    as,
		references: make(map[string]bool),
	}
	if expr, ok := aliased.Expr.(*sqlparser.FuncExpr); ok {
		if expr.Distinct {
			return nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
		}
		switch fname := expr.Name.Lowered(); fname {
		case "count":
			if _, ok := expr.Exprs[0].(*sqlparser.StarExpr); !ok {
				return nil, fmt.Errorf("only count(*) is supported: %v", sqlparser.String(expr))
			}
			cexpr.operation = opCount
			return cexpr, nil
		case "sum":
			if len(expr.Exprs) != 1 {
				return nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
			}
			aInner, ok := expr.Exprs[0].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
			}
			innerCol, ok := aInner.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
			}
			if !innerCol.Qualifier.IsEmpty() {
				return nil, fmt.Errorf("unsupported qualifier for column: %v", sqlparser.String(innerCol))
			}
			cexpr.operation = opSum
			cexpr.expr = innerCol
			tpb.addCol(innerCol.Name)
			cexpr.references[innerCol.Name.Lowered()] = true
			return cexpr, nil
		}
	}
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:

			if !node.Qualifier.IsEmpty() {
				return false, fmt.Errorf("unsupported qualifier for column: %v", sqlparser.String(node))
			}
			tpb.addCol(node.Name)
			cexpr.references[node.Name.Lowered()] = true
		case *sqlparser.Subquery:
			return false, fmt.Errorf("unsupported subquery: %v", sqlparser.String(node))
		case *sqlparser.FuncExpr:
			// Other aggregates are not supported.
			if node.IsAggregate() {
				return false, fmt.Errorf("unexpected: %v", sqlparser.String(node))
			}
		}
		return true, nil
	}, aliased.Expr)
	if err != nil {
		return nil, err
	}
	cexpr.expr = aliased.Expr
	return cexpr, nil
}

// addCol adds the specified column to the send query
// if it's not already present.
func (tpb *tablePlanBuilder) addCol(ident sqlparser.ColIdent) {
	if tpb.selColumns[ident.Lowered()] {
		return
	}
	tpb.selColumns[ident.Lowered()] = true
	tpb.sendSelect.SelectExprs = append(tpb.sendSelect.SelectExprs, &sqlparser.AliasedExpr{
		Expr: &sqlparser.ColName{Name: ident},
	})
}

func (tpb *tablePlanBuilder) analyzeGroupBy(groupBy sqlparser.GroupBy) error {
	if groupBy == nil {
		// If there's no grouping, the it's an insertNormal.
		return nil
	}
	for _, expr := range groupBy {
		colname, ok := expr.(*sqlparser.ColName)
		if !ok {
			return fmt.Errorf("unexpected: %v", sqlparser.String(expr))
		}
		cexpr := tpb.findCol(colname.Name)
		if cexpr == nil {
			return fmt.Errorf("group by expression does not reference an alias in the select list: %v", sqlparser.String(expr))
		}
		if cexpr.operation != opExpr {
			return fmt.Errorf("group by expression is not allowed to reference an aggregate expression: %v", sqlparser.String(expr))
		}
		cexpr.isGrouped = true
	}
	// If all colExprs are grouped, then it's an insertIgnore.
	tpb.onInsert = insertIgnore
	for _, cExpr := range tpb.colExprs {
		if !cExpr.isGrouped {
			// If some colExprs are not grouped, then it's an insertOnDup.
			tpb.onInsert = insertOnDup
			break
		}
	}
	return nil
}

// analyzePK builds tpb.pkCols.
func (tpb *tablePlanBuilder) analyzePK(pkInfoMap map[string]map[string][]*PrimaryKeyInfo) error {
	_, ok := pkInfoMap[tpb.dbname.String()]
	if !ok {
		return fmt.Errorf("db %s not found in schema", tpb.dbname)
	}
	pkcols, ok := pkInfoMap[tpb.dbname.String()][tpb.name.String()]
	if !ok {
		return fmt.Errorf("table %s not found in schema", tpb.name)
	}
	for _, pkcol := range pkcols {
		cexpr := tpb.findCol(sqlparser.NewColIdent(pkcol.Name))
		if cexpr == nil {
			return fmt.Errorf("primary key column %v not found in select list", pkcol)
		}
		if cexpr.operation != opExpr {
			return fmt.Errorf("primary key column %v is not allowed to reference an aggregate expression", pkcol)
		}
		cexpr.isPK = true
		cexpr.dataType = pkcol.DataType
		cexpr.columnType = pkcol.ColumnType
		tpb.pkCols = append(tpb.pkCols, cexpr)
	}
	return nil
}

func (tpb *tablePlanBuilder) findCol(name sqlparser.ColIdent) *colExpr {
	for _, cexpr := range tpb.colExprs {
		if cexpr.colName.Equal(name) {
			return cexpr
		}
	}
	return nil
}

func (tpb *tablePlanBuilder) generateInsertStatement() *sqlparser.ParsedQuery {
	bvf := &bindvarFormatter{}
	buf := sqlparser.NewTrackedBuffer(bvf.formatter)

	tpb.generateInsertPart(buf)
	if tpb.lastpk == nil {
		// If there's no lastpk, generate straight values.
		buf.Myprintf(" values ", tpb.name)
		tpb.generateValuesPart(buf, bvf)
	} else {
		// If there is a lastpk, generate values as a select from dual
		// where the pks < lastpk
		tpb.generateSelectPart(buf, bvf)
	}
	tpb.generateOnDupPart(buf)

	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) generateInsertPart(buf *sqlparser.TrackedBuffer) *sqlparser.ParsedQuery {
	dbname := tpb.dbname
	if !tpb.targetDbname.IsEmpty() {
		dbname = tpb.targetDbname
	}

	if tpb.onInsert == insertIgnore {
		buf.Myprintf("insert ignore into %v.%v(", dbname, tpb.name)
	} else {
		buf.Myprintf("insert into %v.%v(", dbname, tpb.name)
	}
	separator := ""
	for _, cexpr := range tpb.colExprs {
		buf.Myprintf("%s%v", separator, cexpr.colName)
		separator = ","
	}
	buf.Myprintf(")", tpb.name)
	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) generateValuesPart(buf *sqlparser.TrackedBuffer, bvf *bindvarFormatter) *sqlparser.ParsedQuery {
	bvf.mode = bvAfter
	separator := "("
	for _, cexpr := range tpb.colExprs {
		buf.Myprintf("%s", separator)
		separator = ","
		switch cexpr.operation {
		case opExpr:
			if cexpr.colType == proto.Type_JSON {
				buf.Myprintf("convert(%v using utf8mb4)", cexpr.expr)
			} else {
				buf.Myprintf("%v", cexpr.expr)
			}
		case opCount:
			buf.WriteString("1")
		case opSum:
			// NULL values must be treated as 0 for SUM.
			buf.Myprintf("ifnull(%v, 0)", cexpr.expr)
		}
	}
	buf.Myprintf(")")
	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) generateSelectPart(buf *sqlparser.TrackedBuffer, bvf *bindvarFormatter) *sqlparser.ParsedQuery {
	bvf.mode = bvAfter
	buf.WriteString(" select ")
	separator := ""
	for _, cexpr := range tpb.colExprs {
		buf.Myprintf("%s", separator)
		separator = ", "
		switch cexpr.operation {
		case opExpr:
			buf.Myprintf("%v", cexpr.expr)
		case opCount:
			buf.WriteString("1")
		case opSum:
			buf.Myprintf("ifnull(%v, 0)", cexpr.expr)
		}
	}
	buf.WriteString(" from dual where ")
	tpb.generatePKConstraint(buf, bvf)
	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) generateOnDupPart(buf *sqlparser.TrackedBuffer) *sqlparser.ParsedQuery {
	if tpb.onInsert != insertOnDup {
		return nil
	}
	buf.Myprintf(" on duplicate key update ")
	separator := ""
	for _, cexpr := range tpb.colExprs {
		// We don't know of a use case where the group by columns
		// don't match the pk of a table. But we'll allow this,
		// and won't update the pk column with the new value if
		// this does happen. This can be revisited if there's
		// a legitimate use case in the future that demands
		// a different behavior. This rule is applied uniformly
		// for updates and deletes also.
		if cexpr.isGrouped || cexpr.isPK {
			continue
		}
		buf.Myprintf("%s%v=", separator, cexpr.colName)
		separator = ", "
		switch cexpr.operation {
		case opExpr:
			buf.Myprintf("values(%v)", cexpr.colName)
		case opCount:
			buf.Myprintf("%v+1", cexpr.colName)
		case opSum:
			buf.Myprintf("%v", cexpr.colName)
			buf.Myprintf("+ifnull(values(%v), 0)", cexpr.colName)
		}
	}
	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) generateUpdateStatement() *sqlparser.ParsedQuery {
	if tpb.onInsert == insertIgnore {
		return tpb.generateInsertStatement()
	}
	bvf := &bindvarFormatter{}
	buf := sqlparser.NewTrackedBuffer(bvf.formatter)

	dbname := tpb.dbname
	if !tpb.targetDbname.IsEmpty() {
		dbname = tpb.targetDbname
	}
	buf.Myprintf("update %v.%v set ", dbname, tpb.name)
	separator := ""
	for _, cexpr := range tpb.colExprs {
		if cexpr.isGrouped || cexpr.isPK {
			continue
		}
		buf.Myprintf("%s%v=", separator, cexpr.colName)
		separator = ", "
		switch cexpr.operation {
		case opExpr:
			bvf.mode = bvAfter
			buf.Myprintf("%v", cexpr.expr)
		case opCount:
			buf.Myprintf("%v", cexpr.colName)
		case opSum:
			buf.Myprintf("%v", cexpr.colName)
			bvf.mode = bvBefore
			buf.Myprintf("-ifnull(%v, 0)", cexpr.expr)
			bvf.mode = bvAfter
			buf.Myprintf("+ifnull(%v, 0)", cexpr.expr)
		}
	}
	tpb.generateWhere(buf, bvf)
	return buf.ParsedQuery()
}

func (tpb *tablePlanBuilder) generateDeleteStatement() *sqlparser.ParsedQuery {
	bvf := &bindvarFormatter{}
	buf := sqlparser.NewTrackedBuffer(bvf.formatter)

	dbname := tpb.dbname
	if !tpb.targetDbname.IsEmpty() {
		dbname = tpb.targetDbname
	}
	switch tpb.onInsert {
	case insertNormal:
		buf.Myprintf("delete from %v.%v", dbname, tpb.name)
		tpb.generateWhere(buf, bvf)
	case insertOnDup:
		bvf.mode = bvBefore
		buf.Myprintf("update %v.%v set ", dbname, tpb.name)
		separator := ""
		for _, cexpr := range tpb.colExprs {
			if cexpr.isGrouped || cexpr.isPK {
				continue
			}
			buf.Myprintf("%s%v=", separator, cexpr.colName)
			separator = ", "
			switch cexpr.operation {
			case opExpr:
				buf.WriteString("null")
			case opCount:
				buf.Myprintf("%v-1", cexpr.colName)
			case opSum:
				buf.Myprintf("%v-ifnull(%v, 0)", cexpr.colName, cexpr.expr)
			}
		}
		tpb.generateWhere(buf, bvf)
	case insertIgnore:
		return nil
	}
	return buf.ParsedQuery()
}

// For binary(n) column types, the value in the where clause needs to be padded with nulls upto the length of the column
// for MySQL comparison to work properly. This is achieved by casting it to the column type
func castIfNecessary(buf *sqlparser.TrackedBuffer, cexpr *colExpr) {
	if cexpr.dataType == "binary" {
		buf.Myprintf("cast(%v as %s)", cexpr.expr, cexpr.columnType)
		return
	}
	buf.Myprintf("%v", cexpr.expr)
}

func (tpb *tablePlanBuilder) generateWhere(buf *sqlparser.TrackedBuffer, bvf *bindvarFormatter) {
	buf.WriteString(" where ")
	bvf.mode = bvBefore
	separator := ""
	for _, cexpr := range tpb.pkCols {
		if _, ok := cexpr.expr.(*sqlparser.ColName); ok {
			buf.Myprintf("%s%v=", separator, cexpr.colName)
			castIfNecessary(buf, cexpr)
		} else {
			// Parenthesize non-trivial expressions.
			buf.Myprintf("%s%v=(", separator, cexpr.colName)
			castIfNecessary(buf, cexpr)
			buf.Myprintf(")")
		}
		separator = " and "
	}
	if tpb.lastpk != nil {
		buf.WriteString(" and ")
		tpb.generatePKConstraint(buf, bvf)
	}
}

func (tpb *tablePlanBuilder) getCharsetAndCollation(pkname string) (charSet string, collation string) {
	for _, pkInfo := range tpb.pkInfos {
		if strings.EqualFold(pkInfo.Name, pkname) {
			if pkInfo.CharSet != "" {
				charSet = fmt.Sprintf(" _%s ", pkInfo.CharSet)
			}
			if pkInfo.Collation != "" {
				collation = fmt.Sprintf(" COLLATE %s ", pkInfo.Collation)
			}
		}
	}
	return charSet, collation
}

func (tpb *tablePlanBuilder) generatePKConstraint(buf *sqlparser.TrackedBuffer, bvf *bindvarFormatter) {
	type charSetCollation struct {
		charSet   string
		collation string
	}
	var charSetCollations []*charSetCollation
	separator := "("
	for _, pkname := range tpb.lastpk.Fields {
		charSet, collation := tpb.getCharsetAndCollation(pkname.Name)
		charSetCollations = append(charSetCollations, &charSetCollation{charSet: charSet, collation: collation})
		buf.Myprintf("%s%s%v%s", separator, charSet, &sqlparser.ColName{Name: sqlparser.NewColIdent(pkname.Name)}, collation)
		separator = ","
	}
	separator = ") <= ("
	for i, val := range tpb.lastpk.Rows[0] {
		buf.WriteString(separator)
		buf.WriteString(charSetCollations[i].charSet)
		separator = ","
		val.EncodeSQL(buf)
		buf.WriteString(charSetCollations[i].collation)
	}
	buf.WriteString(")")
}

// bindvarFormatter is a dual mode formatter. Its behavior
// can be changed dynamically changed to generate bind vars
// for the 'before' row or 'after' row by setting its mode
// to 'bvBefore' or 'bvAfter'. For example, inserts will always
// use bvAfter, whereas deletes will always use bvBefore.
// For updates, values being set will use bvAfter, whereas
// the where clause will use bvBefore.
type bindvarFormatter struct {
	mode bindvarMode
}

type bindvarMode int

const (
	bvBefore = bindvarMode(iota)
	bvAfter
)

func (bvf *bindvarFormatter) formatter(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	if node, ok := node.(*sqlparser.ColName); ok {
		switch bvf.mode {
		case bvBefore:
			buf.WriteArg(fmt.Sprintf(":b_%s", node.Name.String()))
			return
		case bvAfter:
			buf.WriteArg(fmt.Sprintf(":a_%s", node.Name.String()))
			return
		}
	}
	node.Format(buf)
}
