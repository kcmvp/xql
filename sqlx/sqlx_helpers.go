package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/kcmvp/xql"
	"github.com/kcmvp/xql/entity"
	"github.com/samber/mo"
)

// Internal builder helpers (moved from sqlx.go)
// This file contains package-private helpers used by the public `sqlx` API.
// See sqlx.go for higher-level executors and public APIs.

type whereFunc struct {
	f    func() (string, []any)
	flds []xql.Field
}

func (wf whereFunc) Build() (string, []any) {
	return wf.f()
}

func (wf whereFunc) fields() []xql.Field {
	return wf.flds
}

func and(wheres ...Where) Where {
	f := func() (string, []any) {
		clauses := make([]string, 0, len(wheres))
		var allArgs []any
		for _, w := range wheres {
			if w == nil {
				continue
			}
			clause, args := w.Build()
			if clause == "" {
				continue
			}
			clauses = append(clauses, clause)
			allArgs = append(allArgs, args...)
		}
		if len(clauses) == 0 {
			return "", nil
		}
		return fmt.Sprintf("(%s)", strings.Join(clauses, " AND ")), allArgs
	}

	// aggregate fields from children
	flds := make([]xql.Field, 0)
	for _, w := range wheres {
		if w == nil {
			continue
		}
		// each Where must implement fields()
		flds = append(flds, w.fields()...)
	}
	return whereFunc{f: f, flds: flds}
}

func or(wheres ...Where) Where {
	f := func() (string, []any) {
		clauses := make([]string, 0, len(wheres))
		var allArgs []any
		for _, w := range wheres {
			if w == nil {
				continue
			}
			clause, args := w.Build()
			if clause == "" {
				continue
			}
			clauses = append(clauses, clause)
			allArgs = append(allArgs, args...)
		}
		if len(clauses) == 0 {
			return "", nil
		}
		return fmt.Sprintf("(%s)", strings.Join(clauses, " OR ")), allArgs
	}

	flds := make([]xql.Field, 0)
	for _, w := range wheres {
		if w == nil {
			continue
		}
		flds = append(flds, w.fields()...)
	}
	return whereFunc{f: f, flds: flds}
}

func dbQualifiedNameFromQName(q string) string {
	// We expect q to be either:
	//  - "table.column" (no view)
	//  - "table.column.view" (view included)
	// Table may itself contain '.' (schema-qualified). To handle both cases
	// we parse from the right: find last '.' (separator before view), then
	// the previous '.' separates table and column.
	last := strings.LastIndex(q, ".")
	if last == -1 {
		return q
	}
	prev := strings.LastIndex(q[:last], ".")
	if prev == -1 {
		// only one dot present -> treat as table.column
		return q
	}
	table := q[:prev]
	col := q[prev+1 : last]
	return fmt.Sprintf("%s.%s", table, col)
}

func makePlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	ps := make([]string, n)
	for i := 0; i < n; i++ {
		ps[i] = "?"
	}
	return strings.Join(ps, ",")
}

func op(field xql.Field, operator string, value any) Where {
	f := func() (string, []any) {
		clause := fmt.Sprintf("%s %s ?", dbQualifiedNameFromQName(field.QualifiedName()), operator)
		return clause, []any{value}
	}
	return whereFunc{f: f, flds: []xql.Field{field}}
}

func inWhere(field xql.Field, values ...any) Where {
	if len(values) == 0 {
		return whereFunc{f: func() (string, []any) { return "1=0", nil }, flds: []xql.Field{field}}
	}
	placeholders := makePlaceholders(len(values))
	clause := fmt.Sprintf("%s IN (%s)", dbQualifiedNameFromQName(field.QualifiedName()), placeholders)
	return whereFunc{f: func() (string, []any) { return clause, values }, flds: []xql.Field{field}}
}

func selectSQL[T entity.Entity](schema *Schema, where Where) (string, []any, error) {
	if schema == nil {
		return "", nil, fmt.Errorf("schema is required")
	}
	if len(*schema) == 0 {
		return "", nil, fmt.Errorf("schema has no fields")
	}

	var ent T
	table := ent.Table()
	if strings.TrimSpace(table) == "" {
		return "", nil, fmt.Errorf("entity table is empty")
	}

	cols := make([]string, 0, len(*schema))
	for _, f := range *schema {
		q := dbQualifiedNameFromQName(f.QualifiedName())
		parts := strings.Split(q, ".")
		alias := ""
		if len(parts) == 2 {
			alias = fmt.Sprintf("%s__%s", parts[0], parts[1])
		} else {
			alias = q
		}
		cols = append(cols, fmt.Sprintf("%s AS %s", q, alias))
	}

	sqlStr := fmt.Sprintf("SELECT %s FROM %s", strings.Join(cols, ", "), table)
	if where == nil {
		return sqlStr, nil, nil
	}
	clause, args := where.Build()
	if clause == "" {
		return sqlStr, nil, nil
	}
	return sqlStr + " WHERE " + clause, args, nil
}

func updateSQL[T entity.Entity](schema Schema, g ValueObject, where Where) (string, []any, error) {
	if schema == nil || len(schema) == 0 {
		return "", nil, fmt.Errorf("schema is required")
	}
	if where == nil {
		return "", nil, fmt.Errorf("where is required")
	}
	whereClause, whereArgs := where.Build()
	if whereClause == "" {
		return "", nil, fmt.Errorf("where is required")
	}

	var ent T
	table := ent.Table()
	if strings.TrimSpace(table) == "" {
		return "", nil, fmt.Errorf("entity table is empty")
	}

	sets := make([]string, 0, len(schema))
	args := make([]any, 0)

	if g == nil {
		for _, f := range schema {
			q := dbQualifiedNameFromQName(f.QualifiedName())
			sets = append(sets, fmt.Sprintf("%s = ?", q))
		}
	} else {
		// Build a map of viewName -> number of occurrences to detect ambiguous view names.
		viewMap := make(map[string]int)
		viewToField := make(map[string]xql.Field)
		for _, f := range schema {
			qname := f.QualifiedName()
			parts := strings.Split(qname, ".")
			view := parts[len(parts)-1]
			viewMap[view]++
			// store last seen field for the view (used only when count==1)
			viewToField[view] = f
		}

		for _, f := range schema {
			q := dbQualifiedNameFromQName(f.QualifiedName())
			viewKey := f.QualifiedName()
			// try qualified key first
			vOpt := g.Get(viewKey)
			if vOpt.IsAbsent() {
				// try unqualified view name if unambiguous
				parts := strings.Split(viewKey, ".")
				view := parts[len(parts)-1]
				if count := viewMap[view]; count == 1 {
					// resolve using the single matching schema field
					// ensure caller provided the unqualified key
					vOpt = g.Get(view)
					if !vOpt.IsAbsent() {
						// good: resolved by view name -> use it
					} else {
						// not present by either qualified or unqualified name; skip
					}
				} else if count > 1 {
					// ambiguous view name in schema; require qualified key from caller
					// do not attempt to resolve automatically
					vOpt = g.Get(view) // attempt to see if caller used ambiguous name
					if !vOpt.IsAbsent() {
						return "", nil, fmt.Errorf("ambiguous view name %q present in schema; use qualified field name %q instead", view, viewKey)
					}
				}
			}

			if vOpt.IsAbsent() {
				// no value provided for this schema field; skip
				continue
			}

			sets = append(sets, fmt.Sprintf("%s = ?", q))
			args = append(args, vOpt.MustGet())
		}

		if len(sets) == 0 {
			return "", nil, fmt.Errorf("no fields to update")
		}
	}

	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, strings.Join(sets, ", "), whereClause)
	if len(whereArgs) > 0 {
		args = append(args, whereArgs...)
	}
	return sql, args, nil
}

// updateSQLFromValues builds an UPDATE statement using the provided ValueObject.
// Behavior:
//   - The ValueObject's Fields() are used as the list of fields to update.
//     Keys that are qualified (contain '.') are treated as fully-qualified
//     persistence names and must belong to the same table as the generic T.
//     Unqualified keys are treated as view/property names and are mapped to
//     snake_case columns on T's table.
func updateSQLFromValues[T entity.Entity](setter ValueObject, where Where) (string, []any, error) {
	if where == nil {
		return "", nil, fmt.Errorf("where is required")
	}
	whereClause, whereArgs := where.Build()
	if whereClause == "" {
		return "", nil, fmt.Errorf("where is required")
	}
	if setter == nil {
		return "", nil, fmt.Errorf("values is required")
	}

	var ent T
	table := ent.Table()
	if strings.TrimSpace(table) == "" {
		return "", nil, fmt.Errorf("entity table is empty")
	}

	sets := make([]string, 0)
	args := make([]any, 0)

	// Use keys from the ValueObject (exclude nothing special)
	for _, k := range setter.Fields() {
		vOpt := setter.Get(k)
		if vOpt.IsAbsent() {
			continue
		}

		var q string
		if strings.Contains(k, ".") {
			// keyed value is qualified. Normalize to db-qualified name (table.col)
			q = dbQualifiedNameFromQName(k)
			parts := strings.Split(q, ".")
			if len(parts) < 2 {
				return "", nil, fmt.Errorf("invalid qualified name: %s", k)
			}
			// table part may contain dots (schema.table), so join all but the last element
			tablePart := strings.Join(parts[:len(parts)-1], ".")
			if tablePart != table {
				return "", nil, fmt.Errorf("field %q belongs to table %q, expected %q", k, tablePart, table)
			}
		} else {
			// Strict enforcement: unqualified keys are not allowed here. Callers that
			// want to provide view-style (unqualified) keys must use the schema-aware
			// Update/UpdateJoin APIs which accept a Schema and resolve view keys.
			return "", nil, fmt.Errorf("unqualified value key %q is not allowed in this context; provide a persistence schema via Update(schema, ...) or use a fully-qualified key 'table.column'", k)
		}

		sets = append(sets, fmt.Sprintf("%s = ?", q))
		args = append(args, vOpt.MustGet())
	}

	if len(sets) == 0 {
		return "", nil, fmt.Errorf("no fields to update")
	}

	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, strings.Join(sets, ", "), whereClause)
	if len(whereArgs) > 0 {
		args = append(args, whereArgs...)
	}
	return sql, args, nil
}

func deleteSQL[T entity.Entity](where Where) (string, []any, error) {
	if where == nil {
		return "", nil, fmt.Errorf("where is required")
	}
	clause, args := where.Build()
	if clause == "" {
		return "", nil, fmt.Errorf("where is required")
	}

	var ent T
	table := ent.Table()
	return fmt.Sprintf("DELETE FROM %s WHERE %s", table, clause), args, nil
}

func buildSelectWithJoin(schema Schema, joinstmt string, where Where) (string, []any, error) {
	if schema == nil || len(schema) == 0 {
		return "", nil, fmt.Errorf("schema is required and must contain at least one field")
	}
	// derive base table from first schema field's QualifiedName
	first := schema[0]
	qname := first.QualifiedName()
	parts := strings.Split(qname, ".")
	if len(parts) < 1 {
		return "", nil, fmt.Errorf("invalid qualified name: %s", qname)
	}
	baseTable := parts[0]

	cols := make([]string, 0, len(schema))
	for _, f := range schema {
		q := f.QualifiedName()
		q = dbQualifiedNameFromQName(q)
		ps := strings.Split(q, ".")
		alias := q
		if len(ps) == 2 {
			alias = fmt.Sprintf("%s__%s", ps[0], ps[1])
		}
		cols = append(cols, fmt.Sprintf("%s AS %s", q, alias))
	}

	sqlStr := fmt.Sprintf("SELECT %s FROM %s", strings.Join(cols, ", "), baseTable)
	if strings.TrimSpace(joinstmt) != "" {
		if strings.Contains(joinstmt, "?") {
			return "", nil, fmt.Errorf("joinstmt must not contain placeholders; put parameters in Where")
		}
		sqlStr = sqlStr + " " + joinstmt
	}
	if where == nil {
		return sqlStr, nil, nil
	}
	clause, args := where.Build()
	if clause == "" {
		return sqlStr, nil, nil
	}
	return sqlStr + " WHERE " + clause, args, nil
}

func buildDeleteWithJoin(baseTable string, joinstmt string, where Where) (string, []any, error) {
	if strings.TrimSpace(baseTable) == "" {
		return "", nil, fmt.Errorf("base table is required")
	}
	up := strings.ToUpper(strings.TrimSpace(joinstmt))
	joinIdx := strings.Index(up, "JOIN ")
	onIdx := strings.Index(up, " ON ")
	if joinIdx == -1 || onIdx == -1 {
		return "", nil, fmt.Errorf("unsupported joinstmt for delete: expected single 'JOIN <table> ON <cond>' pattern")
	}
	// locate original ON index in joinstmt
	onIdxOrig := strings.Index(strings.ToUpper(joinstmt), " ON ")
	if onIdxOrig == -1 {
		return "", nil, fmt.Errorf("unsupported joinstmt for delete: missing ON clause")
	}
	// table part between 'JOIN ' and ' ON '
	tablePart := strings.TrimSpace(joinstmt[joinIdx+5 : onIdxOrig])
	onPart := strings.TrimSpace(joinstmt[onIdxOrig+4:])

	clause := ""
	var args []any
	if where != nil {
		c, a := where.Build()
		clause = c
		args = a
	}

	sub := fmt.Sprintf("SELECT 1 FROM %s WHERE %s", tablePart, onPart)
	if clause != "" {
		sub = sub + " AND (" + clause + ")"
	}
	sqlStr := fmt.Sprintf("DELETE FROM %s WHERE EXISTS (%s)", baseTable, sub)
	return sqlStr, args, nil
}

func buildExistsWhere(joinstmt string, where Where) (Where, error) {
	if strings.TrimSpace(joinstmt) == "" {
		// no joinstmt -> if where provided, just use it; otherwise error
		if where == nil {
			return nil, fmt.Errorf("joinstmt or where required")
		}
		return where, nil
	}
	up := strings.ToUpper(strings.TrimSpace(joinstmt))
	joinIdx := strings.Index(up, "JOIN ")
	onIdx := strings.Index(up, " ON ")
	if joinIdx == -1 || onIdx == -1 {
		return nil, fmt.Errorf("unsupported joinstmt for exists: expected single 'JOIN <table> ON <cond>' pattern")
	}
	onIdxOrig := strings.Index(strings.ToUpper(joinstmt), " ON ")
	if onIdxOrig == -1 {
		return nil, fmt.Errorf("unsupported joinstmt for exists: missing ON clause")
	}
	tablePart := strings.TrimSpace(joinstmt[joinIdx+5 : onIdxOrig])
	onPart := strings.TrimSpace(joinstmt[onIdxOrig+4:])

	w := func() (string, []any) {
		clause := ""
		var args []any
		if where != nil {
			c, a := where.Build()
			clause = c
			args = a
		}
		sub := fmt.Sprintf("EXISTS (SELECT 1 FROM %s WHERE %s", tablePart, onPart)
		if clause != "" {
			sub = sub + " AND (" + clause + ")"
		}
		sub = sub + ")"
		return sub, args
	}
	// aggregate fields from inner where
	flds := make([]xql.Field, 0)
	if where != nil {
		flds = append(flds, where.fields()...)
	}
	return whereFunc{f: w, flds: flds}, nil
}

// rowsToValueObjects maps query results to meta.ValueObject using the schema order.
// Mapping policy:
// - Fields are schema field Name() (provider name).
// - Values are scanned as driver values.
func rowsToValueObjects(rows *sql.Rows, schema Schema) ([]ValueObject, error) {
	if rows == nil {
		return nil, fmt.Errorf("rows is required")
	}
	if len(schema) == 0 {
		return nil, fmt.Errorf("schema has no fields")
	}

	// We always project columns in the same order as schema in selectSQL.
	n := len(schema)
	out := make([]ValueObject, 0)

	for rows.Next() {
		vals := make([]any, n)
		dests := make([]any, n)
		for i := range vals {
			dests[i] = &vals[i]
		}
		if err := rows.Scan(dests...); err != nil {
			return nil, err
		}

		m := make(map[string]any, n)
		for i, f := range schema {
			m[f.QualifiedName()] = vals[i]
		}
		out = append(out, valueObject{Data: m})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// -----------------------------
// Executors - SELECT
// -----------------------------

type queryExec[T entity.Entity] struct {
	schema Schema
	where  Where
}

func (q queryExec[T]) Execute(ctx context.Context, ds *sql.DB) (mo.Either[[]ValueObject, sql.Result], error) {
	if ds == nil {
		return mo.Left[[]ValueObject, sql.Result](nil), fmt.Errorf("db is required")
	}
	query, qargs, err := selectSQL[T](&q.schema, q.where)
	if err != nil {
		return mo.Left[[]ValueObject, sql.Result](nil), err
	}
	rows, err := ds.QueryContext(ctx, query, qargs...)
	if err != nil {
		return mo.Left[[]ValueObject, sql.Result](nil), err
	}
	defer func() { _ = rows.Close() }()

	res, err := rowsToValueObjects(rows, q.schema)
	if err != nil {
		return mo.Left[[]ValueObject, sql.Result](nil), err
	}
	return mo.Left[[]ValueObject, sql.Result](res), nil
}

func (q queryExec[T]) sql() (string, error) {
	qstr, _, err := selectSQL[T](&q.schema, q.where)
	return qstr, err
}

// -----------------------------
// Executors - DELETE
// -----------------------------

type deleteExec[T entity.Entity] struct {
	where Where
}

func (d deleteExec[T]) Execute(ctx context.Context, ds *sql.DB) (mo.Either[[]ValueObject, sql.Result], error) {
	if ds == nil {
		return mo.Right[[]ValueObject, sql.Result](nil), fmt.Errorf("db is required")
	}
	query, qargs, err := deleteSQL[T](d.where)
	if err != nil {
		return mo.Right[[]ValueObject, sql.Result](nil), err
	}
	result, err := ds.ExecContext(ctx, query, qargs...)
	if err != nil {
		return mo.Right[[]ValueObject, sql.Result](nil), err
	}
	return mo.Right[[]ValueObject, sql.Result](result), nil
}

func (d deleteExec[T]) sql() (string, error) {
	dstr, _, err := deleteSQL[T](d.where)
	return dstr, err
}

// -----------------------------
// Executors - UPDATE
// -----------------------------

// (moved to sqlx.go: updateExec implementation that is schema-aware)

// -----------------------------
// Executors - JOIN
// -----------------------------

type joinQueryExec struct {
	schema   Schema
	joinstmt string
	where    Where
}

func (j joinQueryExec) Execute(ctx context.Context, ds *sql.DB) (mo.Either[[]ValueObject, sql.Result], error) {
	if ds == nil {
		return mo.Left[[]ValueObject, sql.Result](nil), fmt.Errorf("db is required")
	}
	q, args, err := buildSelectWithJoin(j.schema, j.joinstmt, j.where)
	if err != nil {
		return mo.Left[[]ValueObject, sql.Result](nil), err
	}
	rows, err := ds.QueryContext(ctx, q, args...)
	if err != nil {
		return mo.Left[[]ValueObject, sql.Result](nil), err
	}
	defer func() { _ = rows.Close() }()
	res, err := rowsToValueObjects(rows, j.schema)
	if err != nil {
		return mo.Left[[]ValueObject, sql.Result](nil), err
	}
	return mo.Left[[]ValueObject, sql.Result](res), nil
}

func (j joinQueryExec) sql() (string, error) {
	q, _, err := buildSelectWithJoin(j.schema, j.joinstmt, j.where)
	return q, err
}

// joinDeleteExec implements delete via EXISTS using a joinstmt.
type joinDeleteExec struct {
	baseTable string
	joinstmt  string
	where     Where
}

func (j joinDeleteExec) Execute(ctx context.Context, ds *sql.DB) (mo.Either[[]ValueObject, sql.Result], error) {
	if ds == nil {
		return mo.Right[[]ValueObject, sql.Result](nil), fmt.Errorf("db is required")
	}
	q, args, err := buildDeleteWithJoin(j.baseTable, j.joinstmt, j.where)
	if err != nil {
		return mo.Right[[]ValueObject, sql.Result](nil), err
	}
	res, err := ds.ExecContext(ctx, q, args...)
	if err != nil {
		return mo.Right[[]ValueObject, sql.Result](nil), err
	}
	return mo.Right[[]ValueObject, sql.Result](res), nil
}

func (j joinDeleteExec) sql() (string, error) {
	q, _, err := buildDeleteWithJoin(j.baseTable, j.joinstmt, j.where)
	return q, err
}

// validateSyntax verifies that all provided fields belong to the table for T.
func validateSyntax[T entity.Entity](fields ...xql.Field) error {
	if len(fields) == 0 {
		return nil
	}
	var ent T
	expected := ent.Table()
	if strings.TrimSpace(expected) == "" {
		return fmt.Errorf("entity table is empty")
	}
	for _, f := range fields {
		if f == nil {
			continue
		}
		q := dbQualifiedNameFromQName(f.QualifiedName())
		parts := strings.Split(q, ".")
		if len(parts) < 2 {
			return fmt.Errorf("invalid qualified name: %s", f.QualifiedName())
		}
		tablePart := strings.Join(parts[:len(parts)-1], ".")
		if tablePart != expected {
			return fmt.Errorf("field %q belongs to table %q, expected %q", f.QualifiedName(), tablePart, expected)
		}
	}
	return nil
}

// error executor implementations returned when validation fails early.
// They implement the Executor interface and always return the stored error.

type errorExecutorSelect struct{ err error }

func (e errorExecutorSelect) Execute(ctx context.Context, ds *sql.DB) (mo.Either[[]ValueObject, sql.Result], error) {
	return mo.Left[[]ValueObject, sql.Result](nil), e.err
}

func (e errorExecutorSelect) sql() (string, error) { return "", e.err }

type errorExecutorNonSelect struct{ err error }

func (e errorExecutorNonSelect) Execute(ctx context.Context, ds *sql.DB) (mo.Either[[]ValueObject, sql.Result], error) {
	return mo.Right[[]ValueObject, sql.Result](nil), e.err
}

func (e errorExecutorNonSelect) sql() (string, error) { return "", e.err }
