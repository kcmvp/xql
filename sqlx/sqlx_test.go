package sqlx

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	. "github.com/kcmvp/xql/sample/entity"
	"github.com/kcmvp/xql/sample/gen/field/order"
	"github.com/stretchr/testify/require"
)

// normalizeSQL removes SQL comments and normalizes whitespace for comparison.
func normalizeSQL(s string) string {
	// remove SQL comments starting with --
	re := regexp.MustCompile(`(?m:^\s*--.*$)`) // lines starting with --
	s = re.ReplaceAllString(s, "")
	// normalize whitespace
	re2 := regexp.MustCompile(`\s+`)
	s = re2.ReplaceAllString(strings.TrimSpace(s), " ")
	return s
}

// normalizeForCompare normalizes a multi-line string for stable exact comparison.
// - normalize line endings to LF
// - trim trailing spaces on each line
// - collapse multiple consecutive blank lines into a single blank line
// - trim leading/trailing blank lines and ensure single trailing newline
func normalizeForCompare(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	lines := strings.Split(s, "\n")
	out := make([]string, 0, len(lines))
	blankCount := 0
	for _, ln := range lines {
		ln = strings.TrimRight(ln, " \t")
		if strings.TrimSpace(ln) == "" {
			blankCount++
			if blankCount > 1 {
				// skip additional blank lines
				continue
			}
			out = append(out, "")
			continue
		}
		blankCount = 0
		out = append(out, ln)
	}
	// remove leading/trailing blank lines
	start := 0
	for start < len(out) && strings.TrimSpace(out[start]) == "" {
		start++
	}
	end := len(out)
	for end > start && strings.TrimSpace(out[end-1]) == "" {
		end--
	}
	trimmed := strings.Join(out[start:end], "\n")
	return trimmed + "\n"
}

// loadRawSnapshot reads the snapshot file verbatim (no normalization).
func loadRawSnapshot(testName string) (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", os.ErrNotExist
	}
	pkgDir := filepath.Dir(thisFile)

	candidates := []string{
		filepath.Join(pkgDir, "testdata", "sqlx_test", testName+".sql"),
		filepath.Join(pkgDir, "testdata", testName+".sql"),
		filepath.Join(pkgDir, "..", "testdata", "sqlx_test", testName+".sql"),
		filepath.Join(pkgDir, "..", "testdata", testName+".sql"),
	}

	for _, p := range candidates {
		b, err := os.ReadFile(p)
		if err == nil {
			return string(b), nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
	}
	return "", os.ErrNotExist
}

// extractLeadingCommentPrefix returns the leading comment block (lines starting
// with '--') including a single blank line following the block if present.
// The returned prefix will include a trailing newline(s) as in the original
// raw content.
func extractLeadingCommentPrefix(raw string) string {
	lines := strings.Split(raw, "\n")
	i := 0
	for i < len(lines) && strings.HasPrefix(strings.TrimSpace(lines[i]), "--") {
		i++
	}
	// include a single blank line after comments if present
	if i < len(lines) && strings.TrimSpace(lines[i]) == "" {
		i++
	}
	prefix := strings.Join(lines[:i], "\n")
	if prefix != "" && !strings.HasSuffix(prefix, "\n") {
		prefix += "\n"
	}
	return prefix
}

// formatSQLForComparison converts the generated SQL into the multi-line
// formatted style used by the snapshots (columns on separate lines,
// indented). If the query is not a SELECT, return the original string.
// This also ensures the WHERE clause is placed on its own line after FROM
// to match snapshot layout.
func formatSQLForComparison(q string) string {
	q = strings.TrimSpace(q)
	upper := strings.ToUpper(q)
	if strings.HasPrefix(upper, "SELECT ") {
		// existing SELECT handling
		parts := strings.SplitN(q, " FROM ", 2)
		if len(parts) != 2 {
			return q
		}
		selectPart := strings.TrimPrefix(parts[0], "SELECT ")
		fromPart := parts[1]

		cols := strings.Split(selectPart, ", ")
		for i := range cols {
			cols[i] = strings.TrimSpace(cols[i])
		}
		joinedCols := strings.Join(cols, ",\n       ")

		upperFrom := strings.ToUpper(fromPart)
		whereIdx := strings.Index(upperFrom, " WHERE ")
		if whereIdx >= 0 {
			tablePart := strings.TrimSpace(fromPart[:whereIdx])
			rest := strings.TrimSpace(fromPart[whereIdx+1:])
			formatted := "SELECT " + joinedCols + "\nFROM " + tablePart + "\n" + rest
			if !strings.HasSuffix(formatted, "\n") {
				formatted += "\n"
			}
			return formatted
		}

		formatted := "SELECT " + joinedCols + "\nFROM " + fromPart
		if !strings.HasSuffix(formatted, "\n") {
			formatted += "\n"
		}
		return formatted
	}

	// DELETE formatting
	if strings.HasPrefix(upper, "DELETE FROM ") {
		// Expect form: DELETE FROM <table> WHERE <clause>
		parts := strings.SplitN(q, " WHERE ", 2)
		if len(parts) == 2 {
			tablePart := strings.TrimSpace(strings.TrimPrefix(parts[0], "DELETE FROM "))
			rest := strings.TrimSpace(parts[1])
			formatted := "DELETE FROM " + tablePart + "\nWHERE " + rest
			if !strings.HasSuffix(formatted, "\n") {
				formatted += "\n"
			}
			return formatted
		}
		// no WHERE
		return strings.TrimSpace(q) + "\n"
	}

	// UPDATE formatting: put WHERE on its own line to match snapshots
	if strings.HasPrefix(upper, "UPDATE ") {
		// split into UPDATE <table> SET <sets> [WHERE <clause>]
		parts := strings.SplitN(q, " WHERE ", 2)
		if len(parts) == 2 {
			return strings.TrimSpace(parts[0]) + "\nWHERE " + strings.TrimSpace(parts[1]) + "\n"
		}
		return strings.TrimSpace(q) + "\n"
	}

	return q
}

// TestSqlGeneration_Select exercises many kinds of SELECT SQL generation
// using a table-driven style. We verify the generated SQL contains the
// expected predicate fragments (after normalization) and that no private
// fields like internalNotes are projected.
func TestSqlGeneration_Select(t *testing.T) {
	fields := order.All()

	cases := []struct {
		name         string
		where        Where
		expect       string // substring expected in generated SQL after normalization
		hasArgs      bool
		fullSnapshot bool   // when true, do full snapshot equality check
		snapshotName string // optional snapshot name to use when fullSnapshot is true
	}{
		// No WHERE: full snapshot comparison
		{"NoWhere", nil, "", false, true, ""},
		{"Eq", Eq(order.Amount, 100.0), "WHERE orders.amount = ?", true, false, ""},
		{"Ne", Ne(order.Amount, 100.0), "WHERE orders.amount != ?", true, false, ""},
		{"Gt", Gt(order.Amount, 10.5), "WHERE orders.amount > ?", true, false, ""},
		{"Gte", Gte(order.Amount, 10.5), "WHERE orders.amount >= ?", true, false, ""},
		{"Lt", Lt(order.Amount, 10.5), "WHERE orders.amount < ?", true, false, ""},
		{"Lte", Lte(order.Amount, 10.5), "WHERE orders.amount <= ?", true, false, ""},
		{"Like", Like(order.CreatedBy, "%john%"), "WHERE orders.created_by LIKE ?", true, false, ""},
		{"InNonEmpty", In(order.ID, 1, 2, 3), "WHERE orders.id IN (?,?,?)", true, false, ""},
		{"InEmpty", In(order.ID), "WHERE 1=0", false, false, ""},
		{"And", And(Eq(order.Amount, 50.0), Gt(order.ID, 0)), "WHERE (orders.amount = ? AND orders.id > ?)", true, false, ""},
		{"Or", Or(Eq(order.Amount, 50.0), Eq(order.ID, 5)), "WHERE (orders.amount = ? OR orders.id = ?)", true, false, ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			exec := Query[Order](fields)(c.where)
			require.NotNil(t, exec)

			q, err := exec.sql()
			require.NoError(t, err)
			require.NotEmpty(t, q)

			// ensure private field not present
			require.NotContains(t, q, "internalNotes")

			// prefer exact snapshot comparison if a snapshot file exists for this case
			snapName := c.snapshotName
			if snapName == "" {
				snapName = "TestSqlGeneration_Select_" + c.name
			}
			rawExp, err := loadRawSnapshot(snapName)
			if err == nil {
				// format generated SQL similarly to snapshot layout and compare raw
				formatted := formatSQLForComparison(q)
				// normalize line endings
				formatted = strings.ReplaceAll(formatted, "\r\n", "\n")
				rawExp = strings.ReplaceAll(rawExp, "\r\n", "\n")
				// extract leading comment prefix from the snapshot and prepend it to formatted
				prefix := extractLeadingCommentPrefix(rawExp)
				combined := prefix + formatted
				// ensure trailing newline matches
				if !strings.HasSuffix(combined, "\n") {
					combined += "\n"
				}
				// compare exact after normalizing trailing newlines
				require.Equal(t, normalizeForCompare(rawExp), normalizeForCompare(combined), "generated SQL differs from exact snapshot %s", snapName)
			} else {
				// fallback to substring/normalized checks
				cleanGot := normalizeSQL(q)
				if c.fullSnapshot {
					// No snapshot found but expected full snapshot; fail explicitly
					require.Failf(t, "missing snapshot", "snapshot for %s not found", snapName)
				} else {
					require.Contains(t, cleanGot, c.expect)
				}
			}

		})
	}

	// Complex nested examples: (a OR b) AND c
	// and (a OR b) AND (c OR d)
	complexCases := []struct {
		name   string
		where  Where
		expSub string
	}{
		{"OrAnd", And(Or(Eq(order.Amount, 50.0), Eq(order.ID, 5)), Gt(order.AccountID, 0)), "WHERE ((orders.amount = ? OR orders.id = ?) AND orders.account_id > ?)"},
		{"OrAndOr", And(Or(Eq(order.Amount, 50.0), Eq(order.ID, 5)), Or(Gt(order.AccountID, 0), Lt(order.Amount, 100.0))), "WHERE ((orders.amount = ? OR orders.id = ?) AND (orders.account_id > ? OR orders.amount < ?))"},
	}

	for _, c := range complexCases {
		t.Run(c.name, func(t *testing.T) {
			exec := Query[Order](fields)(c.where)
			require.NotNil(t, exec)

			q, err := exec.sql()
			require.NoError(t, err)
			require.NotEmpty(t, q)

			// attempt exact comparison with snapshot first
			snap := "TestSqlGeneration_Select_" + c.name
			raw, err := loadRawSnapshot(snap)
			if err == nil {
				formatted := formatSQLForComparison(q)
				formatted = strings.ReplaceAll(formatted, "\r\n", "\n")
				raw = strings.ReplaceAll(raw, "\r\n", "\n")
				// extract leading comment prefix and prepend
				prefix := extractLeadingCommentPrefix(raw)
				combined := prefix + formatted
				if !strings.HasSuffix(combined, "\n") {
					combined += "\n"
				}
				require.Equal(t, normalizeForCompare(raw), normalizeForCompare(combined))
			} else {
				cleanGot := normalizeSQL(q)
				require.Contains(t, cleanGot, c.expSub)
			}
		})
	}
}

// TestSqlGeneration_Delete mirrors the Select tests but for DELETE statements.
func TestSqlGeneration_Delete(t *testing.T) {

	cases := []struct {
		name    string
		where   Where
		expect  string // substring expected in generated SQL after normalization
		hasArgs bool
	}{
		{"Eq", Eq(order.Amount, 100.0), "WHERE orders.amount = ?", true},
		{"Ne", Ne(order.Amount, 100.0), "WHERE orders.amount != ?", true},
		{"Gt", Gt(order.Amount, 10.5), "WHERE orders.amount > ?", true},
		{"Gte", Gte(order.Amount, 10.5), "WHERE orders.amount >= ?", true},
		{"Lt", Lt(order.Amount, 10.5), "WHERE orders.amount < ?", true},
		{"Lte", Lte(order.Amount, 10.5), "WHERE orders.amount <= ?", true},
		{"Like", Like(order.CreatedBy, "%john%"), "WHERE orders.created_by LIKE ?", true},
		{"InNonEmpty", In(order.ID, 1, 2, 3), "WHERE orders.id IN (?,?,?)", true},
		{"InEmpty", In(order.ID), "WHERE 1=0", false},
		{"And", And(Eq(order.Amount, 50.0), Gt(order.ID, 0)), "WHERE (orders.amount = ? AND orders.id > ?)", true},
		{"Or", Or(Eq(order.Amount, 50.0), Eq(order.ID, 5)), "WHERE (orders.amount = ? OR orders.id = ?)", true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			exec := Delete[Order](c.where)
			require.NotNil(t, exec)

			q, err := exec.sql()
			require.NoError(t, err)
			require.NotEmpty(t, q)

			// ensure private field not present in SQL
			require.NotContains(t, q, "internalNotes")

			// prefer exact snapshot comparison if a snapshot file exists for this case
			snapName := "TestSqlGeneration_Delete_" + c.name
			rawExp, err := loadRawSnapshot(snapName)
			if err == nil {
				formatted := formatSQLForComparison(q)
				formatted = strings.ReplaceAll(formatted, "\r\n", "\n")
				rawExp = strings.ReplaceAll(rawExp, "\r\n", "\n")
				prefix := extractLeadingCommentPrefix(rawExp)
				combined := prefix + formatted
				if !strings.HasSuffix(combined, "\n") {
					combined += "\n"
				}
				require.Equal(t, normalizeForCompare(rawExp), normalizeForCompare(combined), "generated DELETE SQL differs from exact snapshot %s", snapName)
			} else {
				// fallback to substring/normalized checks
				if c.expect != "" {
					cleanGot := normalizeSQL(q)
					require.Contains(t, cleanGot, c.expect)
				}
			}

			// check args via deleteSQL helper
			_, args, err := deleteSQL[Order](c.where)
			require.NoError(t, err)
			if c.hasArgs {
				require.True(t, args != nil && len(args) > 0)
			} else {
				require.True(t, args == nil || len(args) == 0)
			}
		})
	}

	// Negative case: ensure deleteSQL requires a WHERE clause to avoid accidental full-table deletes
	t.Run("NoWhere_should_error", func(t *testing.T) {
		_, _, err := deleteSQL[Order](nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "where is required")
	})

	// Complex nested examples
	complexCases := []struct {
		name  string
		where Where
		exp   string
	}{
		{"OrAnd", And(Or(Eq(order.Amount, 50.0), Eq(order.ID, 5)), Gt(order.AccountID, 0)), "WHERE ((orders.amount = ? OR orders.id = ?) AND orders.account_id > ?)"},
		{"OrAndOr", And(Or(Eq(order.Amount, 50.0), Eq(order.ID, 5)), Or(Gt(order.AccountID, 0), Lt(order.Amount, 100.0))), "WHERE ((orders.amount = ? OR orders.id = ?) AND (orders.account_id > ? OR orders.amount < ?))"},
	}

	for _, c := range complexCases {
		t.Run(c.name, func(t *testing.T) {
			exec := Delete[Order](c.where)
			require.NotNil(t, exec)

			q, err := exec.sql()
			require.NoError(t, err)
			require.NotEmpty(t, q)

			// attempt exact comparison with snapshot first
			snap := "TestSqlGeneration_Delete_" + c.name
			raw, err := loadRawSnapshot(snap)
			if err == nil {
				formatted := formatSQLForComparison(q)
				formatted = strings.ReplaceAll(formatted, "\r\n", "\n")
				raw = strings.ReplaceAll(raw, "\r\n", "\n")
				prefix := extractLeadingCommentPrefix(raw)
				combined := prefix + formatted
				if !strings.HasSuffix(combined, "\n") {
					combined += "\n"
				}
				require.Equal(t, normalizeForCompare(raw), normalizeForCompare(combined))
			} else {
				cleanGot := normalizeSQL(q)
				require.Contains(t, cleanGot, c.exp)
			}
		})
	}
}

// TestSqlGeneration_Update mirrors the Select/Delete tests but for UPDATE statements.
func TestSqlGeneration_Update(t *testing.T) {
	fields := order.All()
	schema := Schema(fields)

	cases := []struct {
		name    string
		where   Where
		expect  string
		hasArgs bool
	}{
		{"Eq", Eq(order.Amount, 100.0), "WHERE orders.amount = ?", true},
		{"Ne", Ne(order.Amount, 100.0), "WHERE orders.amount != ?", true},
		{"Gt", Gt(order.Amount, 10.5), "WHERE orders.amount > ?", true},
		{"Gte", Gte(order.Amount, 10.5), "WHERE orders.amount >= ?", true},
		{"Lt", Lt(order.Amount, 10.5), "WHERE orders.amount < ?", true},
		{"Lte", Lte(order.Amount, 10.5), "WHERE orders.amount <= ?", true},
		{"Like", Like(order.CreatedBy, "%john%"), "WHERE orders.created_by LIKE ?", true},
		{"InNonEmpty", In(order.ID, 1, 2, 3), "WHERE orders.id IN (?,?,?)", true},
		{"InEmpty", In(order.ID), "WHERE 1=0", false},
		{"And", And(Eq(order.Amount, 50.0), Gt(order.ID, 0)), "WHERE (orders.amount = ? AND orders.id > ?)", true},
		{"Or", Or(Eq(order.Amount, 50.0), Eq(order.ID, 5)), "WHERE (orders.amount = ? OR orders.id = ?)", true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// pass schema explicitly to Update
			exec := Update[Order](schema, nil)(c.where)
			require.NotNil(t, exec)

			q, err := exec.sql()
			require.NoError(t, err)
			require.NotEmpty(t, q)

			// ensure private field not present
			require.NotContains(t, q, "internalNotes")

			// snapshot comparison
			snapName := "TestSqlGeneration_Update_" + c.name
			rawExp, err := loadRawSnapshot(snapName)
			if err == nil {
				formatted := formatSQLForComparison(q)
				formatted = strings.ReplaceAll(formatted, "\r\n", "\n")
				rawExp = strings.ReplaceAll(rawExp, "\r\n", "\n")
				prefix := extractLeadingCommentPrefix(rawExp)
				combined := prefix + formatted
				if !strings.HasSuffix(combined, "\n") {
					combined += "\n"
				}
				require.Equal(t, normalizeForCompare(rawExp), normalizeForCompare(combined), "generated UPDATE SQL differs from exact snapshot %s", snapName)
			} else {
				cleanGot := normalizeSQL(q)
				if c.expect != "" {
					require.Contains(t, cleanGot, c.expect)
				}
			}

			// args via updateSQL with nil getter will include where args only
			_, args, err := updateSQL[Order](schema, nil, c.where)
			require.NoError(t, err)
			if c.hasArgs {
				require.True(t, args != nil && len(args) > 0)
			} else {
				require.True(t, args == nil || len(args) == 0)
			}
		})
	}

	// Complex nested examples
	complexCases := []struct {
		name  string
		where Where
		exp   string
	}{
		{"OrAnd", And(Or(Eq(order.Amount, 50.0), Eq(order.ID, 5)), Gt(order.AccountID, 0)), "WHERE ((orders.amount = ? OR orders.id = ?) AND orders.account_id > ?)"},
		{"OrAndOr", And(Or(Eq(order.Amount, 50.0), Eq(order.ID, 5)), Or(Gt(order.AccountID, 0), Lt(order.Amount, 100.0))), "WHERE ((orders.amount = ? OR orders.id = ?) AND (orders.account_id > ? OR orders.amount < ?))"},
	}

	for _, c := range complexCases {
		t.Run(c.name, func(t *testing.T) {
			// pass schema explicitly to Update
			exec := Update[Order](schema, nil)(c.where)
			require.NotNil(t, exec)

			q, err := exec.sql()
			require.NoError(t, err)
			require.NotEmpty(t, q)

			// attempt exact comparison
			snap := "TestSqlGeneration_Update_" + c.name
			raw, err := loadRawSnapshot(snap)
			if err == nil {
				formatted := formatSQLForComparison(q)
				formatted = strings.ReplaceAll(formatted, "\r\n", "\n")
				raw = strings.ReplaceAll(raw, "\r\n", "\n")
				prefix := extractLeadingCommentPrefix(raw)
				combined := prefix + formatted
				if !strings.HasSuffix(combined, "\n") {
					combined += "\n"
				}
				require.Equal(t, normalizeForCompare(raw), normalizeForCompare(combined))
			} else {
				cleanGot := normalizeSQL(q)
				require.Contains(t, cleanGot, c.exp)
			}
		})
	}
}

func TestJoinAPIs_SQLGeneration(t *testing.T) {
	// prepare schema for Order and pass it explicitly
	fields := order.All()
	schema := Schema(fields)

	join := "JOIN profiles ON profiles.account_id = orders.account_id"

	t.Run("QueryJoin", func(t *testing.T) {
		exec := QueryJoin(schema)(join, Eq(order.Amount, 100.0))
		require.NotNil(t, exec)
		q, err := exec.sql()
		require.NoError(t, err)
		// should include the injected join and the WHERE clause
		require.True(t, strings.Contains(q, "JOIN profiles ON profiles.account_id = orders.account_id"), "join clause missing: %s", q)
		require.True(t, strings.Contains(strings.ToUpper(q), "WHERE"), "where missing: %s", q)
	})

	t.Run("DeleteJoin", func(t *testing.T) {
		exec := DeleteJoin[Order](join, nil)
		require.NotNil(t, exec)
		q, err := exec.sql()
		require.NoError(t, err)
		// expect DELETE FROM <table> WHERE EXISTS (SELECT 1 FROM profiles WHERE ...)
		require.True(t, strings.HasPrefix(strings.TrimSpace(q), "DELETE FROM orders"), "unexpected delete prefix: %s", q)
		require.True(t, strings.Contains(q, "EXISTS (SELECT 1 FROM profiles WHERE profiles.account_id = orders.account_id"), "exists subquery missing: %s", q)
	})

	t.Run("UpdateJoin", func(t *testing.T) {
		// values can be nil for SQL generation test; provide schema explicitly
		exec := UpdateJoin[Order](schema, nil)(join, nil)
		require.NotNil(t, exec)
		q, err := exec.sql()
		require.NoError(t, err)
		// expect UPDATE <table> SET ... WHERE EXISTS(...)
		require.True(t, strings.HasPrefix(strings.TrimSpace(q), "UPDATE orders"), "unexpected update prefix: %s", q)
		require.True(t, strings.Contains(q, "EXISTS (SELECT 1 FROM profiles WHERE profiles.account_id = orders.account_id"), "exists subquery missing in update: %s", q)
	})
}

func TestMapValueObject(t *testing.T) {
	cases := []struct {
		name    string
		payload map[string]any
		wantP   bool
		wantKey string
	}{
		{name: "validKey", payload: map[string]any{"accounts.email": "user@example.com"}, wantP: false, wantKey: "accounts.email"},
		{name: "missingDot", payload: map[string]any{"accounts": 1}, wantP: true},
		{name: "emptyKey", payload: map[string]any{"": "empty"}, wantP: true},
		{name: "singlePartWithDotAtEnd", payload: map[string]any{"accounts.": 2}, wantP: false, wantKey: "accounts."},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantP {
				require.Panics(t, func() { MapValueObject(FlatMap(tc.payload)) })
				return
			}
			vo := MapValueObject(FlatMap(tc.payload))
			require.NotNil(t, vo)
			require.Contains(t, vo.Fields(), tc.wantKey)
			op := vo.Get(tc.wantKey)
			require.True(t, op.IsPresent())
			require.Equal(t, tc.payload[tc.wantKey], op.MustGet())
		})
	}
}
