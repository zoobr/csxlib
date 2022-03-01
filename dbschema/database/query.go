package database

import (
	"fmt"
	"strings"

	pkgerrs "github.com/pkg/errors"
)

// ----------------------------------------------------------------------------
// SELECT statement
// ----------------------------------------------------------------------------

// UnionClause is a struct which stores UNION clause
type UnionClause struct {
	All bool // ALL sign
	Query
}

// AliasedQuery is a struct for SQL SELECT statement with alias
type AliasedQuery struct {
	Alias string
	Query
}

// Query is a base struct for SELECT statement
type Query struct {
	With  []*AliasedQuery // WITH clause
	Union *UnionClause    // UNION clause

	Select string      // list of columns
	From   interface{} // FROM clause (string || AliasedQuery)
	Join   string      // JOIN clause
	Where  string      // WHERE clause
	Group  string      // GROUP BY clause
	Having string      // HAVING clause
	Order  string      // ORDER BY clause
	Limit  int         // LIMIT clause
	Offset int         // OFFSET clause
}

// SetDefaults sets default values for some obligatory query fields
func (q *Query) SetDefaults(tableName string) {
	if len(q.Select) == 0 {
		q.Select = "*"
	}
	if q.From == nil {
		q.From = tableName
	}
}

// prepareFromClause prepares SQL string for FROM clause.
func prepareFromClause(builder *strings.Builder, cl interface{}) error {
	builder.WriteString("\nFROM ")

	switch clause := cl.(type) {
	case string:
		builder.WriteString(clause)
	case *AliasedQuery:
		if len(clause.Alias) == 0 {
			return pkgerrs.New("the subquery in the FROM clause must have an alias")
		}

		builder.WriteByte('(')
		err := prepareSelectStatement(builder, &clause.Query)
		if err != nil {
			return err
		}
		builder.WriteString(fmt.Sprintf(") AS %s", clause.Alias))
	default:
		return pkgerrs.New("FROM clause must be string or *database.Query")
	}

	return nil
}

// prepareSelectStatement prepares SQL string for SELECT statement.
func prepareSelectStatement(builder *strings.Builder, st *Query) error {
	builder.WriteString("\nSELECT ")
	builder.WriteString(st.Select)

	// preparing FROM clause
	err := prepareFromClause(builder, st.From)
	if err != nil {
		return err
	}

	// preparing JOIN, WHERE, GROUP BY, ORDER BY, LIMIT, OFFSET clauses
	if len(st.Join) > 0 {
		builder.WriteByte('\n')
		builder.WriteString(st.Join)
	}
	if len(st.Where) > 0 {
		builder.WriteString("\nWHERE ")
		builder.WriteString(st.Where)
	}
	if len(st.Group) > 0 {
		builder.WriteString("\nGROUP BY ")
		builder.WriteString(st.Group)
		if len(st.Having) > 0 {
			builder.WriteString("\nHAVING ")
			builder.WriteString(st.Having)
		}
	}
	if len(st.Order) > 0 {
		builder.WriteString("\nORDER BY ")
		builder.WriteString(st.Order)
	}
	if st.Limit > 0 {
		builder.WriteString(fmt.Sprintf("\nLIMIT %d", st.Limit))
	}
	if st.Offset > 0 {
		builder.WriteString(fmt.Sprintf("\nOFFSET %d", st.Offset))
	}

	return nil
}

// prepareFromClause prepares SQL string for WITH clause.
func prepareWithClause(builder *strings.Builder, cl []*AliasedQuery) error {
	cnt := len(cl)

	builder.WriteString("WITH")
	for i := 0; i < cnt; i++ {
		if len(cl[i].Alias) == 0 {
			return pkgerrs.New("the subquery in the WITH clause must have an alias")
		}

		builder.WriteString(fmt.Sprintf(" %s AS (", cl[i].Alias))
		err := prepareSelectStatement(builder, &cl[i].Query)
		if err != nil {
			return err
		}

		builder.WriteByte(')')
		if i != cnt-1 { // if not last SELECT
			builder.WriteByte(',')
		}
	}

	return nil
}

// prepareUnionClause prepares SQL string for UNION clause.
func prepareUnionClause(builder *strings.Builder, cl *UnionClause) error {
	var sb strings.Builder

	sb.WriteString("\nUNION")
	if cl.All {
		sb.WriteString(" ALL")
	}
	sb.WriteByte('\n')

	err := prepareSelectStatement(builder, &cl.Query)
	if err != nil {
		return err
	}

	if cl.Query.Union != nil {
		err := prepareUnionClause(builder, cl.Query.Union)
		if err != nil {
			return err
		}
	}

	return nil
}

// prepareQuery prepares SQL string for query.
func prepareQuery(q *Query) (string, error) {
	var sb strings.Builder

	// preparing WITH clause
	if len(q.With) > 0 {
		err := prepareWithClause(&sb, q.With)
		if err != nil {
			return "", err
		}
	}

	// preparing top-level SELECT clause
	err := prepareSelectStatement(&sb, q)
	if err != nil {
		return "", err
	}

	// preparing UNION clause
	if q.Union != nil {
		err := prepareUnionClause(&sb, q.Union)
		if err != nil {
			return "", err
		}
	}

	return sb.String(), nil
}

// ----------------------------------------------------------------------------
// INSERT statement
// ----------------------------------------------------------------------------

const (
	OnConflictDoNothing = iota // ON CONFLICT DO NOTTHING strategy
)

// ConflictClause s a struct that represents ON CONFLICT clause.
type ConflictClause struct {
	Object   string
	Strategy int
}

// InsertExt is a struct that represents the extended clauses of INSERT statement.
type InsertExt struct {
	WhereNotExists string          // WHERE clause for inserting if not exists.
	OnConflict     *ConflictClause // ON CONFLICT clause. Doesn't supports in MySQL.
	Returning      *ReturningDest  // RETURNING clause & list of destinations.
}
