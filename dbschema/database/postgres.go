package database

import (
	"fmt"
	"strings"

	"github.com/zoobr/csxlib/dbschema/schemafield"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jmoiron/sqlx"

	pkgerrs "github.com/pkg/errors"
)

// postgreSQL is a struct which implements Database interface for supproting PostgreSQL
type postgreSQL struct {
	*DatabaseParams
	conn *sqlx.DB // connection instance
}

// Init initializes database by database params.
func (pgsql *postgreSQL) Init(params *DatabaseParams) {
	pgsql.DatabaseParams = params
	if pgsql.MaxOpenConns <= 0 {
		pgsql.MaxOpenConns = MAX_OPEN_CONNS
	}
}

// Connect makes database connection.
func (pgsql *postgreSQL) Connect() error {
	var err error
	pgsql.conn, err = sqlx.Connect(string(pgsql.Driver), pgsql.ConnectionString)
	if err != nil {
		return err
	}

	pgsql.conn.SetMaxOpenConns(pgsql.MaxOpenConns)

	return nil
}

// GetParams returns database params.
func (pgsql *postgreSQL) GetParams() *DatabaseParams { return pgsql.DatabaseParams }

// IsTableExists checks if a table with the given name exists in the database.
func (pgsql *postgreSQL) IsTableExists(tableName string) bool {
	var isExists bool
	query := `SELECT EXISTS (
		SELECT t.table_name FROM information_schema."tables" t WHERE t.table_name = $1
	);`

	err := pgsql.conn.Get(&isExists, query, tableName)
	if err != nil {
		panic(nil)
	}

	return isExists
}

// GetColumnsInfo returns info about table columns from database.
func (pgsql *postgreSQL) GetColumnsInfo(tableName string) ([]*DBColumnInfo, error) {
	data := []*DBColumnInfo{}
	query := `SELECT c.column_name AS "name", c.udt_name AS "type",
			(CASE c.is_nullable WHEN 'YES' THEN true WHEN 'NO' THEN false END) AS "nullable",
			COALESCE(c.character_maximum_length, c.numeric_precision, 0) AS "length", c.column_default AS "default"
		FROM information_schema."columns" c
		WHERE c.table_name = $1;`

	err := pgsql.conn.Select(&data, query, tableName)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// CreateTable creates new table using table name & list of columns.
func (pgsql *postgreSQL) CreateTable(tableName string, fields []*schemafield.SchemaField) error {
	queryStr := pgsql.prepareCreateTableStmt(tableName, fields)

	_, err := pgsql.conn.Exec(queryStr)
	return err
}

// AlterTable updates table in the database according to the schema.
// Now it only adds new columns to table. This behaviour can be changed later.
func (pgsql *postgreSQL) AlterTable(tableName string, fields []*schemafield.SchemaField) error {
	queryStr := pgsql.prepareAddColumnsStmt(tableName, fields)

	_, err := pgsql.conn.Exec(queryStr)
	return err
}

// Migrate make migrations from source to database.
func (pgsql *postgreSQL) Migrate() error {
	driver, err := postgres.WithInstance(pgsql.conn.DB, &postgres.Config{
		DatabaseName: pgsql.DBName,
	})
	if err != nil {
		return err
	}

	sourceURL := fmt.Sprintf("file://%s/%s", DEFAULT_MIGRATIONS_PATH, pgsql.DBName)
	m, err := migrate.NewWithDatabaseInstance(sourceURL, pgsql.DBName, driver)
	if err != nil {
		return err
	}

	return m.Up()
}

// BeginTransaction starts database transaction
func (pgsql *postgreSQL) BeginTransaction() (*sqlx.Tx, error) { return pgsql.conn.Beginx() }

// Select executes a SELECT statement and stores list of rows into dest. Supports transaction.
func (pgsql *postgreSQL) Select(tx *sqlx.Tx, dest interface{}, query *Query, args ...interface{}) error {
	queryStr, err := prepareQuery(query)
	if err != nil {
		return err
	}

	if tx != nil {
		return tx.Select(dest, queryStr, args...)
	}
	return pgsql.conn.Select(dest, queryStr, args...)
}

// Get executes a SELECT statement and stores result row into dest. Supports transaction.
func (pgsql *postgreSQL) Get(tx *sqlx.Tx, dest interface{}, query *Query, args ...interface{}) error {
	query.Limit = 1
	queryStr, err := prepareQuery(query)
	if err != nil {
		return err
	}

	if tx != nil {
		return tx.Get(dest, queryStr, args...)
	}
	return pgsql.conn.Get(dest, queryStr, args...)
}

// Insert executes INSERT statement which saves data to DB and returns values if it needs.
func (pgsql *postgreSQL) Insert(tx *sqlx.Tx, prepared *PreparedData, tableName string, ext *InsertExt, args ...interface{}) error {
	query, err := pgsql.prepareInsertStmt(tableName, prepared.DBFields, len(args), len(prepared.Values), prepared.Query, ext)
	if err != nil {
		return err
	}
	allArgs := append(args, prepared.Values...)

	// RETURNING clause is exists
	if ext != nil && ext.Returning != nil {
		ret := ext.Returning
		if ret.dest == nil {
			return pkgerrs.New("missing destinations for RETURNING clause")
		}

		if tx != nil {
			return tx.QueryRowx(query, allArgs...).Scan(ret.dest...)
		}
		return pgsql.conn.QueryRowx(query, allArgs...).Scan(ret.dest...)
	}

	// RETURNING clause is not exists
	if tx != nil {
		_, err = tx.Exec(query, allArgs...)
	} else {
		_, err = pgsql.conn.Exec(query, allArgs...)
	}
	return err
}

// Update executes UPDATE statement which updates data in DB and returns values if it needs.
func (pgsql *postgreSQL) Update(tx *sqlx.Tx, prepared *PreparedData, tableName, where string, ret *ReturningDest, args ...interface{}) error {
	// 1 - args for WHERE clause, 2 - values for updating
	allArgs := append(args, prepared.Values...)

	// RETURNING clause is exists
	if ret != nil {
		if ret.dest == nil {
			return pkgerrs.New("missing destinations for RETURNING clause")
		}

		query, err := pgsql.prepareUpdateStmt(tableName, where, len(args), prepared.DBFields, prepared.Queries, ret.list)
		if err != nil {
			return err
		}
		if tx != nil {
			return tx.QueryRowx(query, allArgs...).Scan(ret.dest...)
		}
		return pgsql.conn.QueryRowx(query, allArgs...).Scan(ret.dest...)
	}

	// RETURNING clause is not exists
	query, err := pgsql.prepareUpdateStmt(tableName, where, len(args), prepared.DBFields, prepared.Queries)
	if err != nil {
		return err
	}
	if tx != nil {
		_, err = tx.Exec(query, allArgs...)
	} else {
		_, err = pgsql.conn.Exec(query, allArgs...)
	}
	return err
}

// Delete executes DELETE statement which removes data from DB and returns values if it needs
func (pgsql *postgreSQL) Delete(tx *sqlx.Tx, tableName, where string, ret *ReturningDest, args ...interface{}) error {
	// RETURNING clause is exists
	if ret != nil {
		if ret.dest == nil {
			return pkgerrs.New("missing destinations for RETURNING clause")
		}

		query := pgsql.prepareDeleteStmt(tableName, where, ret.list)
		if tx != nil {
			return tx.QueryRowx(query, args...).Scan(ret.dest...)
		}
		return pgsql.conn.QueryRowx(query, args...).Scan(ret.dest...)
	}

	// RETURNING clause is not exists
	query := pgsql.prepareDeleteStmt(tableName, where)
	var err error
	if tx != nil {
		_, err = tx.Exec(query, args...)
	} else {
		_, err = pgsql.conn.Exec(query, args...)
	}
	return err
}

// ----------------------------------------------------------------------------
// preparing query statements
// ----------------------------------------------------------------------------

// prepareColumn prepares SQL string for table column.
func (pgsql *postgreSQL) prepareColumn(builder *strings.Builder, field *schemafield.SchemaField) {
	builder.WriteByte('"')
	builder.WriteString(field.DBName)
	builder.WriteString("\" ")
	builder.WriteString(field.DBType)
	if field.Length > 0 {
		builder.WriteString(fmt.Sprintf("(%d)", field.Length))
	}

	if field.Nullable {
		builder.WriteString(" NULL")
	} else {
		builder.WriteString(" NOT NULL")
	}
	if len(field.Default) > 0 {
		builder.WriteString(" DEFAULT ")
		builder.WriteString(field.Default)
	}
	if len(field.Comment) > 0 {
		builder.WriteString(" COMMENT '")
		builder.WriteString(field.Comment)
		builder.WriteByte('\'')
	}
}

// prepareCreateTableStmt prepares string of SQL CREATE TABLE statement.
func (pgsql *postgreSQL) prepareCreateTableStmt(tableName string, fields []*schemafield.SchemaField) string /* (string, error) */ {
	var sb strings.Builder

	sb.WriteString("CREATE TABLE ")
	sb.WriteString(tableName)
	sb.WriteString(" (")

	// preparing table columns
	cnt := len(fields)
	pks := make([]string, 0, cnt) // primary keys
	for i := 0; i < cnt; i++ {
		f := fields[i]

		sb.WriteString("\n")
		pgsql.prepareColumn(&sb, f)

		if f.IsPrimaryKey {
			pks = append(pks, `"`+f.DBName+`"`)
		}

		if i != cnt-1 { // if not last field
			sb.WriteByte(',')
		}
	}

	// preparing primary key
	if len(pks) > 0 {
		sb.WriteString(",\nPRIMARY KEY (")
		sb.WriteString(strings.Join(pks, ", "))
		sb.WriteByte(')')
	}

	sb.WriteString("\n);")

	return sb.String()
}

// prepareAddColumnsStmt prepares string of SQL ALTER TABLE ADD COLUMN statement.
func (pgsql *postgreSQL) prepareAddColumnsStmt(tableName string, fields []*schemafield.SchemaField) string {
	var sb strings.Builder
	cnt := len(fields)

	sb.WriteString("ALTER TABLE ")
	sb.WriteString(tableName)

	for i := 0; i < cnt; i++ {
		f := fields[i]

		sb.WriteString("\nADD COLUMN ")
		pgsql.prepareColumn(&sb, f)
		if i != cnt-1 { // if not last column
			sb.WriteByte(',')
		}
	}
	sb.WriteByte(';')

	return sb.String()
}

// prepareInsertStmt prepares INSERT statement.
func (pgsql *postgreSQL) prepareInsertStmt(tableName string, fields []string, argsLen, valsLen int, q *Query, ext *InsertExt) (string, error) {
	var sb strings.Builder
	cntf := len(fields)

	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
	sb.WriteString(" (")

	for i := 0; i < cntf; i++ {
		sb.WriteString(fmt.Sprintf(`"%s"`, fields[i]))
		if i != cntf-1 { // if not last field
			sb.WriteByte(',')
		}
	}
	sb.WriteByte(')')

	if q != nil {
		// values gets from SELECT query
		selectStmt, err := prepareQuery(q)
		if err != nil {
			return "", err
		}
		sb.WriteString(fmt.Sprintf("\n(%s)", selectStmt))
	} else if ext != nil && len(ext.WhereNotExists) > 0 {
		// values gets from WHERE NOT EXISTS query
		sb.WriteString("\nSELECT ")

		argNum := argsLen + 1
		for i := 0; i < valsLen; i++ {
			sb.WriteString(fmt.Sprintf("$%d", argNum))
			if i != valsLen-1 { // not last arg
				sb.WriteString(", ")
			}
			argNum++
		}

		sb.WriteString(" WHERE NOT EXISTS\n(SELECT * FROM ")
		sb.WriteString(tableName)
		sb.WriteString(" WHERE ")
		sb.WriteString(ext.WhereNotExists)
		sb.WriteByte(')')
	} else {
		//values gets from binding
		sb.WriteString(" VALUES (")

		argNum := 1
		for i := 0; i < valsLen; i++ {
			sb.WriteString(fmt.Sprintf("$%d", argNum))
			if i != valsLen-1 { // not last arg
				sb.WriteString(", ")
			}
			argNum++
		}

		sb.WriteByte(')')
	}

	if ext != nil {
		if ext.OnConflict != nil {
			conflict := ext.OnConflict
			sb.WriteString(" ON CONFLICT (")
			sb.WriteString(conflict.Object)
			sb.WriteString(") DO ")
			switch conflict.Strategy {
			case OnConflictDoNothing:
				sb.WriteString("NOTHING")
			default:
				return "", pkgerrs.New("wrong ON CONFLICT strategy")
			}
		}

		if ext.Returning != nil {
			sb.WriteString("\nRETURNING ")
			sb.WriteString(ext.Returning.list)
		}
	}

	sb.WriteByte(';')

	return sb.String(), nil
}

// prepareUpdateStmt prepares UPDATE statement.
func (pgsql *postgreSQL) prepareUpdateStmt(tableName, where string, argsLen int, fields []string, queries map[string]*Query, returning ...string) (string, error) {
	var sb strings.Builder

	sb.WriteString("UPDATE ")
	sb.WriteString(tableName)
	sb.WriteString(" SET ")

	// args is values
	cntf := len(fields)
	argNum := argsLen + 1
	for i := 0; i < cntf; i++ {
		sb.WriteString(fmt.Sprintf("%s = $%d", fields[i], argNum))
		if i != cntf-1 { // if not last field
			sb.WriteString(", ")
		}
		argNum++
	}

	// args is queries
	cntq, i := len(queries), 0
	if cntq > 0 {
		sb.WriteString(", ")
		for field, query := range queries {
			queryStr, err := prepareQuery(query)
			if err != nil {
				return "", err
			}

			sb.WriteString(fmt.Sprintf("%s = (%s)", field, queryStr))
			if i != cntq-1 { // if not last query
				sb.WriteString(", ")
			}
			i++
		}
	}

	sb.WriteString(" WHERE ")
	sb.WriteString(where)

	if len(returning) == 1 {
		sb.WriteString(" RETURNING ")
		sb.WriteString(returning[0])
	}

	sb.WriteByte(';')

	return sb.String(), nil
}

// prepareDeleteStmt prepares DELETE statement.
func (pgsql *postgreSQL) prepareDeleteStmt(tableName, where string, returning ...string) string {
	var sb strings.Builder

	sb.WriteString("DELETE FROM ")
	sb.WriteString(tableName)
	sb.WriteString(" WHERE ")
	sb.WriteString(where)

	if len(returning) == 1 {
		sb.WriteString(" RETURNING ")
		sb.WriteString(returning[0])
	}

	sb.WriteByte(';')

	return sb.String()
}
