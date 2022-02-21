package database

import (
	"fmt"
	"strings"

	"github.com/zoobr/csxlib/dbschema/schemafield"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jmoiron/sqlx"
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
			COALESCE(c.character_maximum_length, 0) AS "length", c.column_default AS "default"
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

	for i := 0; i < len(fields); i++ {
		f := fields[i]

		sb.WriteString("ALTER TABLE ")
		sb.WriteString(tableName)
		sb.WriteString(" ADD ")
		pgsql.prepareColumn(&sb, f)
		sb.WriteString(";\n")
	}

	return sb.String()
}
