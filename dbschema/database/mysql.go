package database

import (
	"fmt"
	"strings"

	"github.com/zoobr/csxlib/dbschema/schemafield"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jmoiron/sqlx"
)

// mySQL is a struct which implements Database interface for supproting MySQL
type mySQL struct {
	*DatabaseParams
	conn *sqlx.DB // database connection instance
}

// Init initializes database by database params.
func (msql *mySQL) Init(params *DatabaseParams) {
	msql.DatabaseParams = params
	if msql.MaxOpenConns <= 0 {
		msql.MaxOpenConns = MAX_OPEN_CONNS
	}
	if msql.Ext == nil {
		msql.Ext = make(map[string]interface{})
	}
	if _, ok := msql.Ext["engine"]; !ok {
		msql.Ext["engine"] = "InnoDB"
	}
}

// Connect makes database connection.
func (msql *mySQL) Connect() error {
	var err error
	msql.conn, err = sqlx.Connect(string(msql.Driver), msql.ConnectionString)
	if err != nil {
		return err
	}

	msql.conn.SetMaxOpenConns(msql.MaxOpenConns)

	return nil
}

// GetParams returns database params.
func (msql *mySQL) GetParams() *DatabaseParams { return msql.DatabaseParams }

// IsTableExists checks if a table with the given name exists in the database.
func (msql *mySQL) IsTableExists(tableName string) bool {
	var isExists byte
	query := `SELECT EXISTS (
		SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_NAME = ?
	);`

	err := msql.conn.Get(&isExists, query, tableName)
	if err != nil {
		panic(nil)
	}

	return isExists == 1
}

// GetColumnsInfo returns info about table columns from database.
func (msql *mySQL) GetColumnsInfo(tableName string) ([]*DBColumnInfo, error) {
	data := []*DBColumnInfo{}
	query := ` SELECT COLUMN_NAME AS "name", COLUMN_TYPE AS "type",
			(CASE IS_NULLABLE WHEN 'YES' THEN true WHEN 'NO' THEN false END) AS "nullable",
			COALESCE(CHARACTER_MAXIMUM_LENGTH, 0) AS "length",
			COLUMN_DEFAULT AS "default"
		FROM information_schema.COLUMNS
		WHERE TABLE_NAME = ?;`

	err := msql.conn.Select(&data, query, tableName)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// CreateTable creates new table using table name & list of columns.
func (msql *mySQL) CreateTable(tableName string, fields []*schemafield.SchemaField) error {
	queryStr := msql.prepareCreateTableStmt(tableName, fields, msql.Ext)

	_, err := msql.conn.Exec(queryStr)
	return err
}

// AlterTable updates table in the database according to the schema.
// Now it only adds new columns to table. This behaviour can be changed later.
func (msql *mySQL) AlterTable(tableName string, fields []*schemafield.SchemaField) error {
	queryStr := msql.prepareAddColumnsStmt(tableName, fields)

	_, err := msql.conn.Exec(queryStr)
	return err
}

// Migrate make migrations from source to database.
func (msql *mySQL) Migrate() error {
	driver, err := mysql.WithInstance(msql.conn.DB, &mysql.Config{
		DatabaseName: msql.DBName,
	})
	if err != nil {
		return err
	}

	sourceURL := fmt.Sprintf("file://%s/%s", DEFAULT_MIGRATIONS_PATH, msql.DBName)
	m, err := migrate.NewWithDatabaseInstance(sourceURL, msql.DBName, driver)
	if err != nil {
		return err
	}

	return m.Up()
}

// BeginTransaction starts database transaction
func (msql *mySQL) BeginTransaction() (*sqlx.Tx, error) { return msql.conn.Beginx() }

// Select executes a SELECT statement and stores list of rows into dest. Supports transaction.
func (msql *mySQL) Select(tx *sqlx.Tx, dest interface{}, query *Query, args ...interface{}) error {
	queryStr, err := prepareQuery(query)
	if err != nil {
		return err
	}

	if tx != nil {
		return tx.Select(dest, queryStr, args...)
	}
	return msql.conn.Select(dest, queryStr, args...)
}

// Get executes a SELECT statement and stores result row into dest. Supports transaction.
func (msql *mySQL) Get(tx *sqlx.Tx, dest interface{}, query *Query, args ...interface{}) error {
	query.Limit = 1
	queryStr, err := prepareQuery(query)
	if err != nil {
		return err
	}

	if tx != nil {
		return tx.Get(dest, queryStr, args...)
	}
	return msql.conn.Get(dest, queryStr, args...)
}

// ----------------------------------------------------------------------------
// preparing query statements
// ----------------------------------------------------------------------------

// prepareColumn prepares SQL string for table column.
// TODO:: Add AUTO_INCREMENT supporting
func (msql *mySQL) prepareColumn(builder *strings.Builder, field *schemafield.SchemaField) {
	dbType := strings.Split(field.DBType, ",")

	builder.WriteByte('`')
	builder.WriteString(field.DBName)
	builder.WriteString("` ")
	builder.WriteString(dbType[0])
	if field.Length > 0 {
		builder.WriteString(fmt.Sprintf("(%d)", field.Length))
	}
	if len(dbType) == 2 { // unsigned
		builder.WriteString(fmt.Sprintf(" %s", dbType[1]))
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
func (msql *mySQL) prepareCreateTableStmt(tableName string, fields []*schemafield.SchemaField, ext map[string]interface{}) string /* (string, error) */ {
	var sb strings.Builder

	sb.WriteString("CREATE TABLE `")
	sb.WriteString(tableName)
	sb.WriteString("` (")

	// preparing table columns
	cnt := len(fields)
	pks := make([]string, 0, cnt) // primary keys
	for i := 0; i < cnt; i++ {
		f := fields[i]

		sb.WriteString("\n")
		msql.prepareColumn(&sb, f)

		if f.IsPrimaryKey {
			pks = append(pks, "`"+f.DBName+"`")
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

	sb.WriteString("\n)")
	if engine, ok := ext["engine"]; ok {
		sb.WriteString(" ENGINE=")
		sb.WriteString(engine.(string))
	}
	// TODO:: Add AUTO_INCREMENT supporting
	sb.WriteString(" DEFAULT CHARSET=utf8;")

	return sb.String()
}

// prepareAddColumnsStmt prepares string of SQL ALTER TABLE ADD COLUMN statement.
func (msql *mySQL) prepareAddColumnsStmt(tableName string, fields []*schemafield.SchemaField) string {
	var sb strings.Builder

	for i := 0; i < len(fields); i++ {
		f := fields[i]

		sb.WriteString("ALTER TABLE `")
		sb.WriteString(tableName)
		sb.WriteString("` ADD ")
		msql.prepareColumn(&sb, f)
		sb.WriteString(";\n")
	}

	return sb.String()
}
