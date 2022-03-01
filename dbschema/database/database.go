package database

import (
	"github.com/jmoiron/sqlx"
	pkgerrs "github.com/pkg/errors"
	"github.com/zoobr/csxlib/dbschema/schemafield"
)

type Driver string // type of database driver (postgres, mysql etc)

const (
	DriverPostgreSQL Driver = "postgres"
	DriverMySQL      Driver = "mysql"
)
const MAX_OPEN_CONNS = 100                      // default max count of opened connections
const DEFAULT_MIGRATIONS_PATH = "db/migrations" // default path for migrations

// Database is interface providing common methods to support different databases.
type Database interface {
	// Init initializes database by database params.
	Init(params *DatabaseParams)
	// Connect makes database connection.
	Connect() error
	// GetParams returns database params.
	GetParams() *DatabaseParams

	// IsTableExists checks if a table with the given name exists in the database.
	IsTableExists(tableName string) bool
	// GetColumnsInfo returns info about table columns from database.
	GetColumnsInfo(tableName string) ([]*DBColumnInfo, error)
	// CreateTable creates new table using table name & list of columns.
	CreateTable(tableName string, fields []*schemafield.SchemaField) error
	// AlterTable updates table in the database according to the schema.
	AlterTable(tableName string, fields []*schemafield.SchemaField) error

	// Migrate make migrations from source to database
	Migrate() error

	// BeginTransaction starts database transaction
	BeginTransaction() (*sqlx.Tx, error)

	// Select executes a SELECT statement and stores list of rows into dest
	Select(tx *sqlx.Tx, dest interface{}, query *Query, args ...interface{}) error
	// Get executes a SELECT statement and stores result row into dest
	Get(tx *sqlx.Tx, dest interface{}, query *Query, args ...interface{}) error
	// Insert executes INSERT statement which saves data to DB and returns values if it needs.
	Insert(tx *sqlx.Tx, prepared *PreparedData, tableName string, ext *InsertExt, args ...interface{}) error
	// Update executes UPDATE statement which updates data in DB and returns values if it needs.
	Update(tx *sqlx.Tx, prepared *PreparedData, tableName, where string, ret *ReturningDest, args ...interface{}) error
	// Delete executes DELETE statement which removes data from DB and returns values if it needs.
	Delete(tx *sqlx.Tx, tableName, where string, ret *ReturningDest, args ...interface{}) error
}

// DatabaseParams is a struct for database params.
type DatabaseParams struct {
	Name             string                 // the name under which the database will be registered
	DBName           string                 // database name (optional). If is not defined, Name is used
	Driver           Driver                 // database driver (postgres, mysql etc)
	ConnectionString string                 // database connection string
	MaxOpenConns     int                    // max count of opened connections
	Ext              map[string]interface{} // database specific info (like engine for MySQL databases)
}

// DBColumnInfo is a struct for info about column (from database).
type DBColumnInfo struct {
	Name     string  `db:"name"`     // column name
	Type     string  `db:"type"`     // column type
	Nullable bool    `db:"nullable"` // whether column is NULL or NOT NULL
	Length   int     `db:"length"`   // length of column type
	Default  *string `db:"default"`  // default column value
}

// ReturningDest is a struct for lists of values & destinations for RETURNING clause of INSERT or UPDATE statements.
type ReturningDest struct {
	list string        // list of returned db columns
	dest []interface{} // destinations for RETURNING clause
}

// Returning creates a struct for lists of values & destinations for RETURNING clause of INSERT or UPDATE statements.
func Returning(list string, dest ...interface{}) *ReturningDest {
	return &ReturningDest{list: list, dest: dest}
}

// NewDatabase creates new instance of Database interface using params
func NewDatabase(params *DatabaseParams) (Database, error) {
	if params.Name == "" {
		return nil, pkgerrs.New("database name is missing")
	}
	if params.ConnectionString == "" {
		return nil, pkgerrs.New("connection string is missing")
	}

	if params.DBName == "" {
		params.DBName = params.Name
	}

	var db Database
	switch params.Driver {
	case DriverPostgreSQL:
		db = &postgreSQL{}
	case DriverMySQL:
		db = &mySQL{}
	}
	db.Init(params)

	return db, nil
}

// New creates & registers list of Database instances using list of params
func New(params ...*DatabaseParams) {
	cnt := len(params)
	dbs := make([]Database, 0, cnt)

	for _, p := range params {
		db, err := NewDatabase(p)
		if err != nil {
			panic(err)
		}
		dbs = append(dbs, db)
	}

	manager.MustRegister(dbs...)
}
