package database

import (
	pkgerrs "github.com/pkg/errors"
	"github.com/zoobr/csxlib/dbschema/schemafield"
)

type Driver string // type of database driver (postgres, mysql etc)

const MAX_OPEN_CONNS = 100 // default max count of opened connections

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
}

// DatabaseParams is a struct for database params.
type DatabaseParams struct {
	Name             string                 // database name
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

// NewDatabase creates new instance of Database interface using params
func NewDatabase(params *DatabaseParams) (Database, error) {
	if params.Name == "" {
		return nil, pkgerrs.New("database name is missing")
	}
	if params.ConnectionString == "" {
		return nil, pkgerrs.New("connection string is missing")
	}

	var db Database

	return db, nil
}
