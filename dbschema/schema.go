package dbschema

import (
	"github.com/jmoiron/sqlx"
	"github.com/zoobr/csxlib/dbschema/database"
	"github.com/zoobr/csxlib/dbschema/schemafield"
)

// schemaDatabases is a struct for list of schema databases
type schemaDatabases struct {
	master database.Database // instance of master database (obligatory)
	slave  database.Database // instance of slave database (optional)
}

// SchemaParams is a struct for schema params
type SchemaParams struct {
	Name              string      // schema name
	DatabaseName      string      // database name
	SlaveDatabaseName string      // slave database name (is exists)
	TableName         string      // name of table in database
	Model             interface{} // instance of model
}

// Schema is a struct representing the schema of a table in database
type Schema struct {
	SchemaParams
	fields []*schemafield.SchemaField // list of database columns
	dbs    schemaDatabases            // list of schema databases
}

func (s *Schema) _select(tx *sqlx.Tx, dest interface{}, query *database.Query, args ...interface{}) error {
	db := s.dbs.master
	if s.dbs.slave != nil {
		db = s.dbs.slave
	}
	query.SetDefaults(s.TableName)

	return db.Select(tx, dest, query, args...)
}

func (s *Schema) get(tx *sqlx.Tx, dest interface{}, query *database.Query, args ...interface{}) error {
	db := s.dbs.master
	if s.dbs.slave != nil {
		db = s.dbs.slave
	}
	query.SetDefaults(s.TableName)

	return db.Get(tx, dest, query, args...)
}

func (s *Schema) insert(tx *sqlx.Tx, data interface{}, ext *database.InsertExt, args ...interface{}) error {
	prepared, err := database.PrepareData(data, s.fields)
	if err != nil {
		return err
	}

	return s.dbs.master.Insert(tx, prepared, s.TableName, ext, args...)
}

func (s *Schema) update(tx *sqlx.Tx, data interface{}, where string, ret *database.ReturningDest, args ...interface{}) error {
	prepared, err := database.PrepareData(data, s.fields)
	if err != nil {
		return err
	}

	return s.dbs.master.Update(tx, prepared, s.TableName, where, ret, args...)
}

// BeginTransaction starts database transaction
func (s *Schema) BeginTransaction() (*sqlx.Tx, error) { return s.dbs.master.BeginTransaction() }

// Select executes a SELECT statement and stores list of rows into dest
func (s *Schema) Select(dest interface{}, query *database.Query, args ...interface{}) error {
	return s._select(nil, dest, query, args...)
}

// Select executes a SELECT statement and stores list of rows into dest. Supports transaction.
func (s *Schema) TransactSelect(tx *sqlx.Tx, dest interface{}, query *database.Query, args ...interface{}) error {
	return s._select(tx, dest, query, args...)
}

// SelectOne executes a SELECT statement and stores result row into dest.
func (s *Schema) SelectOne(dest interface{}, query *database.Query, args ...interface{}) error {
	return s.get(nil, dest, query, args...)
}

// SelectOne executes a SELECT statement and stores result row into dest. Supports transaction.
func (s *Schema) TransactSelectOne(tx *sqlx.Tx, dest interface{}, query *database.Query, args ...interface{}) error {
	return s.get(tx, dest, query, args...)
}

// Insert executes INSERT statement which saves data to DB and returns values if it needs.
func (s *Schema) Insert(data interface{}, ext *database.InsertExt, args ...interface{}) error {
	return s.insert(nil, data, ext, args...)
}

// Insert executes INSERT statement which saves data to DB and returns values if it needs. Supports transaction.
func (s *Schema) TransactInsert(tx *sqlx.Tx, data interface{}, ext *database.InsertExt, args ...interface{}) error {
	return s.insert(tx, data, ext, args...)
}

// Update executes UPDATE statement which updates data in DB.
func (s *Schema) Update(data interface{}, where string, args ...interface{}) error {
	return s.update(nil, data, where, nil, args...)
}

// Update executes UPDATE statement which updates data in DB. Supports transaction.
func (s *Schema) TransactUpdate(tx *sqlx.Tx, data interface{}, where string, ret *database.ReturningDest, args ...interface{}) error {
	return s.update(tx, data, where, nil, args...)
}

// Update executes UPDATE statement which updates data in DB and returns values if it needs.
func (s *Schema) UpdateRet(data interface{}, where string, ret *database.ReturningDest, args ...interface{}) error {
	return s.update(nil, data, where, ret, args...)
}

// Update executes UPDATE statement which updates data in DB and returns values if it needs. Supports transaction.
func (s *Schema) TransactUpdateRet(tx *sqlx.Tx, data interface{}, where string, ret *database.ReturningDest, args ...interface{}) error {
	return s.update(tx, data, where, ret, args...)
}
