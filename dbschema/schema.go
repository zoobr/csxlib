package dbschema

import (
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
	SlaveDatabaseName *string     // slave database name (is exists)
	TableName         string      // name of table in database
	Model             interface{} // instance of model
}

// Schema is a struct representing the schema of a table in database
type Schema struct {
	SchemaParams
	fields []*schemafield.SchemaField // list of database columns
	dbs    schemaDatabases            // list of schema databases
}
