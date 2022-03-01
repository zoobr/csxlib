package dbschema

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/zoobr/csxlib/dbschema/database"
	"github.com/zoobr/csxlib/dbschema/schemafield"

	pkgerrs "github.com/pkg/errors"
)

// Config is a struct for dbschema config
type Config struct {
	IsMigrateData bool // whether make data migrations
}

var (
	manager = schemaManager{} // instance of schema manager
	config  = Config{}        //instance of config
)

// getNewSchemaFields returns list of new table columns
func getNewSchemaFields(fields []*schemafield.SchemaField, colInfo []*database.DBColumnInfo) []*schemafield.SchemaField {
	fcnt := len(fields)
	icnt := len(colInfo)

	// no new cols
	if fcnt == icnt {
		return []*schemafield.SchemaField{}
	}

	newFields := make([]*schemafield.SchemaField, 0, fcnt)
	for i := 0; i < fcnt; i++ {
		f := fields[i]
		isExists := false
		for _, info := range colInfo {
			if info.Name == f.DBName {
				isExists = true
				break
			}
		}
		if !isExists {
			newFields = append(newFields, f)
		}
	}

	return newFields
}

// migrateSchema makes schema migration
func migrateSchema(schema *Schema) error {
	db := schema.dbs.master
	if !db.IsTableExists(schema.TableName) {
		// create table if it is not exists
		err := db.CreateTable(schema.TableName, schema.fields)
		if err != nil {
			return err
		}
	} else {
		// alter table if it is exists
		colInfo, err := db.GetColumnsInfo(schema.TableName)
		if err != nil {
			return err
		}
		newFields := getNewSchemaFields(schema.fields, colInfo)
		if len(newFields) > 0 {
			err := db.AlterTable(schema.TableName, newFields)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// prepareSchemaFields creates list of schema fields by model instance
func prepareSchemaFields(model interface{}) ([]*schemafield.SchemaField, error) {
	modelType := reflect.TypeOf(model)
	if modelType.Kind() != reflect.Struct {
		return nil, pkgerrs.Errorf("%s is not struct", modelType.Name())
	}

	fcnt := modelType.NumField()
	fields := make([]*schemafield.SchemaField, 0, fcnt)
	for i := 0; i < fcnt; i++ {
		f := modelType.Field(i)

		dbName := f.Tag.Get("db") // field name in DB
		if len(dbName) == 0 || dbName == "-" {
			continue
		}
		migratable := true
		dbNames := strings.Split(dbName, ",")
		if len(dbNames) == 2 {
			migratable = dbNames[1] != "nomigrate"
		}

		dbType := f.Tag.Get("type")
		if len(dbType) == 0 {
			return nil, pkgerrs.Errorf("type for field '%s' (%s) is missing", dbName, f.Name)
		}

		fKind := f.Type.Kind()
		length, _ := strconv.Atoi(f.Tag.Get("len"))
		field := schemafield.SchemaField{
			Name:       f.Name,
			Migratable: migratable,
			DBName:     dbNames[0],
			DBType:     dbType,
			Nullable:   fKind == reflect.Ptr || fKind == reflect.Map || fKind == reflect.Interface,
			Length:     length,
			Default:    f.Tag.Get("def"),
			Comment:    f.Tag.Get("comment"),
		}

		key := f.Tag.Get("key")
		if len(key) != 0 {
			keys := strings.Split(key, ",")
			if len(keys) != 0 {
				for _, v := range keys {
					switch v {
					case "pk":
						field.IsPrimaryKey = true
					}
				}
			}
		}

		if field.IsPrimaryKey && field.Nullable {
			return nil, pkgerrs.Errorf("primary key '%s' (%s) is nullable", dbName, f.Name)
		}

		fields = append(fields, &field)
	}

	return fields, nil
}

// NewSchema creates new schema by params
func NewSchema(params *SchemaParams) (*Schema, error) {
	fields, err := prepareSchemaFields(params.Model)
	if err != nil {
		return nil, err
	}

	schema := Schema{
		SchemaParams: *params,
		fields:       fields,
	}

	return &schema, nil
}

// New creates & registers list of Schema instances using list of params. It panics if error occurs.
func New(params *SchemaParams) *Schema {
	schema, err := NewSchema(params)
	if err != nil {
		panic(err)
	}
	manager.MustRegister(schema)

	return schema
}

// Init initializes dbschema
func Init(cfg *Config) {
	// storing config
	if cfg != nil {
		config = *cfg
	}

	// connecting to all registered databases
	for _, db := range database.GetAll() {
		err := db.Connect()
		if err != nil {
			panic(err)
		}
	}

	for _, schema := range manager.schemas {
		// adding databases to schema
		master := database.Get(schema.DatabaseName)
		if master == nil {
			panic(fmt.Errorf("database %s not found", schema.DatabaseName))
		}
		schema.dbs.master = master
		if len(schema.SlaveDatabaseName) > 0 {
			slave := database.Get(schema.SlaveDatabaseName)
			if slave == nil {
				panic(fmt.Errorf("database %s not found", schema.SlaveDatabaseName))
			}
			schema.dbs.slave = slave
		}

		// migrating schemas
		if err := migrateSchema(schema); err != nil {
			panic(err)
		}

		// migrating data
		if config.IsMigrateData {
			if err := schema.dbs.master.Migrate(); err != nil {
				panic(err)
			}
		}
	}
}
