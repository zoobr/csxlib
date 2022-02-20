package dbschema

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/zoobr/csxlib/dbschema/database"
	"github.com/zoobr/csxlib/dbschema/schemafield"

	pkgerrs "github.com/pkg/errors"
)

// prepareSchemaFields creates list of schema fields by model instance
func prepareSchemaFields(model interface{}) ([]*schemafield.SchemaField, error) {
	modelType := reflect.TypeOf(model)
	if modelType.Kind() != reflect.Struct {
		return nil, pkgerrs.Errorf("%s is not struct", modelType.Name())
	}

	fcnt := modelType.NumField()
	fields := make([]*schemafield.SchemaField, fcnt)
	for i := 0; i < fcnt; i++ {
		f := modelType.Field(i)
		dbName := f.Tag.Get("db") // field name in DB
		if len(dbName) == 0 || dbName == "-" {
			continue
		}
		dbType := f.Tag.Get("type")
		if len(dbType) == 0 {
			return nil, pkgerrs.Errorf("type for field '%s' (%s) is missing", dbName, f.Name)
		}

		fKind := f.Type.Kind()
		length, _ := strconv.Atoi(f.Tag.Get("len"))
		field := schemafield.SchemaField{
			Name:     f.Name,
			DBName:   dbName,
			DBType:   dbType,
			Nullable: fKind == reflect.Ptr || fKind == reflect.Map,
			Length:   length,
			Default:  f.Tag.Get("def"),
			Comment:  f.Tag.Get("comment"),
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

		fields[i] = &field
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

// Init initializes dbschema
func Init() {
	// connecting to all registered databases
	for _, db := range database.GetAll() {
		err := db.Connect()
		if err != nil {
			panic(err)
		}
	}
}
