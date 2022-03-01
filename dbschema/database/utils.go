package database

import (
	"reflect"
	"strings"

	"github.com/zoobr/csxlib/dbschema/schemafield"

	pkgerrs "github.com/pkg/errors"
)

type PreparedData struct {
	DBFields []string
	Values   []interface{}
	Queries  map[string]*Query // for UPDATE
	Query    *Query            // for INSERT
}

// prepareValsMap prepares values for INSERT or UPDATE statements from map.
func prepareValsMap(data map[string]interface{}, fields []*schemafield.SchemaField) *PreparedData {
	cntf := len(fields)
	prepared := PreparedData{
		DBFields: make([]string, 0, cntf),
		Values:   make([]interface{}, 0, cntf),
		Queries:  make(map[string]*Query),
	}

	for _, f := range fields {
		var (
			d  interface{}
			ok bool
		)

		d, ok = data[f.DBName] // by database column name
		if !ok {
			d, ok = data[f.Name] // by struct field name
		}
		if !ok {
			continue
		}

		// if value is SELECT query
		if dq, okq := d.(*Query); okq {
			prepared.Queries[f.DBName] = dq
			continue
		}

		// otherwise store field & value
		prepared.DBFields = append(prepared.DBFields, f.DBName)
		prepared.Values = append(prepared.Values, d)
	}

	return &prepared
}

// prepareValsMap prepares values for INSERT or UPDATE statements from struct.
// NOTE: it returns only non-zery values of struct. Also unsupports *database.Query as struct field type.
func prepareValsStruct(dataValue reflect.Value, dataType reflect.Type, fields []*schemafield.SchemaField) *PreparedData {
	cntf := len(fields)
	prepared := PreparedData{
		DBFields: make([]string, 0, cntf),
		Values:   make([]interface{}, 0, cntf),
	}

	cntd := dataType.NumField()
	for i := 0; i < cntd; i++ {
		f := dataType.Field(i)
		dbName := f.Tag.Get("db")
		if len(dbName) == 0 || dbName == "-" {
			continue
		}
		dbName = strings.Split(dbName, ",")[0]
		if !schemafield.IsFieldExistsByDBName(fields, dbName) {
			continue
		}

		value := dataValue.FieldByName(f.Name)
		if value.IsZero() {
			continue
		}

		if value.IsValid() {
			prepared.DBFields = append(prepared.DBFields, dbName)
			prepared.Values = append(prepared.Values, value.Interface())
		}

	}

	return &prepared
}

// PrepareData prepares values for INSERT or UPDATE statements from different types.
// Supported types: database.Query, *database.Query, map[string]interface{}, struct
// NOTE: it returns only non-zery values if data is struct.
func PrepareData(data interface{}, fields []*schemafield.SchemaField) (*PreparedData, error) {
	switch d := data.(type) {
	case Query:
		return &PreparedData{Query: &d}, nil
	case *Query:
		return &PreparedData{Query: d}, nil
	case map[string]interface{}:
		return prepareValsMap(d, fields), nil
	default:
		dataValue := reflect.ValueOf(d)
		dataType := dataValue.Type()

		if dataType.Kind() == reflect.Ptr {
			dataValue = reflect.Indirect(dataValue)
			dataType = dataValue.Type()
		}
		if dataType.Kind() != reflect.Struct {
			return nil, pkgerrs.Errorf("wrong data type: need struct, database.Query or map[string]interface{}, got %s", dataType.Name())
		}

		return prepareValsStruct(dataValue, dataType, fields), nil
	}
}
