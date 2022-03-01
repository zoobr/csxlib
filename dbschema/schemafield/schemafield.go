package schemafield

// SchemaField is a struct used to store information about a model field
type SchemaField struct {
	Name         string // field name (from model)
	Migratable   bool   // whether this field allowed to migrate (`db:"db_column,nomigrate"`)
	DBName       string // name of database column (golang tag `db`)
	DBType       string // type of database column (golang tag `type`)
	Nullable     bool   // whether column is NULL or NOT NULL (determined by the presence of a pointer to the model field)
	IsPrimaryKey bool   // whether column is primary key (golang tag `key`, value "pk")
	Length       int    // length of column type (golang tag `len`)
	Default      string // default column value (golang tag `def`)
	Comment      string // column comment (golang tag `comment`)
}

// IsFieldExistsByDBName checks if a field with the given db name exists
func IsFieldExistsByDBName(fields []*SchemaField, dbName string) bool {
	for _, f := range fields {
		if f.DBName == dbName {
			return true
		}
	}
	return false
}
