package dbschema

import (
	"sync"

	pkgerrs "github.com/pkg/errors"
)

// databaseManager is a struct for schema manager which stores Schema instances
type schemaManager struct {
	sync.RWMutex
	schemas map[string]*Schema // list of registered schemas
}

// Register registers schema instance in schema manager. It returns an error if it occurs.
func (sm *schemaManager) Register(schema *Schema) error {
	if len(schema.Name) == 0 {
		return pkgerrs.New("empty schema name")
	}

	sm.Lock()
	defer sm.Unlock()

	if sm.schemas == nil {
		sm.schemas = make(map[string]*Schema)
	}

	if _, ok := sm.schemas[schema.Name]; ok {
		return pkgerrs.Errorf("schema %s is already registered", schema.Name)
	}
	sm.schemas[schema.Name] = schema

	return nil
}

// MustRegister registers list of schemas in schema manager. It panics if error occurs.
func (sm *schemaManager) MustRegister(schemas ...*Schema) {
	for _, s := range schemas {
		if err := sm.Register(s); err != nil {
			panic(err)
		}
	}
}
