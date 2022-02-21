package database

import (
	pkgerrs "github.com/pkg/errors"
)

// databaseManager is a struct for database manager which stores Database instances
type databaseManager struct {
	databases map[string]Database // list of registered databases
}

var manager = databaseManager{} // instance of database manager

// Register registers Database instance in database manager. It returns an error if it occurs.
func (dm *databaseManager) Register(db Database) error {
	if dm.databases == nil {
		dm.databases = make(map[string]Database)
	}

	params := db.GetParams()
	if params == nil {
		return pkgerrs.New("empty database params")
	}
	if params.Name == "" {
		return pkgerrs.New("empty database name")
	}
	if _, ok := dm.databases[params.Name]; ok {
		return pkgerrs.Errorf("database %s is already registered", params.Name)
	}
	dm.databases[params.Name] = db

	return nil
}

// MustRegister registers list of Database instances in database manager. It panics if error occurs.
func (dm *databaseManager) MustRegister(dbs ...Database) {
	for _, db := range dbs {
		if err := dm.Register(db); err != nil {
			panic(err)
		}
	}
}

// Get returns instance of Database by database name
func Get(name string) Database {
	db, ok := manager.databases[name]
	if !ok {
		return nil
	}
	return db
}

// GetAll returns list of all registered databases
func GetAll() map[string]Database {
	return manager.databases
}
