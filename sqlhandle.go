package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"
)

func generateSQL(dbName string, overwrite bool) (*sql.DB, error) {

	_, err := os.Open(dbName)

	if !overwrite && err == nil {
		fmt.Printf("Overwriting %s proceed? (y/n) ", dbName)
		input := bufio.NewScanner(os.Stdin)
		input.Scan()
		switch strings.ToLower(input.Text()) {
		case "y", "yes":
		default:
			return nil, fmt.Errorf("database overwrite cancelled, aborting program")
		}
	}

	// @TODO decide to keep this functionality
	os.Remove(dbName) // generate a clean file each time
	// SQLite is a file based database.

	file, err := os.Create(dbName) // Create SQLite file
	if err != nil {
		return nil, err
	}
	file.Close()

	sqliteDatabase, err := sql.Open("sqlite3", dbName) // Open the created SQLite File
	if err != nil {
		return nil, err
	}

	err = createTableNew(sqliteDatabase)
	if err != nil {
		return nil, err
	}

	return sqliteDatabase, nil
}

func createTableNew(db *sql.DB) error {
	createMetadataTableSQL := `CREATE TABLE metadata (
		"idFrame" integer NOT NULL PRIMARY KEY AUTOINCREMENT,
		"frameId" INTEGER,
		"segment" INTEGER,
		"groupTag" TEXT, 
		"tracksTag" TEXT,		
		"metadataTag" TEXT,
		"source" TEXT	
	  );` // SQL Statement for Create Table
	//"Extra" BLOB

	statement, err := db.Prepare(createMetadataTableSQL) // Prepare SQL Statement
	if err != nil {
		return err
	}
	_, err = statement.Exec() // Execute SQL Statements

	return err
}

// insert MetaData inserts a single row of metaData
func insertMetaData(db *sql.DB, groupTag, tracksTag, metadataTag string, frameID int, source string) error { //Student) {
	// log.Println("Inserting student record ...")
	insertStudentSQL := `INSERT INTO metadata(frameId, groupTag, tracksTag, metadataTag, source) VALUES (?, ?, ?, ?, ?)`
	statement, err := db.Prepare(insertStudentSQL) // Prepare statement.
	// This is to avoid SQL injections
	if err != nil {
		return err
	}
	_, err = statement.Exec(frameID, groupTag, tracksTag, metadataTag, source)

	return err
}

// insertBulkMetadata lets the user declare several rows worth of inputs and arguments to insert into the
// sql database
func insertBulkMetaData(db *sql.DB, insertStudentSQL string, fields []any) error { //Student) {
	// log.Println("Inserting student record ...")
	//insertStudentSQL := `INSERT INTO metadata(frameId, groupTag, tracksTag, metadataTag, source) VALUES (?, ?, ?, ?, ?)`
	statement, err := db.Prepare(insertStudentSQL) // Prepare statement.
	// This is good to avoid SQL injections
	if err != nil {
		return err
	}

	_, err = statement.Exec(fields...) //frameID, groupTag, tracksTag, metadataTag, source)

	return err

}
