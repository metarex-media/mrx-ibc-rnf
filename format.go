package main

import (
	"database/sql"
	"fmt"
)

// sqlInserter groups the metadata into the sql database
func sqlInserter(db *sql.DB, fields []sqlFormat, source, empty string) error {

	// every frame is tagged if it is written to
	// @TODO find a more elegant solution
	frames := make(map[int]bool)
	highest := 0
	for seg, event := range fields {
		if event.end > highest {
			highest = event.end
		}
		// Build a bulk statement for speed inserting instead of individual statements
		stmnt := `INSERT INTO metadata(frameId, segment, groupTag, tracksTag, metadataTag, source) VALUES` // (?, ?, ?, ?, ?)
		var stFields []any
		for i := event.start; i <= event.end; i++ {
			frames[i] = true
			// fmt.Println(i, key, event.Metadata[0].Id)
			if i == event.start {
				stmnt += "(?, ?, ?, ?, ?, ?)"
			} else {
				stmnt += ",(?, ?, ?, ?, ?, ?)"
			}
			stFields = append(stFields, i, seg, event.groupTag, event.tracksTag, event.metadataTag, source)

		}

		// @TODO implement a max input fields tos top errors
		// 1000 is about the max count
		err := insertBulkMetaData(db, stmnt, stFields)
		if err != nil {
			return fmt.Errorf("%v %v", err, event)
		}
	}

	// find the untagged frames
	stmnt := `INSERT INTO metadata(frameId, segment, groupTag, tracksTag, metadataTag, source) VALUES` // (?, ?, ?, ?, ?)
	var stFields []any
	// @TODO change the start from 0 to 1 as frames start at 1?
	for i := 1; i <= highest; i++ {
		if !frames[i] {

			if len(stFields) == 0 {

				stmnt += "(?, ?, ?, ?, ?, ?)"
			} else {
				stmnt += ",(?, ?, ?, ?, ?, ?)"
			}

			stFields = append(stFields, i, -1, "", "", empty, source)

		}

	}

	if len(stFields) != 0 {
		err := insertBulkMetaData(db, stmnt, stFields)
		if err != nil {
			return err
		}
	}

	return nil
}
