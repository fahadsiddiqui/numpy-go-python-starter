package main

import (
	"log"

	_ "github.com/lib/pq" // Import the PostgreSQL driver
)

func main() {
	db, err := connectToDB()
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	schemaDetails, err := fetchMetadata(db)
	if err != nil {
		log.Fatalf("failed to fetch metadata: %v", err)
	}

	for _, table := range schemaDetails.Tables {
		tableData, err := FetchTableData(db, table)
		if err != nil {
			log.Fatalf("failed to fetch table data: %v", err)
		}

		saveTableToNumpy(*tableData)
	}

	if err != nil {
		log.Fatalf("failed to fetch table data: %v", err)
	}
}
