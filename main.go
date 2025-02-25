package main

import (
	"encoding/json"
	"log"
	"os"

	_ "github.com/lib/pq" // Import the PostgreSQL driver
)

func saveFile(filename string, data []byte) error {
	return os.WriteFile(filename, data, 0644)
}

func saveMetadata(metadata SchemaDetails) {
	b, err := json.Marshal(metadata)
	if err != nil {
		log.Fatalf("failed to marshal metadata: %v", err)
	}

	err = saveFile("metadata.json", b)
	if err != nil {
		log.Fatalf("failed to save metadata: %v", err)
	}
}

func main() {
	db, err := connectToDB()
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	metadata, err := fetchMetadata(db)
	if err != nil {
		log.Fatalf("failed to build metadata: %v", err)
	}

	saveMetadata(metadata)

	for _, table := range metadata.Tables {
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
