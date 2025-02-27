package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/lib/pq" // Import the PostgreSQL driver
	"github.com/sbinet/npyio"
)

const BATCHSIZE = 100000 // Reduced batch size for streaming

// RowIterator represents an iterator over table rows
type RowIterator struct {
	db           *sql.DB
	tableName    string
	columns      []string
	metadata     map[string]FieldMetadata
	offset       int
	query        string
	currentRows  *sql.Rows
	currentBatch []TableRow
	batchIndex   int
	err          error
	done         bool
}

// NewRowIterator creates a new iterator for the given table
func NewRowIterator(db *sql.DB, table TableMetadata) (*RowIterator, error) {
	var columnNames []string
	metaMap := make(map[string]FieldMetadata)

	for _, field := range table.Fields {
		columnNames = append(columnNames, field.FieldName)
		metaMap[field.FieldName] = field
	}

	columnsStr := strings.Join(columnNames, ", ")
	query := fmt.Sprintf("SELECT %s FROM %s LIMIT %d OFFSET %%d", columnsStr, table.TableName, BATCHSIZE)

	return &RowIterator{
		db:          db,
		tableName:   table.TableName,
		columns:     columnNames,
		metadata:    metaMap,
		offset:      0,
		query:       query,
		currentRows: nil,
		batchIndex:  0,
		err:         nil,
		done:        false,
	}, nil
}

// Next fetches the next row. Returns false when there are no more rows or an error occurs.
func (ri *RowIterator) Next() (TableRow, bool) {
	// If we've reached the end or encountered an error
	if ri.done || ri.err != nil {
		return nil, false
	}

	// If we need to fetch a new batch
	if ri.currentRows == nil || ri.batchIndex >= len(ri.currentBatch) {
		if ri.currentRows != nil {
			ri.currentRows.Close()
		}

		// Fetch the next batch
		query := fmt.Sprintf(ri.query, ri.offset)
		rows, err := ri.db.Query(query)
		if err != nil {
			ri.err = err
			ri.done = true
			return nil, false
		}

		ri.currentRows = rows
		ri.currentBatch = []TableRow{}
		ri.batchIndex = 0

		// Process the batch
		cols, err := rows.Columns()
		if err != nil {
			ri.err = err
			ri.done = true
			rows.Close()
			return nil, false
		}

		batchCount := 0
		for rows.Next() {
			values := make([]interface{}, len(cols))
			valuePtrs := make([]interface{}, len(cols))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				ri.err = err
				ri.done = true
				rows.Close()
				return nil, false
			}

			rowMap := make(TableRow)
			for i, colName := range cols {
				if meta, ok := ri.metadata[colName]; ok {
					rowMap[colName] = convertValue(values[i], meta.DataType)
				} else {
					rowMap[colName] = values[i]
				}
			}

			ri.currentBatch = append(ri.currentBatch, rowMap)
			batchCount++
		}

		// Check if this was the last batch
		if batchCount == 0 {
			ri.done = true
			rows.Close()
			return nil, false
		}

		ri.offset += batchCount
	}

	// Return the current row and advance the index
	row := ri.currentBatch[ri.batchIndex]
	ri.batchIndex++
	return row, true
}

// Error returns any error that occurred during iteration
func (ri *RowIterator) Error() error {
	return ri.err
}

// Close cleans up resources used by the iterator
func (ri *RowIterator) Close() {
	if ri.currentRows != nil {
		ri.currentRows.Close()
		ri.currentRows = nil
	}
}

// Rest of your types remain the same
type FieldMetadata struct {
	FieldName           string   `json:"field_name"`
	DataType            string   `json:"data_type"`
	IsPrimaryKey        bool     `json:"is_primary_key"`
	IsForeignKey        bool     `json:"is_foreign_key"`
	IsNullable          bool     `json:"nullable"`
	ReferencedTable     *string  `json:"referenced_table_or_collection"`
	ReferencedField     *string  `json:"referenced_field"`
	TransformedFeatures []string `json:"transformed_features"`
}

type TableMetadata struct {
	TableName string          `json:"table_or_collection_name"`
	Fields    []FieldMetadata `json:"fields"`
}

// Define a row as a map where keys are field names and values are the row's data.
type TableRow map[string]interface{}

// TableData contains the table name, column metadata, and rows.
type TableData struct {
	TableName string
	Columns   []FieldMetadata
	Rows      []TableRow
}

// convertValue remains the same
func convertValue(rawValue interface{}, _ string) interface{} {
	return rawValue
}

// Data type constants remain the same
const (
	DataTypeString = "string"
	DataTypeInt    = "int"
	DataTypeFloat  = "float"
	DataTypeBool   = "bool"
	DataTypeTime   = "timestamp"
	DataTypeDate   = "date"
	DataTypeUUID   = "uuid"
	DataTypeNull   = "null"
)

// mapDataType remains the same
func mapDataType(pgType string) string {
	switch pgType {
	case "character varying", "text", "varchar":
		return DataTypeString
	case "integer", "bigint", "smallint":
		return DataTypeInt
	case "numeric", "decimal", "real", "double precision":
		return DataTypeFloat
	case "boolean":
		return DataTypeBool
	case "timestamp without time zone", "timestamp with time zone",
		"time without time zone", "time with time zone":
		return DataTypeTime
	case "date":
		return DataTypeDate
	case "uuid":
		return DataTypeUUID
	default:
		return DataTypeString
	}
}

// createRowIterator - a helper function to create a row iterator
func createRowIterator(db *sql.DB, table TableMetadata) (*RowIterator, error) {
	return NewRowIterator(db, table)
}

// Rest of your types remain the same
type DatasetMetadata struct {
	DatasetName   string                 `json:"dataset_name"`
	SourceType    string                 `json:"source_type"`
	SourceDetails map[string]interface{} `json:"source_details"`
}

type SchemaDetails struct {
	DatasetMetadata DatasetMetadata `json:"dataset_metadata"`
	Tables          []TableMetadata `json:"schema"`
}

// connectToDB remains the same
func connectToDB() (*sql.DB, error) {
	dsn := "user=postgres dbname=centrum_db_dev password=postgres host=localhost sslmode=disable"
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

// fetchMetadata remains the same
func fetchMetadata(db *sql.DB, dbName string, tableNames []string) (SchemaDetails, error) {
	// Implementation remains the same
	// ... (keeping the same implementation)
	var schema SchemaDetails

	// Query to get all user tables in the public schema.
	tablesQuery := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
		  AND table_type = 'BASE TABLE'
	`
	rows, err := db.Query(tablesQuery)
	if err != nil {
		return schema, fmt.Errorf("querying tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return schema, fmt.Errorf("scanning table name: %w", err)
		}

		tableNotAskedFor := true
		for _, t := range tableNames {
			if t == tableName {
				tableNotAskedFor = false
				break
			}
		}

		if tableNotAskedFor {
			continue
		}

		// Create a new TableMetadata for the current table.
		tableMeta := TableMetadata{TableName: tableName}

		// Query column details for the current table.
		columnsQuery := `
			SELECT column_name, data_type, is_nullable
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = $1
			ORDER BY ordinal_position
		`
		colRows, err := db.Query(columnsQuery, tableName)
		if err != nil {
			return schema, fmt.Errorf("querying columns for table %s: %w", tableName, err)
		}
		var fields []FieldMetadata
		for colRows.Next() {
			var colName, dataType, isNullableStr string
			if err := colRows.Scan(&colName, &dataType, &isNullableStr); err != nil {
				colRows.Close()
				return schema, fmt.Errorf("scanning column for table %s: %w", tableName, err)
			}
			dataType = mapDataType(dataType)
			isNullable := (isNullableStr == "YES")
			field := FieldMetadata{
				FieldName:  colName,
				DataType:   dataType,
				IsNullable: isNullable,
			}
			fields = append(fields, field)
		}
		colRows.Close()

		// Query primary key columns for the table.
		pkQuery := `
			SELECT kcu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu 
			  ON tc.constraint_name = kcu.constraint_name
			WHERE tc.constraint_type = 'PRIMARY KEY'
			  AND tc.table_name = $1
		`
		pkRows, err := db.Query(pkQuery, tableName)
		if err != nil {
			return schema, fmt.Errorf("querying primary keys for table %s: %w", tableName, err)
		}
		pkMap := make(map[string]bool)
		for pkRows.Next() {
			var pkColumn string
			if err := pkRows.Scan(&pkColumn); err != nil {
				pkRows.Close()
				return schema, fmt.Errorf("scanning primary key for table %s: %w", tableName, err)
			}
			pkMap[pkColumn] = true
		}
		pkRows.Close()

		// Mark columns that are primary keys.
		for i, field := range fields {
			if pkMap[field.FieldName] {
				fields[i].IsPrimaryKey = true
			}
		}

		// Query foreign key details for the table.
		fkQuery := `
			SELECT kcu.column_name, 
			       ccu.table_name AS foreign_table, 
			       ccu.column_name AS foreign_column
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu 
			  ON tc.constraint_name = kcu.constraint_name
			JOIN information_schema.constraint_column_usage ccu 
			  ON ccu.constraint_name = tc.constraint_name
			WHERE tc.constraint_type = 'FOREIGN KEY'
			  AND tc.table_name = $1
		`
		fkRows, err := db.Query(fkQuery, tableName)
		if err != nil {
			return schema, fmt.Errorf("querying foreign keys for table %s: %w", tableName, err)
		}
		fkMap := make(map[string]struct {
			foreignTable  string
			foreignColumn string
		})
		for fkRows.Next() {
			var colName, foreignTable, foreignColumn string
			if err := fkRows.Scan(&colName, &foreignTable, &foreignColumn); err != nil {
				fkRows.Close()
				return schema, fmt.Errorf("scanning foreign key for table %s: %w", tableName, err)
			}
			fkMap[colName] = struct {
				foreignTable  string
				foreignColumn string
			}{foreignTable: foreignTable, foreignColumn: foreignColumn}
		}
		fkRows.Close()

		// Mark columns that are foreign keys and add referenced table/column.
		for i, field := range fields {
			if fk, ok := fkMap[field.FieldName]; ok {
				fields[i].IsForeignKey = true
				fields[i].ReferencedTable = &fk.foreignTable
				fields[i].ReferencedField = &fk.foreignColumn
			}
		}

		tableMeta.Fields = fields
		schema.Tables = append(schema.Tables, tableMeta)
	}

	if err := rows.Err(); err != nil {
		return schema, fmt.Errorf("processing tables: %w", err)
	}

	for tableIdx, table := range schema.Tables {
		for fieldIdx, field := range table.Fields {
			if field.IsForeignKey && field.ReferencedTable != nil {
				found := false
				// Check if the referenced table is among the selected tables.
				for _, tableName := range tableNames {
					if *field.ReferencedTable == tableName {
						found = true
						break
					}
				}

				// If not found, update the field metadata.
				if !found {
					schema.Tables[tableIdx].Fields[fieldIdx].ReferencedTable = nil
					schema.Tables[tableIdx].Fields[fieldIdx].IsForeignKey = false
					schema.Tables[tableIdx].Fields[fieldIdx].ReferencedField = nil
				}
			}
		}
	}

	schema.DatasetMetadata = DatasetMetadata{
		DatasetName: dbName,
		SourceType:  "Relational Database",
		SourceDetails: map[string]interface{}{
			"database_type":         "PostgreSQL",
			"tables_or_collections": tableNames,
		},
	}

	return schema, nil
}

// saveFile remains the same
func saveFile(filename string, data []byte) error {
	return os.WriteFile(filename, data, 0644)
}

// saveMetadata remains the same
func saveMetadata(metadata SchemaDetails) {
	b, err := json.Marshal(metadata)
	if err != nil {
		log.Fatalf("failed to marshal metadata: %v", err)
	}

	err = saveFile(filepath.Join("data", "metadata.json"), b)
	if err != nil {
		log.Fatalf("failed to save metadata: %v", err)
	}
}

// Modified version of saveTableToNumpy that correctly handles streaming
func saveTableToNumpy(db *sql.DB, table TableMetadata) error {
	log.Printf("Starting export of table %s...", table.TableName)

	// Create a directory for this table's NPY files
	tableDir := filepath.Join("data", table.TableName)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory for table %s: %v", table.TableName, err)
	}

	// Create a row iterator
	rowIterator, err := createRowIterator(db, table)
	if err != nil {
		return fmt.Errorf("failed to create row iterator for table %s: %v", table.TableName, err)
	}
	defer rowIterator.Close()

	// Count total rows first (optional but useful for pre-allocation)
	var totalRows int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", table.TableName)
	if err := db.QueryRow(countQuery).Scan(&totalRows); err != nil {
		log.Printf("Could not determine total row count: %v", err)
		// Continue without knowing total row count
		totalRows = 0
	} else {
		log.Printf("Table %s has approximately %d rows", table.TableName, totalRows)
	}

	// We'll collect all data in memory for each column, then write a single NPY file
	// This avoids multiple headers issue but uses more memory
	columnData := make(map[string]interface{})
	for _, col := range table.Fields {
		switch col.DataType {
		case DataTypeInt:
			columnData[col.FieldName] = make([]int64, 0, totalRows)
		case DataTypeFloat:
			columnData[col.FieldName] = make([]float64, 0, totalRows)
		case DataTypeString, DataTypeDate, DataTypeTime, DataTypeUUID, DataTypeNull:
			columnData[col.FieldName] = make([]string, 0, totalRows)
		case DataTypeBool:
			columnData[col.FieldName] = make([]bool, 0, totalRows)
		default:
			columnData[col.FieldName] = make([]string, 0, totalRows)
		}
	}

	// Stream through the rows
	rowCount := 0
	reportInterval := BATCHSIZE
	nextReport := reportInterval

	for row, ok := rowIterator.Next(); ok; row, ok = rowIterator.Next() {
		// Add each column value to the appropriate array
		for _, col := range table.Fields {
			value := row[col.FieldName]

			switch col.DataType {
			case DataTypeInt:
				array := columnData[col.FieldName].([]int64)
				if value == nil {
					array = append(array, 0)
				} else {
					switch v := value.(type) {
					case int64:
						array = append(array, v)
					case int:
						array = append(array, int64(v))
					case float64:
						array = append(array, int64(v))
					default:
						array = append(array, 0)
						log.Printf("unexpected type for column %s", col.FieldName)
					}
				}
				columnData[col.FieldName] = array

			case DataTypeFloat:
				array := columnData[col.FieldName].([]float64)
				if value == nil {
					array = append(array, 0.0)
				} else {
					switch v := value.(type) {
					case float64:
						array = append(array, v)
					case float32:
						array = append(array, float64(v))
					case int:
						array = append(array, float64(v))
					default:
						array = append(array, 0.0)
						log.Printf("unexpected type for column %s", col.FieldName)
					}
				}
				columnData[col.FieldName] = array

			case DataTypeString, DataTypeDate, DataTypeTime, DataTypeUUID, DataTypeNull:
				array := columnData[col.FieldName].([]string)
				if value == nil {
					array = append(array, "")
				} else {
					switch v := value.(type) {
					case string:
						array = append(array, v)
					case time.Time:
						array = append(array, v.Format(time.RFC3339))
					default:
						array = append(array, fmt.Sprintf("%v", value))
					}
				}
				columnData[col.FieldName] = array

			case DataTypeBool:
				array := columnData[col.FieldName].([]bool)
				if value == nil {
					array = append(array, false)
				} else {
					if v, ok := value.(bool); ok {
						array = append(array, v)
					} else {
						array = append(array, false)
						log.Printf("unexpected type for column %s", col.FieldName)
					}
				}
				columnData[col.FieldName] = array

			default:
				array := columnData[col.FieldName].([]string)
				if value == nil {
					array = append(array, "")
				} else {
					array = append(array, fmt.Sprintf("%v", value))
				}
				columnData[col.FieldName] = array
			}
		}

		rowCount++

		// Report progress periodically
		if rowCount >= nextReport {
			log.Printf("Processed %d rows for table %s...", rowCount, table.TableName)
			nextReport += reportInterval
		}
	}

	// Check for iterator errors
	if err := rowIterator.Error(); err != nil {
		return fmt.Errorf("error during iteration: %v", err)
	}

	// Now write each column to its own NPY file (only once)
	for _, col := range table.Fields {
		fileName := filepath.Join(tableDir, fmt.Sprintf("%s.npy", col.FieldName))

		// Open a file for writing
		f, err := os.Create(fileName)
		if err != nil {
			return fmt.Errorf("failed to create file for column %s: %v", col.FieldName, err)
		}

		// Use defer with a closure to handle errors properly
		defer func() {
			closeErr := f.Close()
			if err == nil && closeErr != nil {
				err = closeErr
			}
		}()

		switch col.DataType {
		case DataTypeInt:
			array := columnData[col.FieldName].([]int64)
			if err := npyio.Write(f, array); err != nil {
				return fmt.Errorf("failed to write NPY file for column %s: %v", col.FieldName, err)
			}
		case DataTypeFloat:
			array := columnData[col.FieldName].([]float64)
			if err := npyio.Write(f, array); err != nil {
				return fmt.Errorf("failed to write NPY file for column %s: %v", col.FieldName, err)
			}
		case DataTypeString, DataTypeDate, DataTypeTime, DataTypeUUID, DataTypeNull:
			array := columnData[col.FieldName].([]string)
			if err := npyio.Write(f, array); err != nil {
				return fmt.Errorf("failed to write NPY file for column %s: %v", col.FieldName, err)
			}
		case DataTypeBool:
			array := columnData[col.FieldName].([]bool)
			if err := npyio.Write(f, array); err != nil {
				return fmt.Errorf("failed to write NPY file for column %s: %v", col.FieldName, err)
			}
		default:
			array := columnData[col.FieldName].([]string)
			if err := npyio.Write(f, array); err != nil {
				return fmt.Errorf("failed to write NPY file for column %s: %v", col.FieldName, err)
			}
		}
	}

	log.Printf("Table %q data saved successfully to directory %s (%d rows total)", table.TableName, tableDir, rowCount)
	return nil
}

func main() {
	// Ensure data directory exists
	if err := os.MkdirAll("data", 0755); err != nil {
		log.Fatalf("failed to create data directory: %v", err)
	}

	// Connect to database
	db, err := connectToDB()
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	// Define which tables to export
	selectedTables := []string{"users", "user_sessions", "tools"}

	// Fetch metadata about the tables
	metadata, err := fetchMetadata(db, "centrum_db_dev", selectedTables)
	if err != nil {
		log.Fatalf("failed to build metadata: %v", err)
	}

	// Save metadata to file
	saveMetadata(metadata)

	// Process each table
	for _, table := range metadata.Tables {
		// Check if this table was requested
		tableRequested := false
		for _, t := range selectedTables {
			if t == table.TableName {
				tableRequested = true
				break
			}
		}

		if !tableRequested {
			continue
		}

		// Stream table data to NPY files
		if err := saveTableToNumpy(db, table); err != nil {
			log.Fatalf("failed to save table data for %s: %v", table.TableName, err)
		}
	}

	log.Println("Data export complete.")
}
