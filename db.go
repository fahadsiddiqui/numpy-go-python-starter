package main

import (
	"database/sql"
	"fmt"
	"strings"
)

const BATCHSIZE = 10000

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

// Define a row as a map where keys are field names and values are the row’s data.
type TableRow map[string]interface{}

// TableData contains the table name, column metadata, and rows.
type TableData struct {
	TableName string
	Columns   []FieldMetadata
	Rows      []TableRow
}

// Optionally, define a helper to convert raw values using the metadata.
// This function can be extended to handle different data types appropriately.
func convertValue(rawValue interface{}, _ string) interface{} {
	// For example, if the expected data type is "int" but the rawValue is []byte,
	// you can convert it accordingly. For now, we return the raw value.
	// You can add cases for "string", "float", "bool", etc.
	return rawValue
}

const (
	// Define constants for our internal data types.
	DataTypeString = "string"
	DataTypeInt    = "int"
	DataTypeFloat  = "float"
	DataTypeBool   = "bool"
	DataTypeTime   = "timestamp"
	DataTypeDate   = "date"
	DataTypeUUID   = "uuid"
	DataTypeNull   = "null"
)

// mapDataType converts PostgreSQL types to our standardized types.
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
		// Fallback to string if unknown; alternatively, return pgType.
		return DataTypeString
	}
}

func FetchTableData(db *sql.DB, table TableMetadata) (*TableData, error) {
	offset := 0
	tableData := &TableData{
		TableName: table.TableName,
		Columns:   table.Fields,
		Rows:      []TableRow{},
	}

	// Build a slice of column names from the metadata.
	var filterColumns []string
	for _, field := range table.Fields {
		filterColumns = append(filterColumns, field.FieldName)
	}

	for {
		columnsStr := strings.Join(filterColumns, ", ")
		query := fmt.Sprintf("SELECT %s FROM %s LIMIT %d OFFSET %d", columnsStr, table.TableName, BATCHSIZE, offset)
		rows, err := db.Query(query)
		if err != nil {
			return nil, err
		}

		// Get column names from the query result.
		cols, err := rows.Columns()
		if err != nil {
			rows.Close()
			return nil, err
		}

		// Prepare a mapping of column names to their corresponding metadata for conversion.
		metaMap := make(map[string]FieldMetadata)
		for _, field := range table.Fields {
			metaMap[field.FieldName] = field
		}

		batchCount := 0
		for rows.Next() {
			// Create a slice to hold column values.
			values := make([]interface{}, len(cols))
			valuePtrs := make([]interface{}, len(cols))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				rows.Close()
				return nil, err
			}

			// Create a row map and use metadata for type conversion if needed.
			rowMap := make(TableRow)
			for i, colName := range cols {
				if meta, ok := metaMap[colName]; ok {
					rowMap[colName] = convertValue(values[i], meta.DataType)
				} else {
					rowMap[colName] = values[i]
				}
			}

			tableData.Rows = append(tableData.Rows, rowMap)
			batchCount++
		}
		rows.Close()

		// If no rows were returned in this batch, exit the loop.
		if batchCount == 0 {
			break
		}
		offset += BATCHSIZE
	}

	return tableData, nil
}

type DatasetMetadata struct {
	DatasetName   string                 `json:"dataset_name"`
	SourceType    string                 `json:"source_type"`
	SourceDetails map[string]interface{} `json:"source_details"`
}

type SchemaDetails struct {
	DatasetMetadata DatasetMetadata `json:"dataset_metadata"`
	Tables          []TableMetadata `json:"tables"`
}

// connectToDB connects to the PostgreSQL database.
func connectToDB() (*sql.DB, error) {
	dsn := "user=postgres dbname=centrum_db_dev password=postgres host=localhost sslmode=disable"
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	// Verify the connection.
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

// fetchMetadata fetches the schema details (tables, columns, primary keys, and foreign keys).
func fetchMetadata(db *sql.DB, dbName string, tableNames []string) (SchemaDetails, error) {
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
