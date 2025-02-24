package main

import (
	"fmt"
	"log"
	"time"

	"github.com/sbinet/npyio/npz"
)

// saveTableToNumpy saves the table as an NPZ file.
// It builds a map[string]interface{} where each key is a column name
// and the value is a slice of that column's data.
func saveTableToNumpy(table TableData) {
	nrows := len(table.Rows)
	arrays := make(map[string]interface{})

	// Create a slice for each column based on its declared data type.
	for _, col := range table.Columns {
		switch col.DataType {
		case DataTypeInt:
			arrays[col.FieldName] = make([]int64, nrows)
		case DataTypeFloat:
			arrays[col.FieldName] = make([]float64, nrows)
		case DataTypeString, DataTypeDate:
			arrays[col.FieldName] = make([]string, nrows)
		case DataTypeBool:
			arrays[col.FieldName] = make([]bool, nrows)
		case DataTypeUUID:
			// Store UUID as a string.
			arrays[col.FieldName] = make([]string, nrows)
		case DataTypeTime:
			// Store time as a formatted string.
			arrays[col.FieldName] = make([]string, nrows)
		case DataTypeNull:
			// Instead of []interface{}, use []string for nil values.
			arrays[col.FieldName] = make([]string, nrows)
		default:
			arrays[col.FieldName] = make([]interface{}, nrows)
		}
	}

	// Populate each column slice with data.
	for r, row := range table.Rows {
		for _, col := range table.Columns {
			value := row[col.FieldName]
			switch col.DataType {
			case DataTypeInt:
				arr := arrays[col.FieldName].([]int64)
				if value == nil {
					arr[r] = 0
				} else {
					switch v := value.(type) {
					case int64:
						arr[r] = v
					case int:
						arr[r] = int64(v)
					case float64:
						arr[r] = int64(v)
					default:
						log.Printf("unexpected type for column %s", col.FieldName)
					}
				}
			case DataTypeFloat:
				arr := arrays[col.FieldName].([]float64)
				if value == nil {
					arr[r] = 0.0
				} else {
					switch v := value.(type) {
					case float64:
						arr[r] = v
					case float32:
						arr[r] = float64(v)
					case int:
						arr[r] = float64(v)
					default:
						log.Printf("unexpected type for column %s", col.FieldName)
					}
				}
			case DataTypeString, DataTypeDate:
				arr := arrays[col.FieldName].([]string)
				if value == nil {
					arr[r] = ""
				} else {
					if v, ok := value.(string); ok {
						arr[r] = v
					} else {
						arr[r] = fmt.Sprintf("%v", value)
					}
				}
			case DataTypeBool:
				arr := arrays[col.FieldName].([]bool)
				if value == nil {
					arr[r] = false
				} else {
					if v, ok := value.(bool); ok {
						arr[r] = v
					} else {
						log.Printf("unexpected type for column %s", col.FieldName)
					}
				}
			case DataTypeUUID:
				arr := arrays[col.FieldName].([]string)
				if value == nil {
					arr[r] = "null"
				} else {
					if v, ok := value.(string); ok {
						arr[r] = v
					} else {
						arr[r] = fmt.Sprintf("%v", value)
					}
				}
			case DataTypeTime:
				arr := arrays[col.FieldName].([]string)
				if value == nil {
					arr[r] = "null"
				} else {
					if v, ok := value.(time.Time); ok {
						arr[r] = v.Format(time.RFC3339)
					} else if v, ok := value.(string); ok {
						arr[r] = v
					} else {
						arr[r] = fmt.Sprintf("%v", value)
					}
				}
			case DataTypeNull:
				arr := arrays[col.FieldName].([]string)
				if value == nil {
					arr[r] = "null"
				} else {
					if s, ok := value.(string); ok {
						arr[r] = s
					} else {
						arr[r] = fmt.Sprintf("%v", value)
					}
				}
			default:
				arr := arrays[col.FieldName].([]interface{})
				arr[r] = value
			}
		}
	}

	// Write the NPZ archive using the filename.
	fileName := fmt.Sprintf("data/%s.npz", table.TableName)
	// Assuming npz.Write is defined; replace with your npz writing function.
	if err := npz.Write(fileName, arrays); err != nil {
		log.Fatalf("failed to write npz file: %v", err)
	}

	log.Printf("Table %q saved successfully to %s", table.TableName, fileName)
}
