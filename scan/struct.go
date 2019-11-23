package scan

import (
	"database/sql"
	"fmt"
	"reflect"
)

// StructSlice scans rows to dest, which must be a pointer to a slice of structs.
func StructSlice(rows *sql.Rows, dest interface{}) error {
	slicevalue := reflect.ValueOf(dest).Elem()
	structtype := slicevalue.Type().Elem()

	structfields := make([]reflect.StructField, 0, structtype.NumField())
	for i := 0; i < structtype.NumField(); i++ {
		structfields = append(structfields, structtype.Field(i))
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	var fieldindexes [][]int
	for _, col := range columns {
		var ok bool
		for _, field := range structfields {
			if col == field.Tag.Get("scan") {
				fieldindexes = append(fieldindexes, field.Index)
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("couldn't map column %s", col)
		}
	}

	// TODO: set slicevalue capacity, to reduce the number of allocations.
	for rows.Next() {
		scannedvalue := reflect.New(structtype).Elem()
		dests := make([]interface{}, 0, len(fieldindexes))
		for _, index := range fieldindexes {
			dests = append(dests, scannedvalue.FieldByIndex(index).Addr().Interface())
		}
		if err := rows.Scan(dests...); err != nil {
			return err
		}
		slicevalue.Set(reflect.Append(slicevalue, scannedvalue))
	}
	return rows.Err()
}

// Struct scans rows to dest, which must be a pointer to struct. Struct returns sql.ErrNoRows is there are no rows.
func Struct(rows *sql.Rows, dest interface{}) error {
	structvalue := reflect.ValueOf(dest).Elem()
	structtype := structvalue.Type()

	fields := make([]reflect.StructField, 0, structtype.NumField())
	for i := 0; i < structtype.NumField(); i++ {
		fields = append(fields, structtype.Field(i))
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	var dests []interface{}
	for _, col := range columns {
		var ok bool
		for _, field := range fields {
			if col == field.Tag.Get("scan") {
				dests = append(dests, structvalue.FieldByIndex(field.Index).Addr().Interface())
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("couldn't map column %s", col)
		}
	}

	if err := rows.Scan(dests...); err != nil {
		return err
	}
	return nil
}
