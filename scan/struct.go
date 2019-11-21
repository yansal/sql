package scan

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
)

// QueryStructSlice runs query with args and scans rows to dest, which must be a pointer to a slice of structs.
func QueryStructSlice(ctx context.Context, db Queryer, dest interface{}, query string, args ...interface{}) error {
	slicevalue := reflect.ValueOf(dest).Elem()
	structtype := slicevalue.Type().Elem()

	structfields := make([]reflect.StructField, 0, structtype.NumField())
	for i := 0; i < structtype.NumField(); i++ {
		structfields = append(structfields, structtype.Field(i))
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

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

// QueryStruct runs query with args and scans the row to dest, which must be a pointer to struct.
func QueryStruct(ctx context.Context, db Queryer, dest interface{}, query string, args ...interface{}) error {
	structvalue := reflect.ValueOf(dest).Elem()
	structtype := structvalue.Type()

	fields := make([]reflect.StructField, 0, structtype.NumField())
	for i := 0; i < structtype.NumField(); i++ {
		fields = append(fields, structtype.Field(i))
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

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
	return rows.Close()
}
