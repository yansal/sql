package scan

import (
	"database/sql"
	"fmt"
	"reflect"
)

// Rows is the interface from which the scan package scans values. It is
// implemented by *sql.Rows.
type Rows interface {
	Columns() ([]string, error)
	Next() bool
	Scan(dest ...interface{}) error
	Err() error
}

// StructSlice scans rows to dest, which must be a pointer to a slice of
// structs.
func StructSlice(rows Rows, dest interface{}) error {
	destvalue := reflect.ValueOf(dest)
	if kind := destvalue.Kind(); kind != reflect.Ptr {
		panic(fmt.Sprintf("scan: dest has kind %s, must be a pointer to a slice of structs", kind))
	}

	var (
		slicevalue = destvalue.Elem()
		structtype = slicevalue.Type().Elem()
	)
	if kind := structtype.Kind(); kind != reflect.Struct {
		panic(fmt.Sprintf("scan: dest is a pointer to a slice of values of kind %s, must be a pointer to a slice of structs", kind))
	}

	numfield := structtype.NumField()
	fields := make([]reflect.StructField, 0, numfield)
	for i := 0; i < numfield; i++ {
		fields = append(fields, structtype.Field(i))
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	var fieldindexes [][]int
	for _, col := range columns {
		var ok bool
		for _, field := range fields {
			if col == field.Tag.Get("scan") {
				fieldindexes = append(fieldindexes, field.Index)
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("scan: couldn't map column %s", col)
		}
	}

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

// Struct scans rows to dest, which must be a pointer to struct. Struct returns
// sql.ErrNoRows is there are no rows.
func Struct(rows Rows, dest interface{}) error {
	destvalue := reflect.ValueOf(dest)
	if kind := destvalue.Kind(); kind != reflect.Ptr {
		panic(fmt.Sprintf("scan: dest has kind %s, must be a pointer to struct", kind))
	}

	var (
		structvalue = destvalue.Elem()
		structtype  = structvalue.Type()
	)
	if kind := structtype.Kind(); kind != reflect.Struct {
		panic(fmt.Sprintf("scan: dest is a pointer to a value of kind %s, must be a pointer to struct", kind))
	}

	numfield := structtype.NumField()
	fields := make([]reflect.StructField, 0, numfield)
	for i := 0; i < numfield; i++ {
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
			return fmt.Errorf("scan: couldn't map column %s", col)
		}
	}

	if err := rows.Scan(dests...); err != nil {
		return err
	}
	return nil
}

// GetColumns returns "scan" struct tag strings in model.
func GetColumns(model interface{}) []string {
	reflecttype := reflect.TypeOf(model)
	var ss []string
	for i := 0; i < reflecttype.NumField(); i++ {
		field := reflecttype.Field(i)
		if value, ok := field.Tag.Lookup("scan"); ok {
			ss = append(ss, value)
		}
	}
	return ss
}

// GetValues returns field values in model where "scan" struct tag strings are
// in columns.
func GetValues(model interface{}, columns []string) []interface{} {
	reflectvalue := reflect.ValueOf(model)
	if reflectvalue.Kind() == reflect.Ptr {
		reflectvalue = reflectvalue.Elem()
	}
	reflecttype := reflectvalue.Type()
	values := make([]interface{}, len(columns))
	for i := range columns {
		var ok bool
		for j := 0; j < reflecttype.NumField(); j++ {
			tagvalue := reflecttype.Field(j).Tag.Get("scan")
			if columns[i] != tagvalue {
				continue
			}
			values[i] = reflectvalue.Field(j).Interface()
			ok = true
			break
		}
		if !ok {
			panic(fmt.Sprintf("scan: unknown column %q", columns[i]))
		}
	}
	return values
}
