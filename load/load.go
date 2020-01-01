package load

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"

	"github.com/yansal/sql/build"
	"github.com/yansal/sql/scan"
)

type DB interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// Struct loads field into s, which must be a pointer to a struct.
//
// TODO: replace field string with fields ...string.
func Struct(ctx context.Context, db DB, s interface{}, field string) error {
	structvalue := reflect.ValueOf(s).Elem()
	structtype := structvalue.Type()

	structfield, ok := structtype.FieldByName(field)
	if !ok {
		panic(fmt.Sprintf("%q struct does not have a field named %q", structtype.Name(), field))
	}

	columns := scanTags(structfield.Type.Elem())
	if len(columns) == 0 {
		panic(fmt.Sprintf(`%q struct does not have fields with a "scan" struct tag`, structtype))
	}

	tag, ok := structfield.Tag.Lookup("load")
	if !ok {
		panic(fmt.Sprintf(`%s.%s struct field does not have a "load" struct tag`, structtype.Name(), structfield.Name))
	}
	submatchs := tagRegexp.FindStringSubmatch(tag)
	if len(submatchs) != 4 {
		panic(fmt.Sprintf(`%s.%s "load" struct tag is not valid`, structtype.Name(), structfield.Name))
	}
	table, column, localfield := submatchs[1], submatchs[2], submatchs[3]
	localvalue := valueWithScanTag(structvalue, localfield)

	query, args := build.Select(build.Columns(columns...)...).
		From(build.Ident(table)).
		Where(
			build.Infix(build.Ident(column)).Equal(build.Bind(localvalue.Interface())),
		).Build()

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	switch structfield.Type.Kind() {
	case reflect.Slice:
		if err := scan.StructSlice(rows, structvalue.FieldByName(field).Addr().Interface()); err != nil {
			return err
		}
	case reflect.Ptr:
		value := reflect.New(structfield.Type.Elem())
		if err := scan.Struct(rows, value.Interface()); err != nil {
			return err
		}
		structvalue.FieldByName(field).Set(value)
	default:
		panic(fmt.Sprintf("don't know how to scan rows into a value of kind %v", structfield.Type.Kind()))
	}
	return nil
}

func scanTags(structtype reflect.Type) []string {
	var columns []string
	for i := 0; i < structtype.NumField(); i++ {
		field := structtype.Field(i)
		if value, ok := field.Tag.Lookup("scan"); ok {
			columns = append(columns, value)
		}
	}
	return columns
}

func valueWithScanTag(structvalue reflect.Value, scantag string) reflect.Value {
	structtype := structvalue.Type()
	for i := 0; i < structtype.NumField(); i++ {
		if scantag == structtype.Field(i).Tag.Get("scan") {
			return structvalue.Field(i)
		}
	}
	panic(fmt.Sprintf(`%q struct does not have a field with the %q "scan" struct tag`, structtype, scantag))
}

var tagRegexp = regexp.MustCompile(`^([\w_]*).([\w_]*)\s=\s([\w_]*)$`)
