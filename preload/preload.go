package preload

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/yansal/sql/build"
	"github.com/yansal/sql/scan"
)

// Querier is the interface required by functions in the preload package. It is
// implemented by *sql.DB and *sql.Tx.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// Struct preloads fieldnames into dest, which must be a pointer to a struct.
func Struct(ctx context.Context, db Querier, dest interface{}, fieldnames ...string) error {
	reflectvalue := reflect.ValueOf(dest).Elem()
	slicevalue := reflect.New(reflect.SliceOf(reflectvalue.Type())).Elem()
	slicevalue = reflect.Append(slicevalue, reflectvalue)
	if err := structslice(ctx, db, slicevalue, fieldnames...); err != nil {
		return err
	}
	reflectvalue.Set(slicevalue.Index(0))
	return nil
}

// StructSlice preloads fieldnames into dest, which must be a struct slice.
func StructSlice(ctx context.Context, db Querier, dest interface{}, fieldnames ...string) error {
	return structslice(ctx, db, reflect.ValueOf(dest), fieldnames...)
}

func structslice(ctx context.Context, db Querier, parentvalues reflect.Value, fieldnames ...string) error {
	if parentvalues.Len() == 0 {
		return nil
	}
	parenttype := parentvalues.Index(0).Type()

	for _, fieldname := range fieldnames {
		if strings.Contains(fieldname, ".") {
			continue
		}

		child, ok := parenttype.FieldByName(fieldname)
		if !ok {
			panic(fmt.Sprintf("%v does not have a field named %q", parenttype.Name(), fieldname))
		}

		columns := listScanTags(child.Type.Elem())
		if len(columns) == 0 {
			panic(fmt.Sprintf(`%v does not have fields with a "scan" struct tag`, child.Type.Elem()))
		}

		tag, ok := child.Tag.Lookup("preload")
		if !ok {
			panic(fmt.Sprintf(`%s.%s does not have a "preload" struct tag`, parenttype.Name(), child.Name))
		}
		submatchs := tagRegexp.FindStringSubmatch(tag)
		if len(submatchs) != 4 {
			panic(fmt.Sprintf(`%s.%s "preload" struct tag is not valid`, parenttype.Name(), child.Name))
		}
		table := submatchs[1]
		whereident := submatchs[2]
		scantag := submatchs[3]

		var bindvalues []interface{}
		for i := 0; i < parentvalues.Len(); i++ {
			bindvalue := getScanTagValue(parentvalues.Index(i), scantag)

			// don't append sql NULLs
			if isnil, err := valuesAreEqual(bindvalue, nil); err != nil {
				return err
			} else if isnil {
				continue
			}

			// TODO: ensure items in bindvalues are unique?
			bindvalues = append(bindvalues, bindvalue)
		}
		if bindvalues == nil {
			continue
		}

		// TODO: add callbacks: WHERE, ORDER BY, LIMIT
		query, args := build.Select(build.Columns(columns...)...).
			From(build.Ident(table)).
			Where(build.Ident(whereident).In(build.Bind(bindvalues))).
			Build()

		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		var dest reflect.Value
		switch child.Type.Kind() {
		case reflect.Ptr:
			dest = reflect.New(reflect.SliceOf(child.Type.Elem()))
		case reflect.Slice:
			dest = reflect.New(child.Type)
		default:
			panic(fmt.Sprintf("don't know how to scan rows into a value of kind %v", child.Type.Kind()))
		}

		if err := scan.StructSlice(rows, dest.Interface()); err != nil {
			return err
		}
		childvalues := dest.Elem()

		nested := trimprefix(fieldnames, fieldname+".")
		if err := structslice(ctx, db, childvalues, nested...); err != nil {
			return err
		}

		for i := 0; i < parentvalues.Len(); i++ {
			parentvalue := parentvalues.Index(i)
			parentfield := parentvalue.FieldByName(fieldname)
			parentscantagvalue := getScanTagValue(parentvalue, scantag)
			for j := 0; j < childvalues.Len(); j++ {
				// TODO: don't recompute childvalues inside the inner loop
				childvalue := childvalues.Index(j)
				childscantagvalue := getScanTagValue(childvalue, whereident)
				if ok, err := valuesAreEqual(parentscantagvalue, childscantagvalue); err != nil {
					return err
				} else if !ok {
					continue
				}

				switch child.Type.Kind() {
				case reflect.Ptr:
					parentfield.Set(childvalue.Addr())
				case reflect.Slice:
					parentfield.Set(reflect.Append(parentfield, childvalue))
				default:
					panic(fmt.Sprintf("don't know how to assign to a value of kind %v", child.Type.Kind()))
				}
			}
		}
	}
	return nil
}

func listScanTags(structtype reflect.Type) []string {
	var columns []string
	for i := 0; i < structtype.NumField(); i++ {
		field := structtype.Field(i)
		if value, ok := field.Tag.Lookup("scan"); ok {
			columns = append(columns, value)
		}
	}
	return columns
}

var tagRegexp = regexp.MustCompile(`\A([\w_]+).([\w_]+)\s=\s([\w_]+)\z`)

func getScanTagValue(structvalue reflect.Value, scantag string) interface{} {
	structtype := structvalue.Type()
	for i := 0; i < structtype.NumField(); i++ {
		if value, ok := structtype.Field(i).Tag.Lookup("scan"); ok && scantag == value {
			return structvalue.Field(i).Interface()
		}
	}
	panic(fmt.Sprintf(`%v does not have a field with the scan:%q struct tag`, structtype, scantag))
}

func valuesAreEqual(v1, v2 interface{}) (bool, error) {
	if dv, ok := v1.(driver.Valuer); ok {
		var err error
		v1, err = dv.Value()
		if err != nil {
			return false, err
		}
	}

	if dv, ok := v2.(driver.Valuer); ok {
		var err error
		v2, err = dv.Value()
		if err != nil {
			return false, err
		}
	}

	return v1 == v2, nil
}

func trimprefix(ss []string, prefix string) []string {
	var trimmed []string
	for _, s := range ss {
		if strings.HasPrefix(s, prefix) {
			trimmed = append(trimmed, strings.TrimPrefix(s, prefix))
		}
	}
	return trimmed
}
