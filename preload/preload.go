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

// A Field is a field to preload.
type Field struct {
	Name    string
	Where   build.Expression
	OrderBy []build.Expression
}

// Struct preloads fields into dest, which must be a pointer to a struct.
func Struct(ctx context.Context, db Querier, dest interface{}, fields []Field) error {
	reflectvalue := reflect.ValueOf(dest).Elem()
	slicevalue := reflect.MakeSlice(reflect.SliceOf(reflectvalue.Type()), 0, 1)
	slicevalue = reflect.Append(slicevalue, reflectvalue)
	if err := structslice(ctx, db, slicevalue, fields); err != nil {
		return err
	}
	reflectvalue.Set(slicevalue.Index(0))
	return nil
}

// StructSlice preloads fields into dest, which must be a slice of structs.
func StructSlice(ctx context.Context, db Querier, dest interface{}, fields []Field) error {
	return structslice(ctx, db, reflect.ValueOf(dest), fields)
}

func structslice(ctx context.Context, db Querier, parentvalues reflect.Value, fields []Field) error {
	for _, field := range fields {
		if err := preload(ctx, db, parentvalues, field); err != nil {
			return err
		}
	}
	return nil
}

func preload(ctx context.Context, db Querier, parentvalues reflect.Value, field Field) error {
	parenttype := parentvalues.Index(0).Type()
	var child reflect.StructField
	for {
		if parentvalues.Len() == 0 {
			return nil
		}

		i := strings.Index(field.Name, ".")
		if i == -1 {
			var ok bool
			child, ok = parenttype.FieldByName(field.Name)
			if !ok {
				panic(fmt.Sprintf("%v does not have a field named %q", parenttype.Name(), field.Name))
			}
			break
		}

		fieldname := field.Name[:i]
		child, ok := parenttype.FieldByName(fieldname)
		if !ok {
			panic(fmt.Sprintf("%v does not have a field named %q", parenttype.Name(), field.Name))
		}

		field.Name = field.Name[i+1:]

		parenttype = child.Type.Elem()
		slicevalue := reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(parenttype)), 0, 0)
		for i := 0; i < parentvalues.Len(); i++ {
			parentvalue := parentvalues.Index(i)
			if parentvalue.Kind() == reflect.Ptr {
				parentvalue = parentvalue.Elem()
			}
			childvalue := parentvalue.FieldByIndex(child.Index)

			switch kind := child.Type.Kind(); kind {
			case reflect.Ptr:
				if !childvalue.IsNil() {
					slicevalue = reflect.Append(slicevalue, childvalue)
				}
			case reflect.Slice:
				for j := 0; j < childvalue.Len(); j++ {
					slicevalue = reflect.Append(slicevalue, childvalue.Index(j).Addr())
				}
			default:
				panic(fmt.Sprintf("don't know how to preload a field of kind %v", kind))
			}
		}
		parentvalues = slicevalue
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

	mapbindvalues := make(map[interface{}]struct{})
	for i := 0; i < parentvalues.Len(); i++ {
		bindvalue := getScanTagValue(parentvalues.Index(i), scantag)

		// don't append sql NULLs
		if isnil, err := valuesAreEqual(bindvalue, nil); err != nil {
			return err
		} else if isnil {
			continue
		}

		mapbindvalues[bindvalue] = struct{}{}
	}
	if len(mapbindvalues) == 0 {
		return nil
	}
	bindvalues := make([]interface{}, 0, len(mapbindvalues))
	for k := range mapbindvalues {
		bindvalues = append(bindvalues, k)
	}

	s := build.Select(build.Columns(columns...)...).
		From(build.Ident(table))
	where := build.Ident(whereident).In(build.Bind(bindvalues))
	if field.Where != nil {
		where = where.And(field.Where)
	}
	s = s.Where(where)
	if field.OrderBy != nil {
		s = s.OrderBy(field.OrderBy...)
	}
	query, args := s.Build()

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	var dest reflect.Value
	switch kind := child.Type.Kind(); kind {
	case reflect.Ptr:
		dest = reflect.New(reflect.SliceOf(child.Type.Elem()))
	case reflect.Slice:
		dest = reflect.New(child.Type)
	default:
		panic(fmt.Sprintf("don't know how to scan rows into a value of kind %v", kind))
	}

	if err := scan.StructSlice(rows, dest.Interface()); err != nil {
		return err
	}
	destvalues := dest.Elem()

	for i := 0; i < parentvalues.Len(); i++ {
		parentvalue := parentvalues.Index(i)
		parentscantagvalue := getScanTagValue(parentvalue, scantag)
		if parentvalue.Kind() == reflect.Ptr {
			parentvalue = parentvalue.Elem()
		}
		childvalue := parentvalue.FieldByName(field.Name)

		for j := 0; j < destvalues.Len(); j++ {
			destvalue := destvalues.Index(j)
			destscantagvalue := getScanTagValue(destvalue, whereident)
			if ok, err := valuesAreEqual(parentscantagvalue, destscantagvalue); err != nil {
				return err
			} else if !ok {
				continue
			}
			switch kind := child.Type.Kind(); kind {
			case reflect.Ptr:
				childvalue.Set(destvalue.Addr())
			case reflect.Slice:
				childvalue.Set(reflect.Append(childvalue, destvalue))
			default:
				panic(fmt.Sprintf("don't know how to assign to a value of kind %v", kind))
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
	if structvalue.Kind() == reflect.Ptr {
		structvalue = structvalue.Elem()
	}
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
