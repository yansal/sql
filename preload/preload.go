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
	ptrvalue := reflect.ValueOf(dest)
	if kind := ptrvalue.Kind(); kind != reflect.Ptr {
		panic(fmt.Sprintf("preload: dest is a value of kind %s, must be a pointer to a struct", kind))
	}
	if kind := ptrvalue.Type().Elem().Kind(); kind != reflect.Struct {
		panic(fmt.Sprintf("preload: dest is a pointer to a value of kind %s, must be a pointer to a struct", kind))
	}
	if ptrvalue.IsNil() {
		panic("preload: dest must not be nil")
	}

	slicevalue := reflect.MakeSlice(reflect.SliceOf(ptrvalue.Type()), 1, 1)
	slicevalue.Index(0).Set(ptrvalue)
	return preloadall(ctx, db, slicevalue, fields)
}

// StructSlice preloads fields into dest, which must be a slice of structs.
func StructSlice(ctx context.Context, db Querier, dest interface{}, fields []Field) error {
	destvalue := reflect.ValueOf(dest)
	if kind := destvalue.Kind(); kind != reflect.Slice {
		panic(fmt.Sprintf("preload: dest is a value of kind %s, must be a slice of structs", kind))
	}
	elemtype := destvalue.Type().Elem()
	if kind := elemtype.Kind(); kind != reflect.Struct {
		panic(fmt.Sprintf("preload: dest is a slice of values of kind %s, must be a slice of structs", kind))
	}
	if destvalue.Len() == 0 {
		return nil
	}

	elemmap := reflect.MakeMap(reflect.MapOf(reflect.PtrTo(elemtype), emptystructtype))
	for i, len := 0, destvalue.Len(); i < len; i++ {
		elemmap.SetMapIndex(destvalue.Index(i).Addr(), emptystruct)
	}
	slicevalue := reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(elemtype)), 0, elemmap.Len())
	for iter := elemmap.MapRange(); iter.Next(); {
		slicevalue = reflect.Append(slicevalue, iter.Key())
	}
	return preloadall(ctx, db, slicevalue, fields)
}

var (
	emptystruct     = reflect.ValueOf(struct{}{})
	emptystructtype = emptystruct.Type()
)

func preloadall(ctx context.Context, db Querier, slicevalue reflect.Value, fields []Field) error {
	for _, field := range fields {
		if err := preload(ctx, db, slicevalue, field); err != nil {
			return err
		}
	}
	return nil
}

func preload(ctx context.Context, db Querier, slicevalue reflect.Value, field Field) error {
	parents := lookupnested(slicevalue, field.Name)
	if parents.slicevalue.Len() == 0 {
		return nil
	}

	query, err := parents.setupquery()
	if err != nil {
		return err
	}
	if len(query.bindvalues) == 0 {
		return nil
	}

	query.where = field.Where
	query.orderby = field.OrderBy

	res, err := query.do(ctx, db)
	if err != nil {
		return err
	}
	if len(res.values) == 0 {
		return nil
	}

	parents.set(res)

	return nil
}

func lookupnested(slicevalue reflect.Value, nested string) *parents {
	reflecttype := slicevalue.Type().Elem().Elem()
	for {
		fieldname := nested
		i := strings.Index(nested, ".")
		if i != -1 {
			fieldname = nested[:i]
		}

		childfield, ok := reflecttype.FieldByName(fieldname)
		if !ok {
			panic(fmt.Sprintf("preload: %s does not have a field named %s", reflecttype, fieldname))
		}

		childtype := childfield.Type
		switch kind := childfield.Type.Kind(); kind {
		case reflect.Ptr:
			childtype = childtype.Elem()
			if kind := childtype.Kind(); kind != reflect.Struct {
				panic(fmt.Sprintf("preload: %s.%s is a pointer to a value of kind %s, must be pointer to struct or slice of structs", reflecttype, fieldname, kind))
			}
		case reflect.Slice:
			childtype = childtype.Elem()
			if kind := childtype.Kind(); kind != reflect.Struct {
				panic(fmt.Sprintf("preload: %s.%s is a slice of values of kind %s, must be pointer to struct or slice of structs", reflecttype, fieldname, kind))
			}
		default:
			panic(fmt.Sprintf("preload: %s.%s is a value of kind %s, must be pointer to struct or slice of structs", reflecttype, fieldname, kind))
		}

		if i == -1 {
			return &parents{
				reflecttype: reflecttype,
				slicevalue:  slicevalue,
				childfield:  childfield,
			}
		}

		nested = nested[i+1:]
		reflecttype = childtype

		childmap := reflect.MakeMap(reflect.MapOf(reflect.PtrTo(childtype), emptystructtype))
		len := slicevalue.Len()
		for i := 0; i < len; i++ {
			childvalue := slicevalue.Index(i).Elem().FieldByIndex(childfield.Index)
			switch kind := childfield.Type.Kind(); kind {
			case reflect.Ptr:
				if !childvalue.IsNil() {
					childmap.SetMapIndex(childvalue, emptystruct)
				}
			case reflect.Slice:
				for j, len := 0, childvalue.Len(); j < len; j++ {
					childmap.SetMapIndex(childvalue.Index(j).Addr(), emptystruct)
				}
			}
		}

		len = childmap.Len()
		slicevalue = reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(childtype)), 0, len)
		for iter := childmap.MapRange(); iter.Next(); {
			slicevalue = reflect.Append(slicevalue, iter.Key())
		}
	}
}

type parents struct {
	reflecttype reflect.Type
	slicevalue  reflect.Value
	childfield  reflect.StructField

	scantagvalues []interface{}
}

func (p *parents) setupquery() (*query, error) {
	var (
		childstructtype = p.childfield.Type.Elem()
		columns         []string
	)
	for i, numfields := 0, childstructtype.NumField(); i < numfields; i++ {
		field := childstructtype.Field(i)
		if value, ok := field.Tag.Lookup("scan"); ok {
			columns = append(columns, value)
		}
	}
	if len(columns) == 0 {
		panic(fmt.Sprintf(`preload: %s does not have fields with a "scan" struct tag`, p.childfield.Type.Elem()))
	}

	tag, ok := p.childfield.Tag.Lookup("preload")
	if !ok {
		panic(fmt.Sprintf(`preload: %s.%s does not have a "preload" struct tag`, p.reflecttype, p.childfield.Name))
	}
	submatchs := tagRegexp.FindStringSubmatch(tag)
	if len(submatchs) != 4 {
		panic(fmt.Sprintf(`preload: %s.%s "preload" struct tag is not valid`, p.reflecttype, p.childfield.Name))
	}
	table := submatchs[1]
	whereident := submatchs[2]
	scantag := submatchs[3]

	bindvaluemap := make(map[interface{}]struct{})
	for i, len := 0, p.slicevalue.Len(); i < len; i++ {
		scantagvalue, err := getScanTagValue(p.slicevalue.Index(i).Elem(), scantag)
		if err != nil {
			return nil, err
		}

		// TODO: don't add sql NULLs, and remove from p.slicevalue
		p.scantagvalues = append(p.scantagvalues, scantagvalue)

		// don't bind sql NULLs
		if scantagvalue == nil {
			continue
		}

		bindvaluemap[scantagvalue] = struct{}{}
	}
	bindvalues := make([]interface{}, 0, len(bindvaluemap))
	for k := range bindvaluemap {
		bindvalues = append(bindvalues, k)
	}

	return &query{
		columns:    columns,
		table:      table,
		whereident: whereident,
		bindvalues: bindvalues,
		childtype:  p.childfield.Type,
	}, nil
}

type query struct {
	columns    []string
	table      string
	whereident string
	bindvalues []interface{}
	childtype  reflect.Type

	where   build.Expression
	orderby []build.Expression
}

var tagRegexp = regexp.MustCompile(`\A([\w_]+).([\w_]+)\s=\s([\w_]+)\z`)

func getScanTagValue(structvalue reflect.Value, scantag string) (interface{}, error) {
	structtype := structvalue.Type()
	for i, numfields := 0, structtype.NumField(); i < numfields; i++ {
		value, ok := structtype.Field(i).Tag.Lookup("scan")
		if !ok || scantag != value {
			continue
		}
		scantagvalue := structvalue.Field(i).Interface()
		if valuer, ok := scantagvalue.(driver.Valuer); ok {
			return valuer.Value()
		}
		return scantagvalue, nil
	}
	panic(fmt.Sprintf(`preload: %s does not have a field with the scan:%s struct tag`, structtype, scantag))
}

func (q *query) do(ctx context.Context, db Querier) (*results, error) {
	var (
		res        results
		bindvalues = q.bindvalues
		desttype   reflect.Type
	)
	switch kind := q.childtype.Kind(); kind {
	case reflect.Ptr:
		desttype = reflect.SliceOf(q.childtype.Elem())
	case reflect.Slice:
		desttype = q.childtype
	}

	for {
		// TODO: make sqliteLimitVariableNumber optional?
		const sqliteLimitVariableNumber = 999
		subslice := bindvalues
		if len(subslice) > sqliteLimitVariableNumber {
			subslice = bindvalues[:sqliteLimitVariableNumber]
		}

		stmt := build.Select(build.Columns(q.columns...)...).
			From(build.Ident(q.table))
		where := build.Ident(q.whereident).In(build.Bind(subslice))
		if q.where != nil {
			where = where.And(q.where)
		}
		stmt = stmt.Where(where)
		if q.orderby != nil {
			stmt = stmt.OrderBy(q.orderby...)
		}
		query, args := stmt.Build()

		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		dest := reflect.New(desttype)
		if err := scan.StructSlice(rows, dest.Interface()); err != nil {
			return nil, err
		}
		destslice := dest.Elem()

		for i, len := 0, destslice.Len(); i < len; i++ {
			value := destslice.Index(i)
			scantagvalue, err := getScanTagValue(value, q.whereident)
			if err != nil {
				return nil, err
			}
			res.values = append(res.values, value)
			res.scantagvalues = append(res.scantagvalues, scantagvalue)
		}

		if len(bindvalues) <= sqliteLimitVariableNumber {
			break
		}
		bindvalues = bindvalues[sqliteLimitVariableNumber:]
	}

	return &res, nil
}

type results struct {
	values        []reflect.Value
	scantagvalues []interface{}
}

func (p *parents) set(res *results) {
	// 1. build the result map
	resultmap := make(map[interface{}]reflect.Value)
	var isptr bool
	if kind := p.childfield.Type.Kind(); kind == reflect.Ptr {
		isptr = true
	}
	for i := range res.values {
		if isptr {
			resultmap[res.scantagvalues[i]] = res.values[i].Addr()
		} else {
			values, ok := resultmap[res.scantagvalues[i]]
			if !ok {
				values = reflect.MakeSlice(p.childfield.Type, 0, 0)
			}
			resultmap[res.scantagvalues[i]] = reflect.Append(values, res.values[i])
		}
	}

	// 2. set child value in parents
	for i, len := 0, p.slicevalue.Len(); i < len; i++ {
		if p.scantagvalues[i] == nil {
			continue
		}

		childvalue := p.slicevalue.Index(i).Elem().FieldByName(p.childfield.Name)
		x, ok := resultmap[p.scantagvalues[i]]
		if !ok {
			continue
		}
		childvalue.Set(x)
	}
}
