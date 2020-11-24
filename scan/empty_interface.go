package scan

import "database/sql"

// Map returns one row scanned into a map[string]interface{} value. Map returns
// sql.ErrNoRows is there are no rows.
func Map(rows Rows) (map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, sql.ErrNoRows
	}

	var scannedptrs []interface{}
	for range columns {
		scannedptrs = append(scannedptrs, new(interface{}))
	}
	if err := rows.Scan(scannedptrs...); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	m := make(map[string]interface{}, len(scannedptrs))
	for i, ptr := range scannedptrs {
		m[columns[i]] = *ptr.(*interface{})
	}
	return m, nil
}

// MapSlice returns rows scanned into a []map[string]interface{} value.
func MapSlice(rows Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	scannedrows, err := scanRows(columns, rows)
	if err != nil {
		return nil, err
	}

	maps := make([]map[string]interface{}, 0, len(scannedrows))
	for _, row := range scannedrows {
		values := make(map[string]interface{}, len(row))
		for i, ptr := range row {
			values[columns[i]] = *ptr.(*interface{})
		}
		maps = append(maps, values)
	}
	return maps, nil
}

// SliceSlice returns rows scanned into a [][]interface{} value.
func SliceSlice(rows Rows) ([][]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	scannedrows, err := scanRows(columns, rows)
	if err != nil {
		return nil, err
	}

	slices := make([][]interface{}, 0, len(scannedrows))
	for _, row := range scannedrows {
		values := make([]interface{}, len(row))
		for i, ptr := range row {
			values[i] = *ptr.(*interface{})
		}
		slices = append(slices, values)
	}
	return slices, nil
}

func scanRows(columns []string, rows Rows) ([][]interface{}, error) {
	var scannedrows [][]interface{}
	for rows.Next() {
		var scannedptrs []interface{}
		for range columns {
			scannedptrs = append(scannedptrs, new(interface{}))
		}

		if err := rows.Scan(scannedptrs...); err != nil {
			return nil, err
		}

		scannedrows = append(scannedrows, scannedptrs)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return scannedrows, nil
}
