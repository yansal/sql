package scan

import "database/sql"

// MapSlice returns rows scanned into a []map[string]interface{} value.
func MapSlice(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

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
