package schema

import (
	"fmt"

	"entgo.io/ent/dialect"
)

var originalTextColumnType = map[string]string{
	dialect.MySQL:    "text",
	dialect.Postgres: "text",
	dialect.SQLite:   "text",
}

var originalIDColumnType = map[string]string{
	dialect.MySQL:    "varchar(36)",
	dialect.Postgres: "text",
	dialect.SQLite:   "text",
}

func originalBoundedStringColumnType(size int) map[string]string {
	return map[string]string{
		dialect.MySQL:    fmt.Sprintf("varchar(%d)", size),
		dialect.Postgres: "text",
		dialect.SQLite:   "text",
	}
}
