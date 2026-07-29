package cachekey

import (
	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/cacheentry"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/predicate"
)

// Exact matches a cache key without depending on the database's default
// collation. The ordinary equality remains present so the composite lookup
// index can narrow candidates before the dialect-specific confirmation.
func Exact(value string) predicate.CacheEntry {
	return func(selector *sql.Selector) {
		column := selector.C(cacheentry.FieldKey)
		predicates := []*sql.Predicate{sql.EQ(column, value)}
		if confirmation := exactConfirmation(selector, column, value); confirmation != nil {
			predicates = append(predicates, confirmation)
		}
		selector.Where(sql.And(predicates...))
	}
}

// Prefix matches the literal cache-key prefix without depending on the
// database's default collation. HasPrefix provides the sargable, escaped LIKE
// candidate predicate; the equality on a length-limited value confirms case
// without interpreting %, _, or backslash as wildcards a second time.
func Prefix(value string) predicate.CacheEntry {
	return func(selector *sql.Selector) {
		column := selector.C(cacheentry.FieldKey)
		selector.Where(sql.And(
			sql.HasPrefix(column, value),
			prefixConfirmation(selector, column, value),
		))
	}
}

func exactConfirmation(selector *sql.Selector, column string, value string) *sql.Predicate {
	switch selector.Dialect() {
	case dialect.SQLite:
		// SQLite equality on the uncollated key column is already binary and
		// case-sensitive; only LIKE needs an explicit confirmation.
		return nil
	case dialect.MySQL:
		return sql.P(func(builder *sql.Builder) {
			builder.WriteString("CAST(").Ident(column).WriteString(" AS BINARY)").
				WriteOp(sql.OpEQ).
				WriteString("CAST(").Arg(value).WriteString(" AS BINARY)")
		})
	case dialect.Postgres:
		return sql.P(func(builder *sql.Builder) {
			builder.Ident(column).WriteString(" COLLATE ").Ident("C").WriteOp(sql.OpEQ).Arg(value)
		})
	default:
		return nil
	}
}

func prefixConfirmation(selector *sql.Selector, column string, value string) *sql.Predicate {
	return sql.P(func(builder *sql.Builder) {
		switch selector.Dialect() {
		case dialect.SQLite:
			builder.WriteString("substr(").Ident(column).WriteString(", 1, length(").Arg(value).
				WriteString(")) COLLATE BINARY").WriteOp(sql.OpEQ).Arg(value)
		case dialect.MySQL:
			builder.WriteString("CAST(LEFT(").Ident(column).WriteString(", CHAR_LENGTH(").Arg(value).
				WriteString(")) AS BINARY)").WriteOp(sql.OpEQ).
				WriteString("CAST(").Arg(value).WriteString(" AS BINARY)")
		case dialect.Postgres:
			builder.WriteString("LEFT(").Ident(column).WriteString(", CHAR_LENGTH(").Arg(value).
				WriteString(")) COLLATE ").Ident("C").WriteOp(sql.OpEQ).Arg(value)
		default:
			builder.WriteString("substr(").Ident(column).WriteString(", 1, length(").Arg(value).
				WriteString("))").WriteOp(sql.OpEQ).Arg(value)
		}
	})
}
