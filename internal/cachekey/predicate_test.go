package cachekey

import (
	"testing"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/cacheentry"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/predicate"
	"github.com/stretchr/testify/require"
)

func TestExactKeepsIndexedEqualityAndAddsDialectConfirmation(t *testing.T) {
	for _, test := range []struct {
		dialect      string
		confirmation string
	}{
		{dialect: dialect.SQLite},
		{dialect: dialect.MySQL, confirmation: "CAST(`cache_entries`.`key` AS BINARY) = CAST(? AS BINARY)"},
		{dialect: dialect.Postgres, confirmation: `"cache_entries"."key" COLLATE "C" = $2`},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			query, args := renderPredicate(test.dialect, Exact("Case-Key"))

			require.Contains(t, query, dialectIdentifier(test.dialect, "key")+" = ")
			if test.confirmation != "" {
				require.Contains(t, query, test.confirmation)
			}
			require.NotEmpty(t, args)
			for _, arg := range args {
				require.Equal(t, "Case-Key", arg)
			}
		})
	}
}

func TestPrefixKeepsEscapedLikeAndUsesLengthLimitedEquality(t *testing.T) {
	const prefix = `literal%_\`
	for _, test := range []struct {
		dialect      string
		confirmation string
	}{
		{dialect: dialect.SQLite, confirmation: "substr(`cache_entries`.`key`, 1, length(?)) COLLATE BINARY = ?"},
		{dialect: dialect.MySQL, confirmation: "CAST(LEFT(`cache_entries`.`key`, CHAR_LENGTH(?)) AS BINARY) = CAST(? AS BINARY)"},
		{dialect: dialect.Postgres, confirmation: `LEFT("cache_entries"."key", CHAR_LENGTH($2)) COLLATE "C" = $3`},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			query, args := renderPredicate(test.dialect, Prefix(prefix))

			require.Contains(t, query, dialectIdentifier(test.dialect, "key")+" LIKE ")
			require.Contains(t, query, test.confirmation)
			require.Equal(t, `literal\%\_\\%`, args[0])
			if test.dialect == dialect.SQLite {
				require.Equal(t, []any{`literal\%\_\\%`, `\`, prefix, prefix}, args)
			} else {
				require.Equal(t, []any{`literal\%\_\\%`, prefix, prefix}, args)
			}
		})
	}
}

func renderPredicate(name string, match predicate.CacheEntry) (string, []any) {
	dialectBuilder := sql.Dialect(name)
	table := dialectBuilder.Table(cacheentry.Table)
	selector := dialectBuilder.Select(table.C(cacheentry.FieldID)).From(table)
	match(selector)
	return selector.Query()
}

func dialectIdentifier(name string, field string) string {
	if name == dialect.Postgres {
		return `"cache_entries"."` + field + `"`
	}
	return "`cache_entries`.`" + field + "`"
}
