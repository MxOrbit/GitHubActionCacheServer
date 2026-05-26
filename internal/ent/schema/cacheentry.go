package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

type CacheEntry struct {
	ent.Schema
}

func (CacheEntry) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "cache_entries"},
	}
}

func (CacheEntry) Fields() []ent.Field {
	return []ent.Field{
		field.String("id").
			Immutable().
			Unique().
			SchemaType(originalIDColumnType),
		field.String("key").MaxLen(512).NotEmpty().SchemaType(originalBoundedStringColumnType(512)),
		field.String("version").MaxLen(255).NotEmpty().SchemaType(originalBoundedStringColumnType(255)),
		field.String("scope").MaxLen(255).NotEmpty().SchemaType(originalBoundedStringColumnType(255)),
		field.String("repoId").
			MaxLen(255).
			NotEmpty().
			StorageKey("repoId").
			SchemaType(originalBoundedStringColumnType(255)),
		field.Int64("updatedAt").StorageKey("updatedAt"),
		field.String("locationId").
			StorageKey("locationId").
			SchemaType(originalIDColumnType),
	}
}

func (CacheEntry) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("location", StorageLocation.Type).
			Ref("cacheEntries").
			Unique().
			Required().
			Field("locationId"),
	}
}

func (CacheEntry) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("key", "version").StorageKey("idx_cache_entries_key_version"),
		index.Fields("scope").StorageKey("idx_cache_entries_scope"),
		index.Fields("repoId").StorageKey("idx_cache_entries_repoId"),
	}
}
