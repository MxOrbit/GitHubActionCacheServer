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
		field.String("id").Immutable().Unique(),
		field.String("key").MaxLen(512).NotEmpty(),
		field.String("version").MaxLen(255).NotEmpty(),
		field.String("scope").MaxLen(255).NotEmpty(),
		field.String("repoId").MaxLen(255).NotEmpty(),
		field.Int64("updatedAt"),
		field.String("locationId"),
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
		index.Fields("key", "version"),
		index.Fields("scope"),
		index.Fields("repoId"),
	}
}
