package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

type StorageLocation struct {
	ent.Schema
}

func (StorageLocation) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "storage_locations"},
	}
}

func (StorageLocation) Fields() []ent.Field {
	return []ent.Field{
		field.String("id").
			Immutable().
			Unique().
			SchemaType(originalIDColumnType),
		field.String("folderName").
			NotEmpty().
			StorageKey("folderName").
			SchemaType(originalTextColumnType),
		field.Int("partCount").
			NonNegative().
			StorageKey("partCount"),
		field.Int64("mergeStartedAt").Optional().Nillable().StorageKey("mergeStartedAt"),
		field.Int64("mergedAt").Optional().Nillable().StorageKey("mergedAt"),
		field.Int64("materializationUnsupportedAt").Optional().Nillable().StorageKey("materializationUnsupportedAt"),
		field.Int64("partsDeletedAt").Optional().Nillable().StorageKey("partsDeletedAt"),
		field.Int64("lastDownloadedAt").Optional().Nillable().StorageKey("lastDownloadedAt"),
	}
}

func (StorageLocation) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("cacheEntries", CacheEntry.Type).
			Annotations(entsql.OnDelete(entsql.Cascade)),
	}
}
