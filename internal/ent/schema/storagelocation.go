package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
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
		field.Int64("sizeBytes").Optional().Nillable().NonNegative().StorageKey("sizeBytes"),
		field.Int64("leaseVersion").Default(0).NonNegative().StorageKey("leaseVersion"),
		field.Int64("deletionRequestedAt").Optional().Nillable().StorageKey("deletionRequestedAt"),
		field.Int64("mergeStartedAt").Optional().Nillable().StorageKey("mergeStartedAt"),
		field.String("mergeLeaseToken").
			Optional().
			Nillable().
			StorageKey("mergeLeaseToken").
			SchemaType(originalIDColumnType),
		field.Int64("mergeLeaseExpiresAt").Optional().Nillable().StorageKey("mergeLeaseExpiresAt"),
		field.Int64("mergedAt").Optional().Nillable().StorageKey("mergedAt"),
		field.Int64("materializationUnsupportedAt").Optional().Nillable().StorageKey("materializationUnsupportedAt"),
		field.Int64("partsDeletedAt").Optional().Nillable().StorageKey("partsDeletedAt"),
		field.Int64("lastDownloadedAt").Optional().Nillable().StorageKey("lastDownloadedAt"),
		// Materialized eviction recency; set at finalize and refreshed by download
		// touch alongside lastDownloadedAt. Relies on one cache entry per location
		// and entry.updatedAt being immutable after attach.
		field.Int64("recencyAt").Default(0).NonNegative().StorageKey("recencyAt"),
	}
}

func (StorageLocation) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("cacheEntries", CacheEntry.Type).
			Annotations(entsql.OnDelete(entsql.Cascade)),
		edge.To("readerLeases", StorageReaderLease.Type).
			Annotations(entsql.OnDelete(entsql.Restrict)),
	}
}

func (StorageLocation) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("recencyAt", "id").StorageKey("idx_storage_locations_recency"),
	}
}
