package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

type StorageReaderLease struct {
	ent.Schema
}

func (StorageReaderLease) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "storage_reader_leases"},
	}
}

func (StorageReaderLease) Fields() []ent.Field {
	return []ent.Field{
		field.String("id").
			Immutable().
			Unique().
			SchemaType(originalIDColumnType),
		field.String("storageLocationId").
			StorageKey("storageLocationId").
			SchemaType(originalIDColumnType),
		field.Enum("scope").Values("parts", "storage"),
		field.Int64("expiresAt").StorageKey("expiresAt"),
	}
}

func (StorageReaderLease) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("storageLocation", StorageLocation.Type).
			Ref("readerLeases").
			Unique().
			Required().
			Field("storageLocationId"),
	}
}

func (StorageReaderLease) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("storageLocationId", "scope", "expiresAt").
			StorageKey("idx_storage_reader_leases_location_scope_expiry"),
	}
}
