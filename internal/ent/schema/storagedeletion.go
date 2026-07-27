package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
)

type StorageDeletion struct {
	ent.Schema
}

func (StorageDeletion) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "storage_deletions"},
	}
}

func (StorageDeletion) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").Immutable(),
		field.String("folderName").
			NotEmpty().
			StorageKey("folderName").
			SchemaType(originalTextColumnType),
		field.Int64("createdAt").StorageKey("createdAt"),
		field.Int("attemptCount").Default(0).NonNegative().StorageKey("attemptCount"),
		field.Int64("lastAttemptedAt").Optional().Nillable().StorageKey("lastAttemptedAt"),
		field.String("lastError").Optional().Nillable().StorageKey("lastError").SchemaType(originalTextColumnType),
	}
}
