package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

type Upload struct {
	ent.Schema
}

func (Upload) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "uploads"},
	}
}

func (Upload) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").
			Immutable().
			Unique().
			Annotations(entsql.Annotation{Incremental: new(bool)}),
		field.String("key").MaxLen(512).NotEmpty().SchemaType(originalBoundedStringColumnType(512)),
		field.String("version").MaxLen(255).NotEmpty().SchemaType(originalBoundedStringColumnType(255)),
		field.String("scope").MaxLen(255).NotEmpty().SchemaType(originalBoundedStringColumnType(255)),
		field.String("repoId").
			MaxLen(255).
			NotEmpty().
			StorageKey("repoId").
			SchemaType(originalBoundedStringColumnType(255)),
		field.Int64("createdAt").StorageKey("createdAt"),
		field.Int64("lastPartUploadedAt").Optional().Nillable().StorageKey("lastPartUploadedAt"),
		field.Int("startedPartUploadCount").Default(0).NonNegative().StorageKey("startedPartUploadCount"),
		field.Int("finishedPartUploadCount").Default(0).NonNegative().StorageKey("finishedPartUploadCount"),
		field.String("folderName").
			NotEmpty().
			StorageKey("folderName").
			SchemaType(originalTextColumnType),
		field.Int("committedPartCount").
			Optional().
			Nillable().
			NonNegative().
			StorageKey("committedPartCount"),
	}
}

func (Upload) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("key", "version").StorageKey("idx_uploads_key_version"),
		index.Fields("scope").StorageKey("idx_uploads_scope"),
		index.Fields("repoId").StorageKey("idx_uploads_repoId"),
		index.Fields("key", "version", "scope", "repoId").Unique(),
	}
}
