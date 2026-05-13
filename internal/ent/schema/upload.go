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
		field.Int64("id").Immutable().Unique(),
		field.String("key").MaxLen(512).NotEmpty(),
		field.String("version").MaxLen(255).NotEmpty(),
		field.String("scope").MaxLen(255).NotEmpty(),
		field.String("repoId").MaxLen(255).NotEmpty(),
		field.Int64("createdAt"),
		field.Int64("lastPartUploadedAt").Optional().Nillable(),
		field.Int("startedPartUploadCount").Default(0).NonNegative(),
		field.Int("finishedPartUploadCount").Default(0).NonNegative(),
		field.String("folderName").NotEmpty(),
	}
}

func (Upload) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("key", "version"),
		index.Fields("scope"),
		index.Fields("repoId"),
		index.Fields("key", "version", "scope", "repoId").Unique(),
	}
}
