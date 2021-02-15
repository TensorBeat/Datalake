package repository

import (
	"context"

	"github.com/TensorBeat/Datalake/pkg/proto"
)

type File struct {
	ID       string
	Name     string
	Uri      string
	MimeType string
	Tags     map[string]string
}

type Repository interface {
	SongRepository
}

type SongRepository interface {
	AddSongs(ctx context.Context, songs []*File) error
	GetSongsByTags(ctx context.Context, tags map[string]string, operator proto.LogicalOperator) ([]*File, error)
	GetSongs(ctx context.Context) ([]*File, error)
}
