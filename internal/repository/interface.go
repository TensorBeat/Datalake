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
	GetSongsByTags(ctx context.Context, tags map[string]string, filter proto.Filter, pageToken int64, pageSize int64) ([]*File, int64, error)
	GetSongsByIDs(ctx context.Context, ids []string, pageToken int64, pageSize int64) ([]*File, int64, error)
	GetAllSongs(ctx context.Context, pageToken int64, pageSize int64) ([]*File, int64, error)
	AddTags(ctx context.Context, id string, tags map[string]string) error
	RemoveTags(ctx context.Context, id string, tags map[string]string) error
}
