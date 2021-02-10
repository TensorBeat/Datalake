package repository

import (
	"context"

	"github.com/TensorBeat/Datalake/pkg/proto"
)

type Repository interface {
	SongRepository
}

type SongRepository interface {
	AddSongs(ctx context.Context, songs []*proto.File) error
	GetSongsByMetadata(ctx context.Context, metadata map[string]string) ([]*proto.File, error)
	GetSongs(ctx context.Context) ([]*proto.File, error)
}
