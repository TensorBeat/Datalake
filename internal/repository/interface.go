package repository

import (
	"context"

	"github.com/TensorBeat/Datalake/pkg/proto"
)

type Repository interface {
	SongRepository
}

type SongRepository interface {
	AddSongs(ctx context.Context, songs []*proto.AddFile) error
	GetSongsByTags(ctx context.Context, tags map[string]string, operator proto.LogicalOperator) ([]*proto.File, error)
	GetSongs(ctx context.Context) ([]*proto.File, error)
}
