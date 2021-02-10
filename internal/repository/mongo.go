package repository

import (
	"context"

	"github.com/TensorBeat/Datalake/pkg/proto"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
)

const (
	songCollectionName = "songs"
)

type MongoRepository struct {
	client       *mongo.Client
	logger       *zap.SugaredLogger
	databaseName string

	songCollection *mongo.Collection
}

func NewMongoRepository(client *mongo.Client, logger *zap.SugaredLogger, databaseName string) *MongoRepository {
	songCollection := client.Database(databaseName).Collection(songCollectionName)

	return &MongoRepository{
		client:         client,
		logger:         logger,
		databaseName:   databaseName,
		songCollection: songCollection,
	}
}

func (r *MongoRepository) AddSongs(ctx context.Context, songs []*proto.File) error {

	documents := make([]interface{}, len(songs))
	for i := range songs {
		documents[i] = songs[i]
	}

	_, err := r.songCollection.InsertMany(ctx, documents)

	if err != nil {
		r.logger.Errorf("Failed to add songs to mongo: %v", songs)
		return err
	}

	r.logger.Infof("Added songs to mongo: %v", songs)

	return nil

}

func (r *MongoRepository) GetSongsByMetadata(ctx context.Context, metadata map[string]string) ([]*proto.File, error) {
	panic("not implemented") // TODO: Implement
}

func (r *MongoRepository) GetSongs(ctx context.Context) ([]*proto.File, error) {
	panic("not implemented") // TODO: Implement
}
