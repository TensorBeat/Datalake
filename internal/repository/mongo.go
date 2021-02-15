package repository

import (
	"context"

	"github.com/TensorBeat/Datalake/pkg/proto"
	"go.mongodb.org/mongo-driver/bson"
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

func (r *MongoRepository) AddSongs(ctx context.Context, songs []*proto.AddFile) error {

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

func (r *MongoRepository) GetSongsByTags(ctx context.Context, tags map[string]string, operator proto.LogicalOperator) ([]*proto.File, error) {

	tagsEntries := make([]bson.M, len(tags))

	for k, v := range tags {
		filterEntry := bson.M{k: v}
		tagsEntries = append(tagsEntries, filterEntry)
	}

	var filterExpression string

	switch operator {
	case proto.LogicalOperator_OR:
		filterExpression = "$in"
	case proto.LogicalOperator_NOT:
		filterExpression = "$nin"

	//TODO: implement AND
	default:
		filterExpression = "$in"
	}

	filter := bson.M{
		"tags": bson.M{
			filterExpression: tagsEntries,
		},
	}
	return r.getSongs(ctx, filter)
}

func (r *MongoRepository) GetSongs(ctx context.Context) ([]*proto.File, error) {

	return r.getSongs(ctx, bson.M{})

}

func (r *MongoRepository) getSongs(ctx context.Context, filter bson.M) ([]*proto.File, error) {
	cur, err := r.songCollection.Find(ctx, filter)
	if err != nil {
		r.logger.Errorf("Failed to find songs in mongo: %v", err)
		return nil, err
	}

	songs := make([]*proto.File, 0)

	cur.All(ctx, &songs)
	if err != nil {
		r.logger.Errorf("Failed to get songs in mongo: %v", err)
		return nil, err
	}

	r.logger.Debugf("Songs: %v", songs)

	return songs, nil
}
