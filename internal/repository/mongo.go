package repository

import (
	"context"

	"github.com/TensorBeat/Datalake/pkg/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
)

const (
	songCollectionName = "songs"
)

type MongoFile struct {
	ID       primitive.ObjectID `bson:"_id,omitempty"`
	Name     string             `bson:"name,omitempty"`
	Uri      string             `bson:"uri,omitempty"`
	MimeType string             `bson:"mimeType,omitempty"`
	Tags     map[string]string  `bson:"tags,omitempty"`
}

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

func (r *MongoRepository) AddSongs(ctx context.Context, songs []*File) error {

	mongoFiles := r.FilesToMongoFiles(songs)

	documents := make([]interface{}, len(songs))
	for i := range mongoFiles {
		documents[i] = mongoFiles[i]
	}

	_, err := r.songCollection.InsertMany(ctx, documents)

	if err != nil {
		r.logger.Errorf("Failed to add songs to mongo: %v", songs)
		return err
	}

	r.logger.Infof("Added songs to mongo: %v", songs)

	return nil

}

func (r *MongoRepository) GetSongsByTags(ctx context.Context, tags map[string]string, operator proto.Filter) ([]*File, error) {

	tagsEntries := make([]bson.M, len(tags))

	for k, v := range tags {
		filterEntry := bson.M{k: v}
		tagsEntries = append(tagsEntries, filterEntry)
	}

	var filterExpression string

	switch operator {
	case proto.Filter_ANY:
		filterExpression = "$in"
	case proto.Filter_NONE:
		filterExpression = "$nin"

	//TODO: implement ALL
	default:
		filterExpression = "$in"
	}

	query := bson.M{
		"tags": bson.M{
			filterExpression: tagsEntries,
		},
	}
	return r.getSongs(ctx, query)
}

func (r *MongoRepository) GetSongsByIDs(ctx context.Context, ids []string) ([]*File, error) {
	mongoIDs := make([]primitive.ObjectID, len(ids))

	for i := range mongoIDs {
		id, err := primitive.ObjectIDFromHex(ids[i])
		if err != nil {
			r.logger.Errorf("bad ID: %v", err)
			return nil, err
		}
		mongoIDs[i] = id
	}

	query := bson.M{"_id": bson.M{"$in": mongoIDs}}

	return r.getSongs(ctx, query)
}

func (r *MongoRepository) GetSongs(ctx context.Context) ([]*File, error) {

	return r.getSongs(ctx, bson.M{})

}

func (r *MongoRepository) getSongs(ctx context.Context, query bson.M) ([]*File, error) {
	cur, err := r.songCollection.Find(ctx, query)
	if err != nil {
		r.logger.Errorf("Failed to find songs in mongo: %v", err)
		return nil, err
	}

	songs := make([]*MongoFile, 0)

	cur.All(ctx, &songs)
	if err != nil {
		r.logger.Errorf("Failed to get songs in mongo: %v", err)
		return nil, err
	}

	r.logger.Debugf("Songs: %v", songs)

	files := r.MongoFilesToFiles(songs)

	return files, nil
}

func (r *MongoRepository) MongoFilesToFiles(mongoFiles []*MongoFile) []*File {
	files := make([]*File, len(mongoFiles))
	for i, mongoFile := range mongoFiles {
		files[i] = &File{
			ID:       mongoFile.ID.Hex(),
			Name:     mongoFile.Name,
			Uri:      mongoFile.Uri,
			MimeType: mongoFile.MimeType,
			Tags:     mongoFile.Tags,
		}
	}
	return files
}

func (r *MongoRepository) FilesToMongoFiles(files []*File) []*MongoFile {
	mongoFiles := make([]*MongoFile, 0)
	for _, file := range files {

		id, err := primitive.ObjectIDFromHex(file.ID)
		if err != nil {
			r.logger.Errorf("Couldn't convert file interface to mongofile due to bad ID, skipping entry: %v", err)
			continue
		}
		mongoFiles = append(mongoFiles, &MongoFile{
			ID:       id,
			Name:     file.Name,
			Uri:      file.Uri,
			MimeType: file.MimeType,
			Tags:     file.Tags,
		})
	}
	return mongoFiles
}
