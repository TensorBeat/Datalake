package repository

import (
	"context"
	"fmt"

	"github.com/TensorBeat/Datalake/pkg/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
)

const (
	songCollectionName = "songs"
	tagsPrefix         = "tags."
	existsCharacter    = "*"
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

func (r *MongoRepository) GetSongsByTags(ctx context.Context, tags map[string]string, operator proto.Filter, pageToken int64, pageSize int64) ([]*File, int64, int64, error) {

	tagsEntries := make([]bson.M, 0)
	for tagName, val := range tags {

		var filterEntry bson.M

		if val == existsCharacter {
			filterEntry = bson.M{tagsPrefix + tagName: bson.M{
				"$exists": true,
			}}
		} else {
			filterEntry = bson.M{tagsPrefix + tagName: val}
		}

		tagsEntries = append(tagsEntries, filterEntry)
	}

	var query bson.M
	switch operator {
	case proto.Filter_ANY:
		query = bson.M{
			"$or": tagsEntries,
		}
	case proto.Filter_ALL:
		query = bson.M{
			"$and": tagsEntries,
		}
	case proto.Filter_NONE:
		query = bson.M{
			"$nor": tagsEntries,
		}
	default:
		query = bson.M{
			"$or": tagsEntries,
		}
	}

	return r.getSongs(ctx, query, pageToken, pageSize)
}

func (r *MongoRepository) GetSongsByIDs(ctx context.Context, ids []string, pageToken int64, pageSize int64) ([]*File, int64, int64, error) {
	mongoIDs := make([]primitive.ObjectID, len(ids))

	for i := range mongoIDs {
		id, err := primitive.ObjectIDFromHex(ids[i])
		if err != nil {
			r.logger.Errorf("bad ID: %v", err)
			return nil, pageToken, 0, err
		}
		mongoIDs[i] = id
	}

	query := bson.M{"_id": bson.M{"$in": mongoIDs}}

	return r.getSongs(ctx, query, pageToken, pageSize)
}

func (r *MongoRepository) GetAllSongs(ctx context.Context, pageToken int64, pageSize int64) ([]*File, int64, int64, error) {

	return r.getSongs(ctx, bson.M{}, pageToken, pageSize)

}

func (r *MongoRepository) getSongs(ctx context.Context, query bson.M, pageToken int64, pageSize int64) ([]*File, int64, int64, error) {

	r.logger.Debugf("query: %v", query)

	if pageToken < 0 {
		err := fmt.Errorf("pageToken must be non-negative: %v", pageToken)
		r.logger.Error(err)
		return nil, 0, 0, err
	}

	if pageSize < 0 {
		err := fmt.Errorf("pageSize must be non-negative: %v", pageSize)
		r.logger.Error(err)
		return nil, pageToken, 0, err
	}

	cur, err := r.songCollection.Find(ctx, query)
	if err != nil {
		r.logger.Errorf("Failed to find songs in mongo: %v", err)
		return nil, pageToken, 0, err
	}

	songs := make([]*MongoFile, 0)

	cur.All(ctx, &songs)
	if err != nil {
		r.logger.Errorf("Failed to get songs in mongo: %v", err)
		return nil, pageToken, 0, err
	}

	r.logger.Debugf("Songs: %v", songs)

	files := r.MongoFilesToFiles(songs)
	if pageSize == 0 {
		files = files[pageToken:]
	} else {
		files = files[pageToken : pageToken+pageSize]
	}

	return files, pageToken + pageSize, int64(len(files)), nil
}

func (r *MongoRepository) AddTags(ctx context.Context, id string, tags map[string]string) error {
	mongoID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		r.logger.Errorf("bad ID: %v", err)
		return err
	}

	tagsToSet := make(map[string]string)
	for tagName, val := range tags {
		tagsToSet[tagsPrefix+tagName] = val
	}

	filter := bson.M{
		"_id": mongoID,
	}
	update := bson.M{
		"$set": tagsToSet,
	}
	_, err = r.songCollection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}

	return nil
}

func (r *MongoRepository) RemoveTags(ctx context.Context, id string, tags map[string]string) error {
	mongoID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		r.logger.Errorf("bad ID: %v", err)
		return err
	}

	tagsToUnset := make(map[string]string)
	for tagName := range tags {
		tagsToUnset[tagsPrefix+tagName] = ""
	}

	filter := bson.M{
		"_id": mongoID,
	}
	update := bson.M{
		"$unset": tagsToUnset,
	}
	_, err = r.songCollection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}

	return nil
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
			mongoFiles = append(mongoFiles, &MongoFile{
				Name:     file.Name,
				Uri:      file.Uri,
				MimeType: file.MimeType,
				Tags:     file.Tags,
			})
		} else {
			mongoFiles = append(mongoFiles, &MongoFile{
				ID:       id,
				Name:     file.Name,
				Uri:      file.Uri,
				MimeType: file.MimeType,
				Tags:     file.Tags,
			})
		}

	}
	return mongoFiles
}
