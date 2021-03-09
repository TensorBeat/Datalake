package controller

import (
	"context"

	"github.com/TensorBeat/Datalake/internal/repository"
	"github.com/TensorBeat/Datalake/pkg/proto"
	"go.uber.org/zap"
)

type DatalakeServiceServer struct {
	repo   repository.Repository
	logger *zap.SugaredLogger
	proto.UnimplementedDatalakeServiceServer
}

func NewDatalakeServiceServer(repo repository.Repository, logger *zap.SugaredLogger) *DatalakeServiceServer {
	return &DatalakeServiceServer{
		repo:   repo,
		logger: logger,
	}
}

func (s *DatalakeServiceServer) RepoFilesToProtoFiles(repoFiles []*repository.File) []*proto.File {
	files := make([]*proto.File, len(repoFiles))
	for i, repoFile := range repoFiles {
		files[i] = &proto.File{
			Id:       repoFile.ID,
			Name:     repoFile.Name,
			Uri:      repoFile.Uri,
			MimeType: repoFile.MimeType,
			Tags:     repoFile.Tags,
		}
	}
	return files
}

func (s *DatalakeServiceServer) ProtoFilesToRepoFiles(protoFiles []*proto.File) []*repository.File {
	files := make([]*repository.File, len(protoFiles))
	for i, protoFile := range protoFiles {
		files[i] = &repository.File{
			ID:       protoFile.Id,
			Name:     protoFile.Name,
			Uri:      protoFile.Uri,
			MimeType: protoFile.MimeType,
			Tags:     protoFile.Tags,
		}
	}
	return files
}

func (s *DatalakeServiceServer) ProtoAddFilesToRepoFiles(protoFiles []*proto.AddFile) []*repository.File {
	files := make([]*repository.File, len(protoFiles))
	for i, protoFile := range protoFiles {
		files[i] = &repository.File{
			Name:     protoFile.Name,
			Uri:      protoFile.Uri,
			MimeType: protoFile.MimeType,
			Tags:     protoFile.Tags,
		}
	}
	return files
}

func (s *DatalakeServiceServer) GetAllSongs(ctx context.Context, req *proto.GetAllSongsRequest) (*proto.GetAllSongsResponse, error) {

	var songs []*repository.File
	var nextToken int64
	var err error

	songs, nextToken, err = s.repo.GetAllSongs(ctx, *req.PageToken, *req.PageSize)

	if err != nil {
		s.logger.Errorf("Failed to get songs: %v", err)
		return nil, err
	}

	res := &proto.GetAllSongsResponse{
		Songs:     s.RepoFilesToProtoFiles(songs),
		NextToken: nextToken,
	}
	return res, nil
}

func (s *DatalakeServiceServer) GetSongsByIDs(ctx context.Context, req *proto.GetSongsByIDsRequest) (*proto.GetSongsByIDsResponse, error) {
	var songs []*repository.File
	var nextToken int64
	var err error

	songs, nextToken, err = s.repo.GetSongsByIDs(ctx, req.Ids, *req.PageToken, *req.PageSize)

	if err != nil {
		s.logger.Errorf("Failed to get songs: %v", err)
		return nil, err
	}

	res := &proto.GetSongsByIDsResponse{
		Songs:     s.RepoFilesToProtoFiles(songs),
		NextToken: nextToken,
	}
	return res, nil
}

func (s *DatalakeServiceServer) GetSongsByTags(ctx context.Context, req *proto.GetSongsByTagsRequest) (*proto.GetSongsByTagsResponse, error) {

	var songs []*repository.File
	var nextToken int64
	var err error

	songs, nextToken, err = s.repo.GetSongsByTags(ctx, req.Tags, req.Filter, *req.PageToken, *req.PageSize)

	if err != nil {
		s.logger.Errorf("Failed to get songs: %v", err)
		return nil, err
	}

	res := &proto.GetSongsByTagsResponse{
		Songs:     s.RepoFilesToProtoFiles(songs),
		NextToken: nextToken,
	}
	return res, nil
}

func (s *DatalakeServiceServer) AddSongs(ctx context.Context, req *proto.AddSongsRequest) (*proto.AddSongsResponse, error) {

	songs := s.ProtoAddFilesToRepoFiles(req.Songs)

	err := s.repo.AddSongs(ctx, songs)

	if err != nil {
		s.logger.Errorf("Failed to add songs: %v", err)
		res := &proto.AddSongsResponse{
			Successful: false,
		}
		return res, err
	}

	res := &proto.AddSongsResponse{
		Successful: true,
	}
	return res, nil
}

func (s *DatalakeServiceServer) AddTags(ctx context.Context, req *proto.AddTagsRequest) (*proto.AddTagsResponse, error) {

	err := s.repo.AddTags(ctx, req.Id, req.Tags)

	if err != nil {
		s.logger.Errorf("Failed to add tags: %v", err)
		res := &proto.AddTagsResponse{
			Successful: false,
		}
		return res, err
	}

	res := &proto.AddTagsResponse{
		Successful: true,
	}
	return res, nil
}

func (s *DatalakeServiceServer) RemoveTags(ctx context.Context, req *proto.RemoveTagsRequest) (*proto.RemoveTagsResponse, error) {
	err := s.repo.RemoveTags(ctx, req.Id, req.Tags)

	if err != nil {
		s.logger.Errorf("Failed to remove tags: %v", err)
		res := &proto.RemoveTagsResponse{
			Successful: false,
		}
		return res, err
	}

	res := &proto.RemoveTagsResponse{
		Successful: true,
	}
	return res, nil
}
