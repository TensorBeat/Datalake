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
	for _, repoFile := range repoFiles {
		files = append(files, &proto.File{
			Id:       repoFile.ID,
			Name:     repoFile.Name,
			Uri:      repoFile.Uri,
			MimeType: repoFile.MimeType,
			Tags:     repoFile.Tags,
		})
	}
	return files
}

func (s *DatalakeServiceServer) ProtoFilesToRepoFiles(protoFiles []*proto.File) []*repository.File {
	files := make([]*repository.File, len(protoFiles))
	for _, protoFile := range protoFiles {
		files = append(files, &repository.File{
			ID:       protoFile.Id,
			Name:     protoFile.Name,
			Uri:      protoFile.Uri,
			MimeType: protoFile.MimeType,
			Tags:     protoFile.Tags,
		})
	}
	return files
}

func (s *DatalakeServiceServer) ProtoAddFilesToRepoFiles(protoFiles []*proto.AddFile) []*repository.File {
	files := make([]*repository.File, len(protoFiles))
	for _, protoFile := range protoFiles {
		files = append(files, &repository.File{
			Name:     protoFile.Name,
			Uri:      protoFile.Uri,
			MimeType: protoFile.MimeType,
			Tags:     protoFile.Tags,
		})
	}
	return files
}

func (s *DatalakeServiceServer) GetSongs(ctx context.Context, req *proto.GetSongsRequest) (*proto.GetSongsResponse, error) {

	var songs []*repository.File
	var err error

	if len(req.Tags) > 0 {
		songs, err = s.repo.GetSongsByTags(ctx, req.Tags, req.Operator)
	} else {
		songs, err = s.repo.GetSongs(ctx)
	}

	if err != nil {
		s.logger.Errorf("Failed to get songs: %v", err)
		return nil, err
	}

	res := &proto.GetSongsResponse{
		Songs: s.RepoFilesToProtoFiles(songs),
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
	panic("not implemented") // TODO: Implement
}

func (s *DatalakeServiceServer) RemoveTags(ctx context.Context, req *proto.RemoveTagsRequest) (*proto.RemoveTagsResponse, error) {
	panic("not implemented") // TODO: Implement
}
