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

func (s *DatalakeServiceServer) GetSongs(ctx context.Context, req *proto.GetSongsRequest) (*proto.GetSongsResponse, error) {

	var songs []*proto.File
	var err error

	if len(req.Metadata) > 0 {
		songs, err = s.repo.GetSongsByMetadata(ctx, req.Metadata)
	} else {
		songs, err = s.repo.GetSongs(ctx)
	}

	if err != nil {
		s.logger.Errorf("Failed to get songs: %v", err)
		return nil, err
	}

	res := &proto.GetSongsResponse{
		Songs: songs,
	}
	return res, nil
}

func (s *DatalakeServiceServer) AddSongs(ctx context.Context, req *proto.AddSongsRequest) (*proto.AddSongsResponse, error) {
	err := s.repo.AddSongs(ctx, req.Songs)

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
