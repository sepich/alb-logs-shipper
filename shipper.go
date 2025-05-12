package main

import (
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type Shipper struct {
	opts     Options
	elbMeta  *ELBMeta
	s3Client *s3.Client
	logger   log.Logger
}

func NewShipper(opts Options, elbMeta *ELBMeta, s3Client *s3.Client, logger log.Logger) *Shipper {
	return &Shipper{
		opts:     opts,
		elbMeta:  elbMeta,
		s3Client: s3Client,
		logger:   logger,
	}
}

func (s *Shipper) run() error {
	level.Info(s.logger).Log("msg", "Starting run")

	return nil
}
