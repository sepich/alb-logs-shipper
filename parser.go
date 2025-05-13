package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

var (
	// source:  https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-file-format
	// format:  bucket[/prefix]/AWSLogs/aws-account-id/elasticloadbalancing/region/yyyy/mm/dd/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_end-time_ip-address_random-string.log.gz
	// example: my-bucket/AWSLogs/123456789012/elasticloadbalancing/us-east-1/2022/01/24/123456789012_elasticloadbalancing_us-east-1_app.my-loadbalancer.b13ea9d19f16d015_20220124T0000Z_0.0.0.0_2et2e1mx.log.gz
	fnRegex = regexp.MustCompile(`AWSLogs\/(?P<account_id>\d+)\/elasticloadbalancing\/(?P<region>[\w-]+)\/(?P<year>\d+)\/(?P<month>\d+)\/(?P<day>\d+)\/\d+\_elasticloadbalancing_(?:\w+-\w+-(?:\w+-)?\d)_app\.(?P<id>[a-zA-Z0-9\-]+)\..+\.log\.gz`)
	tsRegex = regexp.MustCompile(`(?P<timestamp>\d+-\d+-\d+T\d+:\d+:\d+(?:\.\d+Z)?)`)
)

type Parser struct {
	opts     Options
	elbMeta  *ELBMeta
	s3Client *s3.Client
	logger   log.Logger
}

func NewParser(opts Options, elbMeta *ELBMeta, s3Client *s3.Client, logger log.Logger) *Parser {
	return &Parser{
		opts:     opts,
		elbMeta:  elbMeta,
		s3Client: s3Client,
		logger:   logger,
	}
}

func (s *Parser) run() error {
	level.Info(s.logger).Log("msg", "Starting run")

	ctx := context.Background()
	maxKeys := int32(1000) //no pager, tune interval to have less files per run
	output, err := s.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  &s.opts.BucketName,
		MaxKeys: &maxKeys,
	})
	if err != nil {
		return err
	}

	for _, obj := range output.Contents {
		if obj.Key == nil {
			continue
		}
		matches := fnRegex.FindStringSubmatch(*obj.Key)
		if len(matches) == 0 {
			level.Debug(s.logger).Log("msg", "skipping non-alb log file", "key", *obj.Key)
			continue
		}
		if err := s.parseFile(ctx, *obj.Key, matches[fnRegex.SubexpIndex("id")]); err != nil {
			level.Error(s.logger).Log("msg", "failed to parse file", "key", *obj.Key, "err", err)
			continue
		}

		if _, err := s.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &s.opts.BucketName,
			Key:    obj.Key,
		}); err != nil {
			level.Error(s.logger).Log("msg", "failed to delete file", "key", *obj.Key, "err", err)
			continue
		}
	}

	return nil
}

func (s *Parser) parseFile(ctx context.Context, fn string, lb string) error {
	meta, err := s.elbMeta.Get(lb)
	if err != nil {
		return fmt.Errorf("failed to get metadata for load balancer %s: %w", lb, err)
	}
	level.Debug(s.logger).Log("msg", "processing log file", "key", fn, "namespace", meta.Namespace, "ingress", meta.Ingress)
	labels := make(map[string]string)
	labels["namespace"] = meta.Namespace
	labels["ingress"] = meta.Ingress
	labels["stream"] = "alb"
	for k, v := range s.opts.Labels {
		labels[k] = v
		if k == "cluster" {
			labels["index"] = v + "-" + meta.Namespace
		}
	}
	obj, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.opts.BucketName,
		Key:    &fn,
	})
	if err != nil {
		return fmt.Errorf("failed to get object %s: %w", fn, err)
	}
	defer obj.Body.Close()

	gzreader, err := gzip.NewReader(obj.Body)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzreader.Close()

	var lineCount int
	scanner := bufio.NewScanner(gzreader)
	for scanner.Scan() {
		lineCount++
		logLine := scanner.Text()
		level.Debug(s.logger).Log("msg", logLine)

		matches := tsRegex.FindStringSubmatch(logLine)
		if len(matches) == 0 {
			level.Error(s.logger).Log("msg", "skipping log line without a timestamp", "line", logLine)
			continue
		}
		timestamp, err := time.Parse(time.RFC3339, matches[1])
		if err != nil {
			level.Error(s.logger).Log("msg", "skipping log line with invalid timestamp", "line", logLine, "err", err)
			continue
		}
		level.Debug(s.logger).Log("msg", "processing log line", "timestamp", timestamp)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to scan file %s: %w", fn, err)
	}
	os.Exit(0)
	return nil
}
