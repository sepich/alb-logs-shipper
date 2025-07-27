package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	// source:  https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-file-format
	// format:  bucket[/prefix]/AWSLogs/aws-account-id/elasticloadbalancing/region/yyyy/mm/dd/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_end-time_ip-address_random-string.log.gz
	// example: my-bucket/AWSLogs/123456789012/elasticloadbalancing/us-east-1/2022/01/24/123456789012_elasticloadbalancing_us-east-1_app.my-loadbalancer.b13ea9d19f16d015_20220124T0000Z_0.0.0.0_2et2e1mx.log.gz
	fnRegex    = regexp.MustCompile(`AWSLogs\/(?P<account_id>\d+)\/elasticloadbalancing\/(?P<region>[\w-]+)\/(?P<year>\d+)\/(?P<month>\d+)\/(?P<day>\d+)\/\d+\_elasticloadbalancing_(?:\w+-\w+-(?:\w+-)?\d)_app\.(?P<id>[a-zA-Z0-9\-]+)\..+\.log\.gz`)
	tsRegex    = regexp.MustCompile(`(?P<timestamp>\d+-\d+-\d+T\d+:\d+:\d+(?:\.\d+Z)?)`)
	evRegex    = regexp.MustCompile(`(?P<type>\S+) (?P<time>\S+) (?P<elb>\S+) (?P<client>\S+) (?P<target>\S+) (?P<request_processing_time>\S+) (?P<target_processing_time>\S+) (?P<response_processing_time>\S+) (?P<elb_status_code>\S+) (?P<target_status_code>\S+) (?P<received_bytes>\S+) (?P<sent_bytes>\S+) (?P<request>".+") (?P<user_agent>".*") (?P<ssl_cipher>\S+) (?P<ssl_protocol>\S+) (?P<target_group_arn>\S+) (?P<trace_id>".+") (?P<domain_name>".+") (?P<chosen_cert_arn>".+") (?P<matched_rule_priority>\S+) (?P<request_creation_time>\S+) (?P<actions_executed>".+") (?P<redirect_url>".+") (?P<error_reason>".+") (?P<targets>".+") (?P<target_status_code_list>".+") (?P<classification>".+") (?P<classification_reason>".+") (?P<conn_trace_id>\S+)`)
	skipFields = map[string]bool{
		"chosen_cert_arn":         true, // hardcoded in ingress
		"target_group_arn":        true, // not configured directly
		"matched_rule_priority":   true, // not configured directly
		"error_reason":            true, // only for lambda
		"targets":                 true, // same as target
		"target_status_code_list": true, // same as target_status_code
		"classification":          true, // not used
		"classification_reason":   true, // not used
		"conn_trace_id":           true, // only for connection logs
	}
	quoteFields = map[string]bool{
		"request":                 true,
		"user_agent":              true,
		"trace_id":                true,
		"domain_name":             true,
		"chosen_cert_arn":         true,
		"actions_executed":        true,
		"redirect_url":            true,
		"error_reason":            true,
		"targets":                 true,
		"target_status_code_list": true,
		"classification":          true,
		"classification_reason":   true,
	}
	numFields = map[string]bool{
		"elb_status_code":          true,
		"received_bytes":           true,
		"request_processing_time":  true,
		"response_processing_time": true,
		"sent_bytes":               true,
		"target_processing_time":   true,
		"target_status_code":       true,
	}
)

type Parser struct {
	opts     Options
	elbMeta  *ELBMeta
	s3Client *s3.Client
	logger   *slog.Logger
	queue    chan *string
	stop     bool
	line     LineParser
}

func NewParser(opts Options, elbMeta *ELBMeta, s3Client *s3.Client, logger *slog.Logger) *Parser {
	parser := &Parser{
		opts:     opts,
		elbMeta:  elbMeta,
		s3Client: s3Client,
		logger:   logger,
		queue:    make(chan *string, 10*opts.Workers),
		line:     &LineSlice{},
	}
	return parser
}

// Stop gracefully all workers
func (s *Parser) Stop() {
	if s.stop {
		return
	}
	s.stop = true
	close(s.queue)
}

func (s *Parser) scan() error {
	num := 0
	ctx := context.Background()
	maxKeys := int32(1000) //no pager, tune interval to have less files per run
	output, err := s.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  &s.opts.BucketName,
		MaxKeys: &maxKeys,
	})
	if err != nil {
		return err
	}

	start := time.Now()
	for _, obj := range output.Contents {
		if obj.Key == nil || s.stop {
			continue
		}
		s.queue <- obj.Key
		num++
	}
	if num > 0 {
		s.logger.Info("new files", "found", num, "time", time.Since(start), "queue", len(s.queue))
	}
	return nil
}

func (s *Parser) worker() error {
	ctx := context.Background() // limit time to process file? will restart of processing help?

	for fn := range s.queue {
		matches := fnRegex.FindStringSubmatch(*fn)
		if len(matches) == 0 {
			s.logger.Debug("skipping non-alb log file", "key", *fn)
			continue
		}
		if err := s.parseFile(ctx, *fn, matches[fnRegex.SubexpIndex("account_id")], matches[fnRegex.SubexpIndex("id")]); err != nil {
			s.logger.Error("failed to ship file", "key", *fn, "err", err)
			return err // pod restart instead of deletion of not-shipped file
		}

		if _, err := s.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &s.opts.BucketName,
			Key:    fn,
		}); err != nil {
			s.logger.Error("failed to delete file", "key", *fn, "err", err)
		}
	}
	return nil
}

func (s *Parser) parseFile(ctx context.Context, fn string, accountID, lb string) error {
	start := time.Now()
	meta, err := s.elbMeta.Get(accountID, lb)
	if err != nil {
		return fmt.Errorf("failed to get metadata for load balancer %s/%s: %w", accountID, lb, err)
	}
	labels := map[string]string{
		"namespace": meta.Namespace,
		"ingress":   meta.Ingress,
	}
	if meta.Cluster != "" {
		labels["cluster"] = meta.Cluster
		labels["index"] = meta.Cluster + "-" + meta.Namespace
	}
	for k, v := range s.opts.Labels {
		labels[k] = v
	}
	b := newBatch(labels, s.opts, s.logger)

	obj, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.opts.BucketName,
		Key:    &fn,
	})
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			s.logger.Debug("skipping non-existent file", "key", fn)
			return nil
		}
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
		ts, logLine, err := s.line.As(s.opts.Format, scanner.Text())
		if err != nil {
			return err
		}
		if err = b.add(*ts, logLine); err != nil {
			return fmt.Errorf("failed to send batch: %w", err)
		}
	}
	if err = scanner.Err(); err != nil {
		return fmt.Errorf("failed to scan file %s: %w", fn, err)
	}
	if err = b.flush(); err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}
	s.logger.Debug("shipped file", "key", fn, "labels", fmt.Sprintf("%v", labels), "lines", lineCount, "time", time.Since(start), "lines/s", fmt.Sprintf("%.2f", float64(lineCount)/time.Since(start).Seconds()))
	return nil
}

func (s *Parser) metrics() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "alb_logs_shipper_queue_length %d\n", len(s.queue))
	})
}
