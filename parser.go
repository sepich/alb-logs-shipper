package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

var (
	// source:  https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-file-format
	// format:  bucket[/prefix]/AWSLogs/aws-account-id/elasticloadbalancing/region/yyyy/mm/dd/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_end-time_ip-address_random-string.log.gz
	// example: my-bucket/AWSLogs/123456789012/elasticloadbalancing/us-east-1/2022/01/24/123456789012_elasticloadbalancing_us-east-1_app.my-loadbalancer.b13ea9d19f16d015_20220124T0000Z_0.0.0.0_2et2e1mx.log.gz
	fnRegex    = regexp.MustCompile(`AWSLogs\/(?P<account_id>\d+)\/elasticloadbalancing\/(?P<region>[\w-]+)\/(?P<year>\d+)\/(?P<month>\d+)\/(?P<day>\d+)\/\d+\_elasticloadbalancing_(?:\w+-\w+-(?:\w+-)?\d)_app\.(?P<id>[a-zA-Z0-9\-]+)\..+\.log\.gz`)
	tsRegex    = regexp.MustCompile(`(?P<timestamp>\d+-\d+-\d+T\d+:\d+:\d+(?:\.\d+Z)?)`)
	evRegex    = regexp.MustCompile(`(?P<type>\S+) (?P<time>\S+) (?P<elb>\S+) (?P<client>\S+) (?P<target>\S+) (?P<request_processing_time>\S+) (?P<target_processing_time>\S+) (?P<response_processing_time>\S+) (?P<elb_status_code>\S+) (?P<target_status_code>\S+) (?P<received_bytes>\S+) (?P<sent_bytes>\S+) "(?P<request>.+)" "(?P<user_agent>.+)" (?P<ssl_cipher>\S+) (?P<ssl_protocol>\S+) (?P<target_group_arn>\S+) "(?P<trace_id>.+)" "(?P<domain_name>.+)" "(?P<chosen_cert_arn>.+)" (?P<matched_rule_priority>\S+) (?P<request_creation_time>\S+) "(?P<actions_executed>.+)" "(?P<redirect_url>.+)" "(?P<error_reason>.+)" "(?P<targets>.+)" "(?P<target_status_code_list>.+)" "(?P<classification>.+)" "(?P<classification_reason>.+)" (?P<conn_trace_id>\S+)`)
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

	for _, obj := range output.Contents {
		if obj.Key == nil {
			continue
		}
		matches := fnRegex.FindStringSubmatch(*obj.Key)
		if len(matches) == 0 {
			level.Debug(s.logger).Log("msg", "skipping non-alb log file", "key", *obj.Key)
			continue
		}
		if err := s.parseFile(ctx, *obj.Key, matches[fnRegex.SubexpIndex("account_id")], matches[fnRegex.SubexpIndex("id")]); err != nil {
			level.Error(s.logger).Log("msg", "failed to ship file", "key", *obj.Key, "err", err)
			os.Exit(1) // pod restart instead of deletion of not-shipped file
		}

		if _, err := s.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &s.opts.BucketName,
			Key:    obj.Key,
		}); err != nil {
			level.Error(s.logger).Log("msg", "failed to delete file", "key", *obj.Key, "err", err)
		}
		num++
	}
	if num > 0 {
		level.Info(s.logger).Log("msg", "shipped", "files", num)
	}

	return nil
}

func (s *Parser) parseFile(ctx context.Context, fn string, accountID, lb string) error {
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
	level.Debug(s.logger).Log("msg", "processing log file", "key", fn, "labels", fmt.Sprintf("%v", labels))
	b := newBatch(labels, s.opts, &s.logger)

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
		switch s.opts.Format {
		case "logfmt":
			logLine = toLogfmt(logLine)
		case "json":
			logLine = toJSON(logLine)
		}
		if err = b.add(timestamp, logLine); err != nil {
			return fmt.Errorf("failed to send batch: %w", err)
		}
	}
	if err = scanner.Err(); err != nil {
		return fmt.Errorf("failed to scan file %s: %w", fn, err)
	}
	if err = b.flush(); err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}
	return nil
}

// toLogfmt converts a ALB log line to logfmt format
func toLogfmt(line string) string {
	matches := evRegex.FindStringSubmatch(line)
	if len(matches) == 0 { // TODO
		fmt.Println("failed to parse log line:", line)
		os.Exit(1)
	}
	res := []string{}
	for i, name := range evRegex.SubexpNames()[1:] {
		if skipFields[name] {
			continue // drop non relevant for EKS ALB
		}
		if quoteFields[name] {
			res = append(res, fmt.Sprintf("%s=%q", name, matches[i+1]))
		} else {
			res = append(res, fmt.Sprintf("%s=%s", name, matches[i+1]))
		}
	}
	return strings.Join(res, " ")
}

// toJSON converts a ALB log line to JSON format
func toJSON(line string) string {
	matches := evRegex.FindStringSubmatch(line)
	if len(matches) == 0 { // TODO
		fmt.Println("failed to parse log line:", line)
		os.Exit(1)
	}
	res := []string{}
	for i, name := range evRegex.SubexpNames()[1:] {
		if skipFields[name] {
			continue // drop non relevant for EKS ALB
		}
		if numFields[name] {
			res = append(res, fmt.Sprintf("%q:%s", name, matches[i+1]))
		} else {
			res = append(res, fmt.Sprintf("%q:%q", name, matches[i+1]))
		}
	}

	return "{" + strings.Join(res, ",") + "}"
}
