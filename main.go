package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/version"
)

type Options struct {
	BucketName   string
	WaitInterval time.Duration
	Format       string
	LokiURL      string
	LokiUser     string
	LokiPassword string
	Labels       map[string]string
	Workers      int
	Port         int
}

// stringSliceFlag implements flag.Value
type stringSliceFlag []string

func (s *stringSliceFlag) String() string {
	return fmt.Sprintf("%v", *s)
}

func (s *stringSliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	var opts Options
	var logLevel string
	var labels stringSliceFlag
	var roles stringSliceFlag
	var ver bool
	opts.Labels = make(map[string]string)
	flag.StringVar(&opts.BucketName, "bucket-name", "", "Name of the S3 bucket with ALB logs (required)")
	flag.DurationVar(&opts.WaitInterval, "wait", 60*time.Second, "Interval to wait between runs")
	flag.StringVar(&opts.LokiURL, "loki-url", "", "URL to Loki API (required)")
	flag.StringVar(&opts.LokiUser, "loki-user", "", "User to use for Loki authentication")
	flag.StringVar(&logLevel, "log-level", "info", "Log level (info, debug)")
	flag.StringVar(&opts.Format, "format", "raw", "Format to parse and ship log lines as (logfmt, json, raw)")
	flag.Var(&labels, "label", "Label to add to Loki stream, can be specified multiple times (key=value)")
	flag.Var(&roles, "role-arn", "ARN of the IAM role to assume to access ALB tags, can be specified multiple times")
	flag.IntVar(&opts.Workers, "workers", 4, "Number of workers to run")
	flag.IntVar(&opts.Port, "port", 8080, "Port to expose metrics on")
	flag.BoolVar(&ver, "version", false, "Show version and exit")
	flag.Parse()
	if ver {
		fmt.Println(version.Print("alb-logs-shipper"))
		os.Exit(0)
	}

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = level.NewFilter(logger, level.Allow(level.ParseDefault(logLevel, level.InfoValue())))
	logger = log.With(logger, "caller", log.DefaultCaller)

	if opts.BucketName == "" {
		level.Error(logger).Log("msg", "--bucket-name is required")
		os.Exit(1)
	}

	if opts.LokiURL == "" {
		level.Error(logger).Log("msg", "--loki-url is required")
		os.Exit(1)
	}

	if os.Getenv("LOKI_PASSWORD") == "" {
		level.Error(logger).Log("msg", "LOKI_PASSWORD environment variable is required")
		os.Exit(1)
	}
	opts.LokiPassword = os.Getenv("LOKI_PASSWORD")

	for _, label := range labels {
		parts := strings.SplitN(label, "=", 2)
		if len(parts) < 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
			level.Error(logger).Log("msg", "invalid label format (k=v)", "label", label)
			os.Exit(1)
		}
		opts.Labels[parts[0]] = parts[1]
	}

	roleMap := make(map[string]string)
	for _, role := range roles {
		id := strings.Split(role, ":")
		if len(id) != 6 {
			level.Error(logger).Log("msg", "invalid role ARN", "role", role)
			os.Exit(1)
		}
		roleMap[id[4]] = role
	}

	level.Info(logger).Log("msg", "Starting alb-logs-shipper", "version", version.Version, "metrics-port", opts.Port)
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		level.Error(logger).Log("msg", "unable to load AWS SDK config", "err", err)
		os.Exit(1)
	}

	s3Client := s3.NewFromConfig(cfg)
	elbMeta := NewELBMeta(roleMap)
	parser := NewParser(opts, elbMeta, s3Client, logger)

	sgnl := make(chan os.Signal, 1)
	signal.Notify(sgnl, syscall.SIGINT, syscall.SIGTERM)
	waitTimer := time.NewTimer(0)

	go func() {
		for {
			select {
			case <-waitTimer.C:
				waitTimer.Reset(opts.WaitInterval)
				if err := parser.scan(); err != nil {
					level.Error(logger).Log("msg", "scan S3 failed", "err", err)
					os.Exit(1)
				}
			case <-sgnl:
				level.Info(logger).Log("msg", "Received SIGINT or SIGTERM. Shutting down")
				os.Exit(0)
			}
		}
	}()

	http.Handle("/metrics", parser.metrics())
	if err := http.ListenAndServe(fmt.Sprintf(":%d", opts.Port), nil); err != nil {
		level.Error(logger).Log("msg", "metrics server failed", "err", err)
	}
}
