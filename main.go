package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type Options struct {
	BucketName   string
	WaitInterval time.Duration
	LokiURL      string
	LokiUser     string
	LokiPassword string
}

func main() {
	var opts Options
	var LogLevel string
	flag.StringVar(&opts.BucketName, "bucket-name", "", "Name of the S3 bucket with ALB logs (required)")
	flag.DurationVar(&opts.WaitInterval, "wait", 60*time.Second, "Interval to wait between runs")
	flag.StringVar(&opts.LokiURL, "loki-url", "", "URL to Loki API (required)")
	flag.StringVar(&opts.LokiUser, "loki-user", "", "User to use for Loki authentication")
	flag.StringVar(&LogLevel, "log-level", "info", "Log level (info, debug)")
	flag.Parse()

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = level.NewFilter(logger, level.Allow(level.ParseDefault(LogLevel, level.InfoValue())))
	logger = log.With(logger, "caller", log.DefaultCaller)

	if opts.BucketName == "" {
		level.Error(logger).Log("msg", "--bucket-name is required")
		flag.Usage()
		os.Exit(1)
	}

	if opts.LokiURL == "" {
		level.Error(logger).Log("msg", "--loki-url is required")
		flag.Usage()
		os.Exit(1)
	}

	if os.Getenv("LOKI_PASSWORD") == "" {
		level.Error(logger).Log("msg", "LOKI_PASSWORD environment variable is required")
		os.Exit(1)
	}
	opts.LokiPassword = os.Getenv("LOKI_PASSWORD")

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		level.Error(logger).Log("msg", "unable to load AWS SDK config", "err", err)
		os.Exit(1)
	}

	s3Client := s3.NewFromConfig(cfg)
	elbClient := elasticloadbalancingv2.NewFromConfig(cfg)
	elbMeta := NewELBMeta(elbClient)
	shipper := NewShipper(opts, elbMeta, s3Client, logger)

	sgnl := make(chan os.Signal, 1)
	signal.Notify(sgnl, syscall.SIGINT, syscall.SIGTERM)
	waitTimer := time.NewTimer(0)

	//go func() {
	for {
		select {
		case <-waitTimer.C:
			if err := shipper.run(); err != nil {
				level.Error(logger).Log("msg", "run failed", "err", err)
				os.Exit(1)
			}
			waitTimer.Reset(opts.WaitInterval)
		case <-sgnl:
			level.Info(logger).Log("msg", "Received SIGINT or SIGTERM. Shutting down")
			os.Exit(0)
		}
	}
	//}()
	// metrics server
}
