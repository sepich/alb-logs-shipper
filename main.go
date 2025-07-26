package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/common/version"
	"github.com/spf13/pflag"
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

func main() {
	var opts Options
	opts.Labels = make(map[string]string)
	pflag.StringVarP(&opts.BucketName, "bucket-name", "b", "", "Name of the S3 bucket with ALB logs (required)")
	pflag.DurationVarP(&opts.WaitInterval, "wait", "w", 60*time.Second, "Interval to wait between runs")
	pflag.StringVarP(&opts.LokiURL, "loki-url", "H", "", "URL to Loki API (required)")
	pflag.StringVarP(&opts.LokiUser, "loki-user", "u", "", "User to use for Loki authentication")
	var logLevel = pflag.StringP("log-level", "", "info", "Log level (info, debug)")
	pflag.StringVarP(&opts.Format, "format", "o", "raw", "Format to parse and ship log lines as (logfmt, json, raw)")
	var labels = pflag.StringArrayP("label", "l", []string{}, "Label to add to Loki stream, can be specified multiple times (key=value)")
	var roles = pflag.StringArrayP("role-arn", "a", []string{}, "ARN of the IAM role to assume to access ALB tags, can be specified multiple times")
	pflag.IntVarP(&opts.Workers, "workers", "n", 4, "Number of workers to run")
	pflag.IntVarP(&opts.Port, "port", "p", 8080, "Port to expose metrics on")
	var ver = pflag.BoolP("version", "v", false, "Show version and exit")
	pflag.Parse()
	if *ver {
		fmt.Println(version.Print("alb-logs-shipper"))
		os.Exit(0)
	}
	logger := getLogger(*logLevel)

	if opts.BucketName == "" {
		logger.Error("--bucket-name is required")
		os.Exit(1)
	}

	if opts.LokiURL == "" {
		logger.Error("--loki-url is required")
		os.Exit(1)
	}

	if opts.LokiUser != "" && os.Getenv("LOKI_PASSWORD") == "" {
		logger.Error("LOKI_PASSWORD environment variable is required")
		os.Exit(1)
	}
	opts.LokiPassword = os.Getenv("LOKI_PASSWORD")

	for _, label := range *labels {
		parts := strings.SplitN(label, "=", 2)
		if len(parts) < 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
			logger.Error("invalid label format (k=v)", "label", label)
			os.Exit(1)
		}
		opts.Labels[parts[0]] = parts[1]
	}

	roleMap := make(map[string]string)
	for _, role := range *roles {
		id := strings.Split(role, ":")
		if len(id) != 6 {
			logger.Error("invalid role ARN", "role", role)
			os.Exit(1)
		}
		roleMap[id[4]] = role
	}

	logger.Info("Starting alb-logs-shipper", "version", version.Version, "metrics-port", opts.Port)
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		logger.Error("unable to load AWS SDK config", "err", err)
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
					logger.Error("scan S3 failed", "err", err)
					parser.Stop()
					return
				}
			case <-sgnl:
				logger.Info("received SIGINT or SIGTERM, shutting down...")
				parser.Stop()
				return
			}
		}
	}()

	go func() {
		http.Handle("/metrics", parser.metrics())
		if err := http.ListenAndServe(fmt.Sprintf(":%d", opts.Port), nil); err != nil {
			logger.Error("metrics server failed", "err", err)
			parser.Stop()
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := parser.worker(); err != nil {
				parser.Stop() // pod restart instead of deletion of not-shipped file
			}
		}()
	}
	wg.Wait()
}

func getLogger(logLevel string) *slog.Logger {
	var l = slog.LevelInfo
	if logLevel == "debug" {
		l = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:     l,
		AddSource: logLevel == "debug",
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && len(groups) == 0 {
				return slog.Attr{}
			}
			if a.Key == slog.SourceKey {
				s := a.Value.String()
				i := strings.LastIndex(s, "/")
				j := strings.LastIndex(s, " ")
				a.Value = slog.StringValue(s[i+1:j] + ":" + s[j+1:len(s)-1])
			}
			if a.Key == slog.LevelKey {
				a.Value = slog.StringValue(strings.ToLower(a.Value.String()))
			}
			return a
		},
	}))
}
