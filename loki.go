package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/golang/snappy"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/loki/v3/pkg/logproto"
)

const (
	timeout    = 11 * time.Second // 10s on loki side
	minBackoff = 100 * time.Millisecond
	maxBackoff = 30 * time.Second
	maxRetries = 10
)

type batch struct {
	stream *logproto.Stream
	lines  int
	client *lokiClient
}

func newBatch(labels map[string]string, opts Options, logger *slog.Logger) *batch {
	ls := make([]string, 0, len(labels))
	for l, v := range labels {
		ls = append(ls, fmt.Sprintf("%s=%q", l, v))
	}
	sort.Strings(ls)
	return &batch{
		stream: &logproto.Stream{
			Labels: fmt.Sprintf("{%s}", strings.Join(ls, ", ")),
		},
		client: newLokiClient(opts.LokiURL, opts.LokiUser, opts.LokiPassword, logger),
	}
}

func (b *batch) add(ts time.Time, line string) error {
	b.stream.Entries = append(b.stream.Entries, logproto.Entry{
		Timestamp: ts,
		Line:      line,
	})
	b.lines++
	if b.lines >= 100 {
		return b.flush()
	}
	return nil
}

func (b *batch) flush() error {
	if b.lines == 0 {
		return nil
	}

	buf, err := b.encode()
	if err != nil {
		return err
	}
	if err = b.client.send(buf); err != nil {
		return err
	}

	b.lines = 0
	b.stream.Entries = b.stream.Entries[:0]
	return nil
}

func (b *batch) encode() ([]byte, error) {
	req := logproto.PushRequest{
		Streams: []logproto.Stream{*b.stream},
	}
	buf, err := proto.Marshal(&req)
	if err != nil {
		return nil, err
	}

	return snappy.Encode(nil, buf), nil
}

type lokiClient struct {
	http         *http.Client
	logger       *slog.Logger
	LokiURL      string
	LokiUser     string
	LokiPassword string
}

func newLokiClient(lokiURL, lokiUser, lokiPassword string, logger *slog.Logger) *lokiClient {
	return &lokiClient{
		http:         &http.Client{},
		logger:       logger,
		LokiURL:      lokiURL,
		LokiUser:     lokiUser,
		LokiPassword: lokiPassword,
	}
}

func (c *lokiClient) send(buf []byte) error {
	backoff := backoff.New(context.Background(), backoff.Config{
		MinBackoff: minBackoff,
		MaxBackoff: maxBackoff,
		MaxRetries: maxRetries,
	})
	var status int
	var err error
	for {
		status, err = c.req(buf)

		// Only retry 429s, 5xx, and connection-level errors.
		if status > 0 && status != 429 && status/100 != 5 {
			break
		}
		c.logger.Error("error sending batch, will retry", "status", status, "err", err)
		backoff.Wait()

		// Make sure it sends at least once before checking for retry.
		if !backoff.Ongoing() {
			break
		}
	}

	return err
}

func (c *lokiClient) req(buf []byte) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequest("POST", c.LokiURL, bytes.NewReader(buf))
	if err != nil {
		return -1, err
	}
	// snappy-encoded protobufs over http by default.
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("User-Agent", "alb-logs-shipper")

	if c.LokiUser != "" && c.LokiPassword != "" {
		req.SetBasicAuth(c.LokiUser, c.LokiPassword)
	}

	resp, err := c.http.Do(req.WithContext(ctx))
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(resp.Body, 1024))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = fmt.Errorf("server returned HTTP status %s (%d): %s", resp.Status, resp.StatusCode, line)
	}

	return resp.StatusCode, err
}
