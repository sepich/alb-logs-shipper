package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// LineParser defines the interface for converting log lines to different formats
type LineParser interface {
	As(format, line string) (*time.Time, string, error)
}

// Cache the subexp names to avoid repeated calls
var subexpNames = evRegex.SubexpNames()[1:]

type LineRegex struct{}

var _ LineParser = &LineRegex{}

// As parses log line via regex and converts it to the specified format
func (r *LineRegex) As(format, line string) (*time.Time, string, error) {
	matches := evRegex.FindStringSubmatch(line)
	if len(matches) == 0 {
		return nil, "", fmt.Errorf("failed to parse log line: %s", line)
	}
	return LineAs(format, line, matches[1:])
}

type LineSlice struct{}

var _ LineParser = &LineSlice{}

// As parses log line by slice and converts it to the specified format
func (r *LineSlice) As(format, line string) (*time.Time, string, error) {
	matches := []string{}
	start := 0
	end := 0
	for _, name := range subexpNames {
		if start >= len(line) {
			return nil, "", fmt.Errorf("failed to parse log line: %s", line)
		}
		for end = start + 1; end < len(line); end++ {
			if line[end] == ' ' {
				if !quoteFields[name] || (line[end-1] == '"' && line[end-2] != '\\') {
					break
				}
			}
		}
		matches = append(matches, line[start:end])
		start = end + 1
	}
	return LineAs(format, line, matches)
}

func LineAs(format, line string, matches []string) (*time.Time, string, error) {
	var builder strings.Builder
	builder.Grow(1024) // Preallocate builder with estimated capacity

	var ts time.Time
	var err error
	isFirst := true
	isJSON := format == "json"
	if isJSON {
		builder.WriteByte('{')
	}

	for i, name := range subexpNames {
		if skipFields[name] {
			continue // drop non relevant for EKS ALB
		}

		value := matches[i]
		if name == "time" {
			if ts, err = time.Parse(time.RFC3339, value); err != nil {
				return nil, "", fmt.Errorf("skipping log line with invalid timestamp %w: %s", err, line)
			}
		}

		// separator
		if !isFirst {
			if isJSON {
				builder.WriteByte(',')
			} else {
				builder.WriteByte(' ')
			}
		}
		isFirst = false

		// unescape
		if quoteFields[name] {
			s, err := strconv.Unquote(value) // `\x5C` to `"`
			if err == nil {
				value = strconv.Quote(s)
			}
		}
		if isJSON {
			builder.WriteString(`"` + name + `":`)
			if numFields[name] || quoteFields[name] {
				builder.WriteString(value)
			} else {
				builder.WriteString(`"` + value + `"`)
			}
		} else {
			builder.WriteString(name + "=" + value)
		}
	}

	if isJSON {
		builder.WriteByte('}')
	}
	return &ts, builder.String(), nil
}
