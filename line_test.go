package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestLineParser_As(t *testing.T) {
	tests := []struct {
		name   string
		format string
		in     string
		out    string
		ts     time.Time
		err    bool
	}{
		{
			name:   "http logfmt",
			format: "logfmt",
			in:     `http 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.000 0.001 0.000 200 200 34 366 "GET http://www.example.com:80/ HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337262-36d228ad5d99923122bbe354" "-" "-" 0 2018-07-02T22:22:48.364000Z "forward" "-" "-" "10.0.0.1:80" "200" "-" "-" TID_1234abcd5678ef90`,
			ts:     time.Date(2018, time.July, 2, 22, 23, 0, 186641000, time.UTC),
			out:    `type=http time=2018-07-02T22:23:00.186641Z elb=app/my-loadbalancer/50dc6c495c0c9188 client=192.168.131.39:2817 target=10.0.0.1:80 request_processing_time=0.000 target_processing_time=0.001 response_processing_time=0.000 elb_status_code=200 target_status_code=200 received_bytes=34 sent_bytes=366 request="GET http://www.example.com:80/ HTTP/1.1" user_agent="curl/7.46.0" ssl_cipher=- ssl_protocol=- trace_id="Root=1-58337262-36d228ad5d99923122bbe354" domain_name="-" request_creation_time=2018-07-02T22:22:48.364000Z actions_executed="forward" redirect_url="-"`,
			err:    false,
		},
		{
			name:   "http json",
			format: "json",
			in:     `http 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.000 0.001 0.000 200 200 34 366 "GET http://www.example.com:80/ HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337262-36d228ad5d99923122bbe354" "-" "-" 0 2018-07-02T22:22:48.364000Z "forward" "-" "-" "10.0.0.1:80" "200" "-" "-" TID_1234abcd5678ef90`,
			ts:     time.Date(2018, time.July, 2, 22, 23, 0, 186641000, time.UTC),
			out:    `{"type":"http","time":"2018-07-02T22:23:00.186641Z","elb":"app/my-loadbalancer/50dc6c495c0c9188","client":"192.168.131.39:2817","target":"10.0.0.1:80","request_processing_time":0.000,"target_processing_time":0.001,"response_processing_time":0.000,"elb_status_code":200,"target_status_code":200,"received_bytes":34,"sent_bytes":366,"request":"GET http://www.example.com:80/ HTTP/1.1","user_agent":"curl/7.46.0","ssl_cipher":"-","ssl_protocol":"-","trace_id":"Root=1-58337262-36d228ad5d99923122bbe354","domain_name":"-","request_creation_time":"2018-07-02T22:22:48.364000Z","actions_executed":"forward","redirect_url":"-"}`,
			err:    false,
		},
		{
			name:   "http wrong ts",
			format: "logfmt",
			in:     `http 2018-07-02 22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.000 0.001 0.000 200 200 34 366 "GET http://www.example.com:80/ HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337262-36d228ad5d99923122bbe354" "-" "-" 0 2018-07-02T22:22:48.364000Z "forward" "-" "-" "10.0.0.1:80" "200" "-" "-" TID_1234abcd5678ef90`,
			ts:     time.Date(2018, time.July, 2, 22, 23, 0, 186641000, time.UTC),
			out:    `type=http time=2018-07-02T22:23:00.186641Z elb=app/my-loadbalancer/50dc6c495c0c9188 client=192.168.131.39:2817 target=10.0.0.1:80 request_processing_time=0.000 target_processing_time=0.001 response_processing_time=0.000 elb_status_code=200 target_status_code=200 received_bytes=34 sent_bytes=366 request="GET http://www.example.com:80/ HTTP/1.1" user_agent="curl/7.46.0" ssl_cipher=- ssl_protocol=- trace_id="Root=1-58337262-36d228ad5d99923122bbe354" domain_name="-" request_creation_time=2018-07-02T22:22:48.364000Z actions_executed="forward" redirect_url="-"`,
			err:    true,
		},
		{
			name:   "http2 quoted logfmt",
			format: "logfmt",
			in:     `h2 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 10.0.1.252:48160 10.0.0.66:9000 0.000 0.002 0.000 200 200 5 257 "GET https://10.0.2.105:773/ HTTP/2.0" "user\x22agent\x22" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337327-72bd00b0343d75b906739c42" "-" "-" 1 2018-07-02T22:22:48.364000Z "redirect" "https://example.com:80/" "-" "10.0.0.66:9000" "200" "-" "-" TID_1234abcd5678ef90`,
			ts:     time.Date(2018, time.July, 2, 22, 23, 0, 186641000, time.UTC),
			out:    `type=h2 time=2018-07-02T22:23:00.186641Z elb=app/my-loadbalancer/50dc6c495c0c9188 client=10.0.1.252:48160 target=10.0.0.66:9000 request_processing_time=0.000 target_processing_time=0.002 response_processing_time=0.000 elb_status_code=200 target_status_code=200 received_bytes=5 sent_bytes=257 request="GET https://10.0.2.105:773/ HTTP/2.0" user_agent="user\"agent\"" ssl_cipher=ECDHE-RSA-AES128-GCM-SHA256 ssl_protocol=TLSv1.2 trace_id="Root=1-58337327-72bd00b0343d75b906739c42" domain_name="-" request_creation_time=2018-07-02T22:22:48.364000Z actions_executed="redirect" redirect_url="https://example.com:80/"`,
			err:    false,
		},
		{
			name:   "http2 quoted json",
			format: "json",
			in:     `h2 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 10.0.1.252:48160 10.0.0.66:9000 0.000 0.002 0.000 200 200 5 257 "GET https://10.0.2.105:773/ HTTP/2.0" "user\x22agent\x22 UpstreamClient(Apache-HttpClient/5.0.3 \x5C(Java/21.0.4\x5C))" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337327-72bd00b0343d75b906739c42" "-" "-" 1 2018-07-02T22:22:48.364000Z "redirect" "https://example.com:80/" "-" "10.0.0.66:9000" "200" "-" "-" TID_1234abcd5678ef90`,
			ts:     time.Date(2018, time.July, 2, 22, 23, 0, 186641000, time.UTC),
			out:    `{"type":"h2","time":"2018-07-02T22:23:00.186641Z","elb":"app/my-loadbalancer/50dc6c495c0c9188","client":"10.0.1.252:48160","target":"10.0.0.66:9000","request_processing_time":0.000,"target_processing_time":0.002,"response_processing_time":0.000,"elb_status_code":200,"target_status_code":200,"received_bytes":5,"sent_bytes":257,"request":"GET https://10.0.2.105:773/ HTTP/2.0","user_agent":"user\"agent\" UpstreamClient(Apache-HttpClient/5.0.3 \\(Java/21.0.4\\))","ssl_cipher":"ECDHE-RSA-AES128-GCM-SHA256","ssl_protocol":"TLSv1.2","trace_id":"Root=1-58337327-72bd00b0343d75b906739c42","domain_name":"-","request_creation_time":"2018-07-02T22:22:48.364000Z","actions_executed":"redirect","redirect_url":"https://example.com:80/"}`,
			err:    false,
		},
	}

	lr := &LineRegex{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, out, err := lr.As(tt.format, tt.in)

			if (err != nil) != tt.err {
				t.Errorf("LineRegex.As() error = %v, wantErr %v", err, tt.err)
				return
			}

			if tt.err {
				return
			}

			if !ts.Equal(tt.ts) {
				t.Errorf("LineRegex.As() ts = %v, want %v", ts, tt.ts)
			}

			if out != tt.out {
				t.Errorf("LineRegex.As() out:\n%v\nwant:\n%v", out, tt.out)
			}
			if tt.format == "json" {
				if !json.Valid([]byte(out)) {
					t.Errorf("LineRegex.As() out is not valid JSON")
				}
			}
		})
	}

	ls := &LineSlice{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, out, err := ls.As(tt.format, tt.in)

			if (err != nil) != tt.err {
				t.Errorf("LineSlice.As() error = %v, wantErr %v", err, tt.err)
				return
			}

			if tt.err {
				return
			}

			if !ts.Equal(tt.ts) {
				t.Errorf("LineSlice.As() ts = %v, want %v", ts, tt.ts)
			}

			if out != tt.out {
				t.Errorf("LineSlice.As() out:\n%v\nwant:\n%v", out, tt.out)
			}
			if tt.format == "json" {
				if !json.Valid([]byte(out)) {
					t.Errorf("LineSlice.As() out is not valid JSON")
				}
			}
		})
	}
}

func BenchmarkLineRegex_AsLogfmt(b *testing.B) {
	lr := &LineRegex{}
	in := `http 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.000 0.001 0.000 200 200 34 366 "GET http://www.example.com:80/ HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337262-36d228ad5d99923122bbe354" "-" "-" 0 2018-07-02T22:22:48.364000Z "forward" "-" "-" "10.0.0.1:80" "200" "-" "-" TID_1234abcd5678ef90`
	for b.Loop() {
		lr.As("logfmt", in)
	}
}

func BenchmarkLineRegex_AsJson(b *testing.B) {
	lr := &LineRegex{}
	in := `http 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.000 0.001 0.000 200 200 34 366 "GET http://www.example.com:80/ HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337262-36d228ad5d99923122bbe354" "-" "-" 0 2018-07-02T22:22:48.364000Z "forward" "-" "-" "10.0.0.1:80" "200" "-" "-" TID_1234abcd5678ef90`
	for b.Loop() {
		lr.As("json", in)
	}
}

func BenchmarkLineSlice_AsLogfmt(b *testing.B) {
	l := &LineSlice{}
	in := `http 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.000 0.001 0.000 200 200 34 366 "GET http://www.example.com:80/ HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337262-36d228ad5d99923122bbe354" "-" "-" 0 2018-07-02T22:22:48.364000Z "forward" "-" "-" "10.0.0.1:80" "200" "-" "-" TID_1234abcd5678ef90`
	for b.Loop() {
		l.As("logfmt", in)
	}
}

func BenchmarkLineSlice_AsJson(b *testing.B) {
	l := &LineSlice{}
	in := `http 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.000 0.001 0.000 200 200 34 366 "GET http://www.example.com:80/ HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337262-36d228ad5d99923122bbe354" "-" "-" 0 2018-07-02T22:22:48.364000Z "forward" "-" "-" "10.0.0.1:80" "200" "-" "-" TID_1234abcd5678ef90`
	for b.Loop() {
		l.As("json", in)
	}
}
