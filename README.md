# ALB Logs Shipper

A Go project for shipping Application Load Balancer (ALB) logs to Loki.
Has 2 run modes:

1. Cli Mode
It uses usual AWS SDK env vars to configure AWS credentials and region.
All files in the bucket are processed, and deleted if sucess. Could be run on a interval.
Cli flags:
- `--bucket-name` Name of the S3 bucket with ALB logs
- `--wait=60s` Interval to wait between runs
- `--loki-url` URL to Loki API
- `--loki-user` User to use for Loki authentication
Password is expected to be in the env var `LOKI_PASSWORD`. POST to loki happens in chunks of 100 entries or when S3 file is smaller.
Also it gets corresponding ALB tags:
- elbv2.k8s.aws/cluster
- ingress.k8s.aws/stack
and adds them to the log entry as labels.

2. Lambda Mode
It uses usual AWS SDK env vars to configure AWS credentials and region.
Bucket name is configured in the S3 trigger.
This mode leaves processed files in the bucket.

### Specs
Logs path example:
```
bucket[/prefix]/AWSLogs/aws-account-id/elasticloadbalancing/region/yyyy/mm/dd/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_end-time_ip-address_random-string.log.gz
```
Where `load-balancer-id` is used to discover ALB tags.

Log entries:
https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-entry-format

