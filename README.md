# ALB Logs Shipper

Yet another ALB S3 logs shipper to Loki. This one mostly targeted for an EKS use case where ALB is dynamically created by `aws-load-balancer-controller`

### How it works
- You have a S3 bucket [with permissions for ALB](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/enable-access-logging.html#access-log-create-bucket) to write logs to
- You or mutation adds the following annotation to an Ingress object of alb ingressClass:  
`alb.ingress.kubernetes.io/load-balancer-attributes: access_logs.s3.enabled=true,access_logs.s3.bucket=<bucket-name>`
- ALB creates log files in the bucket in the format:  
  ```
  bucket[/prefix]/AWSLogs/aws-account-id/elasticloadbalancing/region/yyyy/mm/dd/aws-account-id_elasticloadbalancing_region_app.load-balancer-id_end-time_ip-address_random-string.log.gz
  ```
- `alb-logs-shipper` start to list all files from `--bucket-name` and process only those matching the pattern above
- Based on `aws-account-id` and `load-balancer-id` in the filename it lazily reads (and caches) tag `ingress.k8s.aws/stack` from the corresponding ALB to get `ingress` and `namespace` labels. These labels are added to the log stream. That is why such IAM permissions are required:
  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:DeleteObject"
        ],
        "Resource": [
          "arn:aws:s3:::${aws_s3_bucket.alb_logs.id}",
          "arn:aws:s3:::${aws_s3_bucket.alb_logs.id}/*"
        ]
      },
      {
        "Effect": "Allow",
        "Action": [
          "elasticloadbalancing:DescribeLoadBalancers",
          "elasticloadbalancing:DescribeTags"
        ],
        "Resource": "*"
      }
    ]
  }
  ```
- The log.gz file is read from S3, unpacked on the fly, and then sent to Loki in batches of 100 lines. 429 and 5xx responses are retried with backoff. On success the file is deleted from S3. So no lifecycle is required on the S3 side, and the bucket would be empty under normal operation.
- After all files are processed, it waits `--wait=60s` and then scan for new files again. New log files appear in S3 with a delay of ~2m.

### Multicluster mode
It is possible to ship logs from ALB in aws account `A` to S3 bucket in account `B`. So, in multicluster multiaccount setup it is possible to have the same annotation in Ingress objects to ship logs to the single S3 bucket. Note that ALB only ships to bucket in the same region, so it is bucket-per-region.
- From log filename we can get source `account-id` and `loadbalancer-id`. But then, to get ALB tags, we need a list of IAM roles (`--role-arn`) to do `AssumeRole` for each account writing ALB logs to the bucket.
- Additional IAM permissions are required on `alb-logs-shipper` (S3 bucket) side:
  ```json
  {
  "Version": "2012-10-17",
  "Statement": [
      {
        "Effect": "Allow",
        "Action": "sts:AssumeRole",
        "Resource": "*"
      }
    ]
  }
  ```
- and on the ALB account side:
  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "elasticloadbalancing:DescribeLoadBalancers",
          "elasticloadbalancing:DescribeTags"
        ],
        "Resource": "*"
      }
    ]
  }
  ```
  with Assume Role Policy:
  ```json
  {
      "Version": "2012-10-17",
      "Statement": [{
          "Effect": "Allow",
          "Principal": {"AWS": "arn:aws:iam::<bucket-account-id>:role/<alb-logs-shipper-role>"},
          "Action": "sts:AssumeRole"
      }]
  }
  ```
- Multiple clusters in the same account are distinguished by `cluster-id` tag on ALB. Which could be added by [--default-tags](https://kubernetes-sigs.github.io/aws-load-balancer-controller/v2.5/deploy/configurations/#controller-command-line-flags) option of aws-load-balancer-controller.

### Cli args
```bash
# docker run sepa/alb-logs-shipper
Usage of /alb-logs-shipper:
  -bucket-name string
        Name of the S3 bucket with ALB logs (required)
  -format string
        Format to parse and ship log lines as (logfmt, json, raw) (default "raw")
  -label value
        Label to add to Loki stream, can be specified multiple times (key=value)
  -log-level string
        Log level (info, debug) (default "info")
  -loki-url string
        URL to Loki API (required)
  -loki-user string
        User to use for Loki authentication
  -role-arn value
        ARN of the IAM role to assume to access ALB tags, can be specified multiple times
  -version
        Show version and exit
  -wait duration
        Interval to wait between runs (default 1m0s)
```
And the password for Loki endpoint could be set via `LOKI_PASSWORD` env var.

### Log entries format
https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-entry-format

### Lambda mode  
There are pros and cons for running this as a lambda:
https://github.com/grafana/loki/blob/main/tools/lambda-promtail/README.md  

pros:
- the latency of the latest logs delivery is lower, as we ship it immediately on write to S3
- on low-load buckets it might be cheaper to run it at 2x cpu/mem price in short bursts, than always working container at 0.3x spot price

cons:
- separate execution per-file means we cannot cache ALB tags between runs and would hit API for each file
- log files could be massive for loaded LB, and with retries time to process the file could? hit lambda execution limit (15m) 
- missed logs on non-delivered events, or on temporary problems with loki because no second lambda execution for old events would be done. Probably could be solved by SQS with ACK, or additional cron lambda which processes the whole bucket.
 
### TODO
- The tag `ingress.k8s.aws/stack` is set to `namespace/ingressname` only for an implicit IngressGroup. When the IngressGroup is set on Ingress, there is no way to get ns/ingressname. Dynamic placeholders are not supported in `--default-tags` of alb controller. Need to use mutation for Ingress objects adding `alb.ingress.kubernetes.io/tags` annotation with ns/ingressname.
- handle SIGTERM between files?
