package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
)

type ELBMeta struct {
	client *elasticloadbalancingv2.Client
	data   map[string]Meta
}

type Meta struct {
	Namespace string
	Ingress   string
}

func NewELBMeta(client *elasticloadbalancingv2.Client) *ELBMeta {
	return &ELBMeta{
		data:   make(map[string]Meta),
		client: client,
	}
}

// Get lazily returns metadata for a load balancer
func (e *ELBMeta) Get(lbName string) (Meta, error) {
	if meta, ok := e.data[lbName]; ok {
		return meta, nil
	}

	lbs, err := e.client.DescribeLoadBalancers(context.TODO(), &elasticloadbalancingv2.DescribeLoadBalancersInput{
		Names: []string{lbName},
	})
	if err != nil {
		return Meta{}, err
	}
	if len(lbs.LoadBalancers) == 0 {
		return Meta{}, fmt.Errorf("load balancer %s not found", lbName)
	}

	tags, err := e.client.DescribeTags(context.TODO(), &elasticloadbalancingv2.DescribeTagsInput{
		ResourceArns: []string{*lbs.LoadBalancers[0].LoadBalancerArn},
	})
	if err != nil {
		return Meta{}, err
	}

	meta := Meta{}
	for _, tag := range tags.TagDescriptions[0].Tags {
		if *tag.Key == "ingress.k8s.aws/stack" {
			tmp := strings.Split(*tag.Value, "/")
			if len(tmp) != 2 {
				return Meta{}, fmt.Errorf("invalid ingress tag format: %s", *tag.Value)
			}
			meta.Namespace, meta.Ingress = tmp[0], tmp[1]
		}
	}
	e.data[lbName] = meta
	return meta, nil
}
