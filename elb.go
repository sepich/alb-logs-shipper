package main

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type ELBMeta struct {
	data  sync.Map
	roles map[string]string
}

type Meta struct {
	Cluster   string
	Namespace string
	Ingress   string
}

func NewELBMeta(roles map[string]string) *ELBMeta {
	return &ELBMeta{
		data:  sync.Map{},
		roles: roles,
	}
}

// Get lazily returns metadata for a load balancer
func (e *ELBMeta) Get(accountID, lbName string) (Meta, error) {
	if meta, ok := e.data.Load(accountID + "/" + lbName); ok {
		return meta.(Meta), nil
	}

	cli := e.client(accountID)
	lbs, err := cli.DescribeLoadBalancers(context.TODO(), &elasticloadbalancingv2.DescribeLoadBalancersInput{
		Names: []string{lbName},
	})
	if err != nil {
		return Meta{}, err
	}
	if len(lbs.LoadBalancers) == 0 {
		return Meta{}, fmt.Errorf("load balancer %s not found", lbName)
	}

	tags, err := cli.DescribeTags(context.TODO(), &elasticloadbalancingv2.DescribeTagsInput{
		ResourceArns: []string{*lbs.LoadBalancers[0].LoadBalancerArn},
	})
	if err != nil {
		return Meta{}, err
	}

	meta := Meta{}
	for _, tag := range tags.TagDescriptions[0].Tags {
		switch *tag.Key {
		case "ingress.k8s.aws/stack":
			tmp := strings.Split(*tag.Value, "/")
			if len(tmp) != 2 {
				return Meta{}, fmt.Errorf("invalid ingress tag format: %s", *tag.Value)
			}
			meta.Namespace, meta.Ingress = tmp[0], tmp[1]
		case "cluster-id":
			meta.Cluster = *tag.Value
		}
	}
	e.data.Store(accountID+"/"+lbName, meta)
	return meta, nil
}

func (e *ELBMeta) client(accountID string) *elasticloadbalancingv2.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil
	}

	if e.roles[accountID] != "" {
		roleAssumptionProvider := stscreds.NewAssumeRoleProvider(
			sts.NewFromConfig(cfg),
			e.roles[accountID],
			func(o *stscreds.AssumeRoleOptions) {
				o.RoleSessionName = "alb-logs-shipper"
			},
		)
		cfg, err = config.LoadDefaultConfig(context.TODO(),
			config.WithCredentialsProvider(roleAssumptionProvider),
		)
		if err != nil {
			return nil
		}
	}
	return elasticloadbalancingv2.NewFromConfig(cfg)
}
