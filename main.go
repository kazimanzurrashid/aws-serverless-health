package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	le "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/endpoints"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchevents"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type info struct {
	Current int `json:"current"`
	Limit   int `json:"limit"`
}

type action func(context.Context, aws.Config, chan<- info)

const defaultName = "aws-serverless-health"

var live = os.Getenv("LAMBDA_TASK_ROOT") != ""

func logError(cfg aws.Config, err error) {
	fmt.Printf("%v: %v\n", cfg.Region, err)
}

func getCloudFormationInfo(ctx context.Context, cfg aws.Config, out chan<- info) {
	defer close(out)

	c := make(chan int)
	l := make(chan int)

	go func() {
		count := getCloudFormationStackCount(ctx, cfg, 0, nil)
		c <- count
		close(c)
	}()

	go func() {
		limit := getCloudFormationLimit(ctx, cfg)
		l <- limit
		close(l)
	}()

	out <- info{<-c, <-l}
}

func getCloudFormationStackCount(ctx context.Context, cfg aws.Config, runningCount int, lastToken *string) int {
	svc := cloudformation.New(cfg)
	params := &cloudformation.DescribeStacksInput{NextToken: lastToken}
	req := svc.DescribeStacksRequest(params)
	res, err := req.Send(ctx)

	if err != nil {
		logError(cfg, err)
		return runningCount
	}

	var count int

	if res.Stacks != nil {
		count = runningCount + len(res.Stacks)
	}

	if res.NextToken != nil {
		return getCloudFormationStackCount(ctx, cfg, count, res.NextToken)
	}

	return count
}

func getCloudFormationLimit(ctx context.Context, cfg aws.Config) int {
	svc := cloudformation.New(cfg)
	params := &cloudformation.DescribeAccountLimitsInput{}
	req := svc.DescribeAccountLimitsRequest(params)
	res, err := req.Send(ctx)

	var l int

	if err != nil {
		logError(cfg, err)
		return l
	}

	if res.AccountLimits != nil {
		for _, limit := range res.AccountLimits {
			if *limit.Name == "StackLimit" {
				l = int(*limit.Value)
				break
			}
		}
	}

	return l
}

func getAPIGatewayInfo(ctx context.Context, cfg aws.Config, out chan<- info) {
	defer close(out)

	c := make(chan int)
	go func() {
		count := getAPIGatewayAPICount(ctx, cfg, 0, nil)
		c <- count
		close(c)
	}()

	out <- info{<-c, 60}
}

func getAPIGatewayAPICount(ctx context.Context, cfg aws.Config, runningCount int, lastPosition *string) int {
	svc := apigateway.New(cfg)
	params := &apigateway.GetRestApisInput{Position: lastPosition}
	req := svc.GetRestApisRequest(params)
	res, err := req.Send(ctx)

	if err != nil {
		logError(cfg, err)
		return runningCount
	}

	var count int

	if res.Items != nil {
		count = runningCount + len(res.Items)
	}

	if res.Position != nil {
		return getAPIGatewayAPICount(ctx, cfg, count, res.Position)
	}

	return count
}

func getLambdaInfo(ctx context.Context, cfg aws.Config, out chan<- info) {
	defer close(out)

	oneGB := int64(1024 * 1024 * 1024)
	svc := lambda.New(cfg)
	params := &lambda.GetAccountSettingsInput{}
	req := svc.GetAccountSettingsRequest(params)
	res, err := req.Send(ctx)

	if err != nil {
		logError(cfg, err)
		out <- info{0, 0}
		return
	}

	current := *res.AccountUsage.TotalCodeSize / oneGB
	limit := *res.AccountLimit.TotalCodeSize / oneGB

	out <- info{int(current), int(limit)}
}

func getDynamoDBInfo(ctx context.Context, cfg aws.Config, out chan<- info) {
	defer close(out)

	c := make(chan int)

	go func() {
		count := getDynamoDBTableCount(ctx, cfg, 0, nil)
		c <- count
		close(c)
	}()

	out <- info{<-c, 256}
}

func getDynamoDBTableCount(ctx context.Context, cfg aws.Config, runningCount int, lastTable *string) int {
	svc := dynamodb.New(cfg)
	params := &dynamodb.ListTablesInput{ExclusiveStartTableName: lastTable}
	req := svc.ListTablesRequest(params)
	res, err := req.Send(ctx)

	if err != nil {
		logError(cfg, err)
		return runningCount
	}

	var count int

	if res.TableNames != nil {
		count = runningCount + len(res.TableNames)
	}

	if res.LastEvaluatedTableName != nil {
		return getDynamoDBTableCount(ctx, cfg, count, res.LastEvaluatedTableName)
	}

	return count
}

func getKinesisInfo(ctx context.Context, cfg aws.Config, out chan<- info) {
	defer close(out)

	c := make(chan int)
	l := make(chan int)

	go func() {
		count := getKinesisStreamCount(ctx, cfg, 0, nil)
		c <- count
		close(c)
	}()

	go func() {
		limit := getKinesisLimit(ctx, cfg)
		l <- limit
		close(l)
	}()

	out <- info{<-c, <-l}
}

func getKinesisStreamCount(ctx context.Context, cfg aws.Config, runningCount int, lastStream *string) int {
	svc := kinesis.New(cfg)
	params := &kinesis.ListStreamsInput{ExclusiveStartStreamName: lastStream}
	req := svc.ListStreamsRequest(params)
	res, err := req.Send(ctx)

	if err != nil {
		logError(cfg, err)
		return runningCount
	}

	var count int

	if res.StreamNames != nil {
		length := len(res.StreamNames)
		count = runningCount + length
		if *res.HasMoreStreams {
			last := res.StreamNames[length-1]
			return getKinesisStreamCount(ctx, cfg, count, &last)
		}
	}

	return count
}

func getKinesisLimit(ctx context.Context, cfg aws.Config) int {
	svc := kinesis.New(cfg)
	params := &kinesis.DescribeLimitsInput{}
	req := svc.DescribeLimitsRequest(params)
	res, err := req.Send(ctx)

	var limit int

	if err != nil {
		logError(cfg, err)
		return limit
	}

	if res.ShardLimit != nil {
		limit = int(*res.ShardLimit)
	}

	return limit
}

func getCloudWatchEventInfo(ctx context.Context, cfg aws.Config, out chan<- info) {
	defer close(out)

	c := make(chan int)

	go func() {
		count := getCloudWatchEventRuleCount(ctx, cfg, 0, nil)
		c <- count
		close(c)
	}()

	out <- info{<-c, 100}
}

func getCloudWatchEventRuleCount(ctx context.Context, cfg aws.Config, runningCount int, lastToken *string) int {
	svc := cloudwatchevents.New(cfg)
	params := &cloudwatchevents.ListRulesInput{NextToken: lastToken}
	req := svc.ListRulesRequest(params)
	res, err := req.Send(ctx)

	if err != nil {
		logError(cfg, err)
		return runningCount
	}

	var count int

	if res.Rules != nil {
		count = runningCount + len(res.Rules)
	}

	if res.NextToken != nil {
		return getCloudWatchEventRuleCount(ctx, cfg, count, res.NextToken)
	}

	return count
}

func getSupportedRegions(ctx context.Context, baseConfig aws.Config) []string {
	mapping := make(map[string]chan bool)

	partition, _ := endpoints.NewDefaultResolver().Partitions().ForPartition("aws")

	for region := range partition.Regions() {
		cfg := baseConfig.Copy()
		cfg.Region = region

		e := make(chan bool)

		go func(c chan bool, r aws.Config) {
			c <- regionEnabled(ctx, r)
			close(c)
		}(e, cfg)

		mapping[region] = e
	}

	var regions []string

	for k, v := range mapping {
		if <-v {
			regions = append(regions, k)
		}
	}

	sort.Strings(regions)

	return regions
}

func regionEnabled(ctx context.Context, cfg aws.Config) bool {
	svc := sts.New(cfg)
	params := &sts.GetCallerIdentityInput{}
	req := svc.GetCallerIdentityRequest(params)
	_, err := req.Send(ctx)

	return err == nil
}

func publishInSns(ctx context.Context, cfg aws.Config, payload []byte) {
	topicName := os.Getenv("SNS_TOPIC")
	if topicName == "" {
		topicName = defaultName
	}

	message := string(payload)
	topicArn := getTopicArn(ctx, cfg, topicName, nil)
	svc := sns.New(cfg)
	params := &sns.PublishInput{TopicArn: &topicArn, Message: &message}
	req := svc.PublishRequest(params)
	_, _ = req.Send(ctx)
}

func getTopicArn(ctx context.Context, cfg aws.Config, topicName string, lastToken *string) string {
	svc := sns.New(cfg)
	params := &sns.ListTopicsInput{NextToken: lastToken}
	req := svc.ListTopicsRequest(params)
	res, _ := req.Send(ctx)

	for _, topic := range res.Topics {
		if strings.HasSuffix(*topic.TopicArn, topicName) {
			return *topic.TopicArn
		}
	}

	if res.NextToken != nil {
		return getTopicArn(ctx, cfg, topicName, res.NextToken)
	}

	panic("Unable to find the sns topic \"" + topicName + "\".")
}

func putInS3(ctx context.Context, cfg aws.Config, payload []byte) {
	bucketName := os.Getenv("S3_BUCKET")
	if bucketName == "" {
		bucketName = defaultName
	}

	key := time.Now().Format("20060102150405") + ".json"
	contentType := "text/json"

	svc := s3.New(cfg)
	params := &s3.PutObjectInput{
		Bucket:       &bucketName,
		Key:          &key,
		ContentType:  &contentType,
		Body:         bytes.NewReader(payload),
		StorageClass: s3.StorageClassReducedRedundancy}
	req := svc.PutObjectRequest(params)
	_, _ = req.Send(ctx)
}

func load(ctx context.Context, cfg aws.Config, out chan<- map[string]info) {
	defer close(out)

	actions := map[string]action{
		"cloudFormation":  getCloudFormationInfo,
		"apiGateway":      getAPIGatewayInfo,
		"lambda":          getLambdaInfo,
		"dynamodb":        getDynamoDBInfo,
		"kinesis":         getKinesisInfo,
		"cloudWatchEvent": getCloudWatchEventInfo}

	results := make(map[string]chan info)
	for k, v := range actions {
		t := make(chan info)
		results[k] = t
		go v(ctx, cfg, t)
	}

	result := make(map[string]info)
	for k, v := range results {
		result[k] = <-v
	}

	out <- result
}

func handler(ctx context.Context) ([]byte, error) {
	base, _ := external.LoadDefaultAWSConfig()

	results := make(map[string]chan map[string]info)
	regions := getSupportedRegions(ctx, base)

	for _, region := range regions {
		cfg := base.Copy()
		cfg.Region = region
		c := make(chan map[string]info)
		results[cfg.Region] = c
		go load(ctx, cfg, c)
	}

	report := make(map[string]map[string]info)
	for k, v := range results {
		report[k] = <-v
	}

	payload, _ := json.MarshalIndent(&report, "", "  ")

	if live {
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			publishInSns(ctx, base, payload)
			wg.Done()
		}()

		go func() {
			putInS3(ctx, base, payload)
			wg.Done()
		}()

		wg.Wait()
	}

	return payload, nil
}

func main() {
	if live {
		le.Start(handler)
	} else {
		p, _ := handler(context.Background())
		fmt.Println(string(p))
	}
}
