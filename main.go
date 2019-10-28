package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
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

var appName = os.Getenv("APP_NAME")
var live = os.Getenv("LAMBDA_TASK_ROOT") != ""

func logError(cfg aws.Config, err error) {
	log.Printf("%v: %v\n", cfg.Region, err)
}

func getCloudFormationInfo(ctx context.Context, cfg aws.Config, out chan<- info) {
	defer close(out)

	c := make(chan int)
	l := make(chan int)

	go func() {
		c <- getCloudFormationStackCount(ctx, cfg, 0, nil)
		close(c)
	}()

	go func() {
		l <- getCloudFormationLimit(ctx, cfg)
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

	var r int

	if err != nil {
		logError(cfg, err)
		return r
	}

	if res.AccountLimits != nil {
		for _, limit := range res.AccountLimits {
			if *limit.Name == "StackLimit" {
				r = int(*limit.Value)
				break
			}
		}
	}

	return r
}

func getAPIGatewayInfo(ctx context.Context, cfg aws.Config, out chan<- info) {
	defer close(out)

	c := make(chan int)
	go func(r aws.Config) {
		count := getAPIGatewayAPICount(ctx, cfg, 0, nil)
		c <- count
		close(c)
	}(cfg)

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
		c <- getDynamoDBTableCount(ctx, cfg, 0, nil)
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
		c <- getKinesisStreamCount(ctx, cfg, 0, nil)
		close(c)
	}()

	go func() {
		l <- getKinesisLimit(ctx, cfg)
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
		c <- getCloudWatchEventRuleCount(ctx, cfg, 0, nil)
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

func getSupportedRegions(ctx context.Context, baseRegion aws.Config) []string {
	mapping := make(map[string]chan bool)

	partition, _ := endpoints.NewDefaultResolver().Partitions().ForPartition("aws")

	for region := range partition.Regions() {
		cfg := baseRegion.Copy()
		cfg.Region = region

		e := make(chan bool)

		go func(c chan bool, r aws.Config) {
			c <- enabled(ctx, r)
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

func enabled(ctx context.Context, cfg aws.Config) bool {
	svc := sts.New(cfg)
	params := &sts.GetCallerIdentityInput{}
	req := svc.GetCallerIdentityRequest(params)
	_, err := req.Send(ctx)

	return err == nil
}

func publishInSns(ctx context.Context, cfg aws.Config, payload []byte) {
	topicArn := getTopicArn(ctx, cfg, nil)

	if topicArn == nil {
		return
	}

	message := string(payload)
	svc := sns.New(cfg)
	params := &sns.PublishInput{TopicArn: topicArn, Message: &message}
	req := svc.PublishRequest(params)
	_, _ = req.Send(ctx)
}

func getTopicArn(ctx context.Context, cfg aws.Config, lastToken *string) *string {
	if appName == "" {
		return nil
	}

	svc := sns.New(cfg)
	params := &sns.ListTopicsInput{NextToken: lastToken}
	req := svc.ListTopicsRequest(params)
	res, _ := req.Send(ctx)

	for _, topic := range res.Topics {
		if strings.HasSuffix(*topic.TopicArn, appName) {
			return topic.TopicArn
		}
	}

	if res.NextToken != nil {
		return getTopicArn(ctx, cfg, res.NextToken)
	}

	log.Fatalf("Unable to find the sns topic %q.", appName)

	return nil
}

func putInS3(ctx context.Context, cfg aws.Config, payload []byte) {
	if appName == "" {
		return
	}

	key := time.Now().Format("20060102150405") + ".json"

	svc := s3.New(cfg)
	params := &s3.PutObjectInput{
		Bucket:       &appName,
		Key:          &key,
		ContentType:  aws.String("text/json"),
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

func handler(ctx context.Context) error {
	defaultConfig, _ := external.LoadDefaultAWSConfig()

	results := make(map[string]chan map[string]info)
	regions := getSupportedRegions(ctx, defaultConfig)

	for _, region := range regions {
		cfg := defaultConfig.Copy()
		cfg.Region = region
		c := make(chan map[string]info)
		results[cfg.Region] = c
		go load(ctx, cfg, c)
	}

	report := make(map[string]map[string]info)
	for k, v := range results {
		report[k] = <-v
	}

	if live {
		payload, _ := json.Marshal(&report)
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			publishInSns(ctx, defaultConfig, payload)
			wg.Done()
		}()

		go func() {
			putInS3(ctx, defaultConfig, payload)
			wg.Done()
		}()

		wg.Wait()
	} else {
		payload, _ := json.MarshalIndent(&report, "", "  ")
		log.Println(string(payload))
	}

	return nil
}

func main() {
	if live {
		le.Start(handler)
	} else {
		_ = handler(context.Background())
	}
}
