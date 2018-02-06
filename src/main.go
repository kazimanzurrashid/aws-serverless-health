package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
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
)

type action func(aws.Config, chan<- info)

// info of an aws service
type info struct {
	Current int `json:"current"`
	Limit   int `json:"limit"`
}

const defaultName = "aws-serverless-health"

var inLambda = os.Getenv("LAMBDA_TASK_ROOT") != ""

func getCloudFormationInfo(config aws.Config, t chan<- info) {
	c := make(chan int)
	l := make(chan int)

	go getCloudFormationStackCount(config, 0, nil, c)
	go getCloudFormationLimit(config, l)

	t <- info{<-c, <-l}

	close(t)
}

func getCloudFormationStackCount(config aws.Config, previousCount int, nextToken *string, c chan<- int) {
	svc := cloudformation.New(config)
	params := &cloudformation.DescribeStacksInput{NextToken: nextToken}
	req := svc.DescribeStacksRequest(params)
	res, _ := req.Send()

	count := previousCount + len(res.Stacks)

	if res.NextToken != nil {
		getCloudFormationStackCount(config, count, res.NextToken, c)
	}

	c <- count

	close(c)
}

func getCloudFormationLimit(config aws.Config, c chan<- int) {
	svc := cloudformation.New(config)
	params := &cloudformation.DescribeAccountLimitsInput{}
	req := svc.DescribeAccountLimitsRequest(params)
	res, _ := req.Send()

	c <- int(*res.AccountLimits[0].Value)

	close(c)
}

func getAPIGatewayInfo(config aws.Config, t chan<- info) {
	c := make(chan int)
	go getAPIGatewayAPICount(config, 0, nil, c)

	t <- info{<-c, 60}

	close(t)
}

func getAPIGatewayAPICount(config aws.Config, previousCount int, nextPosition *string, c chan<- int) {
	svc := apigateway.New(config)
	params := &apigateway.GetRestApisInput{Position: nextPosition}
	req := svc.GetRestApisRequest(params)
	res, _ := req.Send()

	count := previousCount + len(res.Items)

	if res.Position != nil {
		getAPIGatewayAPICount(config, count, res.Position, c)
	}

	c <- count

	close(c)
}

func getLambdaInfo(config aws.Config, t chan<- info) {
	oneGB := int64(1024 * 1024 * 1024)
	svc := lambda.New(config)
	params := &lambda.GetAccountSettingsInput{}
	req := svc.GetAccountSettingsRequest(params)
	res, _ := req.Send()

	current := *res.AccountUsage.TotalCodeSize / oneGB
	limit := *res.AccountLimit.TotalCodeSize / oneGB

	t <- info{int(current), int(limit)}

	close(t)
}

func getDynamoDBInfo(config aws.Config, t chan<- info) {
	c := make(chan int)
	go getDynamoDBTableCount(config, 0, nil, c)

	t <- info{<-c, 256}

	close(t)
}

func getDynamoDBTableCount(config aws.Config, previousCount int, nextTable *string, c chan<- int) {
	svc := dynamodb.New(config)
	params := &dynamodb.ListTablesInput{ExclusiveStartTableName: nextTable}
	req := svc.ListTablesRequest(params)
	res, _ := req.Send()

	count := previousCount + len(res.TableNames)

	if res.LastEvaluatedTableName != nil {
		getDynamoDBTableCount(config, count, res.LastEvaluatedTableName, c)
	}

	c <- count

	close(c)
}

func getKinesisInfo(config aws.Config, t chan<- info) {
	c := make(chan int)
	go getKinesisStreamCount(config, 0, nil, c)

	l := make(chan int)
	go getKinesisLimit(config, l)

	t <- info{<-c, <-l}

	close(t)
}

func getKinesisLimit(config aws.Config, c chan<- int) {
	svc := kinesis.New(config)
	params := &kinesis.DescribeLimitsInput{}
	req := svc.DescribeLimitsRequest(params)
	res, _ := req.Send()

	c <- int(*res.ShardLimit)

	close(c)
}

func getKinesisStreamCount(config aws.Config, previousCount int, nextStream *string, c chan<- int) {
	svc := kinesis.New(config)
	params := &kinesis.ListStreamsInput{ExclusiveStartStreamName: nextStream}
	req := svc.ListStreamsRequest(params)
	res, _ := req.Send()

	count := previousCount + len(res.StreamNames)

	if *res.HasMoreStreams {
		last := res.StreamNames[len(res.StreamNames)-1]
		getKinesisStreamCount(config, count, &last, c)
	}

	c <- count

	close(c)
}

func getCloudWatchEventInfo(config aws.Config, t chan<- info) {
	c := make(chan int)
	go getCloudWatchEventRuleCount(config, 0, nil, c)

	t <- info{<-c, 100}

	close(t)
}

func getCloudWatchEventRuleCount(config aws.Config, previousCount int, nextToken *string, c chan<- int) {
	svc := cloudwatchevents.New(config)
	params := &cloudwatchevents.ListRulesInput{NextToken: nextToken}
	req := svc.ListRulesRequest(params)
	res, _ := req.Send()

	count := previousCount + len(res.Rules)

	if res.NextToken != nil {
		getCloudWatchEventRuleCount(config, count, res.NextToken, c)
	}

	c <- count

	close(c)
}

func publishInSns(config aws.Config, payload []byte, wg *sync.WaitGroup) {
	defer wg.Done()

	topicName := os.Getenv("SNS_TOPIC")
	if topicName == "" {
		topicName = defaultName
	}

	message := string(payload)
	topicArn := getTopicArn(config, topicName, nil)
	svc := sns.New(config)
	params := &sns.PublishInput{TopicArn: &topicArn, Message: &message}
	req := svc.PublishRequest(params)
	req.Send()
}

func getTopicArn(config aws.Config, topicName string, nextToken *string) string {
	svc := sns.New(config)
	params := &sns.ListTopicsInput{NextToken: nextToken}
	req := svc.ListTopicsRequest(params)
	res, _ := req.Send()

	for _, topic := range res.Topics {
		if strings.HasSuffix(*topic.TopicArn, topicName) {
			return *topic.TopicArn
		}
	}

	if res.NextToken != nil {
		return getTopicArn(config, topicName, res.NextToken)
	}

	panic("Unable to find the sns topic \"" + topicName + "\".")
}

func putInS3(config aws.Config, payload []byte, wg *sync.WaitGroup) {
	defer wg.Done()

	bucketName := os.Getenv("S3_BUCKET")
	if bucketName == "" {
		bucketName = defaultName
	}

	key := time.Now().Format("20060102150405") + ".json"
	contentType := "text/json"

	svc := s3.New(config)
	params := &s3.PutObjectInput{
		Bucket:       &bucketName,
		Key:          &key,
		ContentType:  &contentType,
		Body:         bytes.NewReader(payload),
		StorageClass: s3.StorageClassStandardIa}
	req := svc.PutObjectRequest(params)
	req.Send()
}

func load(config aws.Config, c chan<- map[string]info) {
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
		go v(config, t)
	}

	result := make(map[string]info)
	for k, v := range results {
		result[k] = <-v
	}

	c <- result

	close(c)
}

func handler() error {
	defaultConfig, _ := external.LoadDefaultAWSConfig()

	results := make(map[string]chan map[string]info)
	partition, _ := endpoints.NewDefaultResolver().Partitions().ForPartition("aws")
	for region := range partition.Regions() {
		regionConfig := defaultConfig.Copy()
		regionConfig.Region = region
		c := make(chan map[string]info)
		results[regionConfig.Region] = c
		go load(regionConfig, c)
	}

	report := make(map[string]map[string]info)
	for k, v := range results {
		report[k] = <-v
	}

	payload, _ := json.Marshal(&report)

	if inLambda {
		var wg sync.WaitGroup
		wg.Add(2)

		go publishInSns(defaultConfig, payload, &wg)
		go putInS3(defaultConfig, payload, &wg)

		wg.Wait()
	} else {
		fmt.Println(string(payload))
	}

	return nil
}

func main() {
	if inLambda {
		le.Start(handler)
	} else {
		handler()
	}
}
