Steps
=====

1. Run the following commands:

```bash
GOOS=linux go build main.go
zip handler.zip ./main
```

2. And then

```bash
aws lambda create-function \
--region us-east-1 \
--function-name aws-serverless-health-go \
--memory 1536 \
--role arn:aws:iam::xxxxxxxxxxxx:role/aws-serverless-health-role \
--runtime go1.x \
--zip-file fileb://./handler.zip \
--handler main
```
