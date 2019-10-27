BUILD_DIR=dist
TARGET=aws-serverless-health-go
TARGET_FILE=${TARGET}.zip
S3_BUCKET= YOUR_BUCKET

all: init build compress upload cleanup update

init:
	mkdir -p ${BUILD_DIR}

build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ${BUILD_DIR}/main

compress:
	cd ${BUILD_DIR};zip -r -9 ${TARGET_FILE} *

upload:
	aws s3 cp ${BUILD_DIR}/${TARGET_FILE} s3://${S3_BUCKET}/${TARGET_FILE} --storage-class  STANDARD_IA

cleanup:
	rm -r ${BUILD_DIR}

update:
	aws lambda update-function-code --function-name ${TARGET} --s3-bucket ${S3_BUCKET} --s3-key ${TARGET_FILE}