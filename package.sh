#!/bin/bash

cp lambda_function.py ./build/
cd ./build
zip -r ../lambda.zip ./*
cd ..
aws s3 cp lambda.zip s3://<your_bucket_for_lambda_code>/
