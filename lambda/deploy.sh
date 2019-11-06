
# First, build the zip file
echo 'Creating zip file'
cd src
zip -r ../LambdaDeploy.zip .
cd ..

# Next, package it
aws cloudformation package --template ./cloudformation.yaml --s3-bucket usf-deploys --output-template packaged-sam.yaml --region us-east-1

# Next, upload
aws cloudformation deploy --template-file packaged-sam.yaml --stack-name Usf2019Lambda --capabilities CAPABILITY_IAM --region us-east-1 --parameter-overrides LogLevel=info
