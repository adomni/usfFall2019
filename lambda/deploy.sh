
# First, build the zip file
echo 'Creating zip file'
rm -f LambdaDeploy.zip
cd src
zip -r ../LambdaDeploy.zip .
cd ../venv/lib/python3.7/site-packages
pwd
#read -p "Press [Enter] key to continue"
zip -r -u ../../../../LambdaDeploy.zip .
#read -p "Press [Enter] key to continue"
cd ../../../..
pwd
#read -p "Press [Enter] key to continue"

# Next, package it
aws cloudformation package --template ./cloudformation.yaml --s3-bucket usf-deploys --output-template packaged-sam.yaml --region us-east-1

# Next, upload
aws cloudformation deploy --template-file packaged-sam.yaml --stack-name Usf2019Lambda --capabilities CAPABILITY_IAM --region us-east-1 --parameter-overrides LogLevel=info
