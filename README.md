# Pricing API

The Pricing API was developed to quickly price various products with different pricing methods and parameters.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Docker Image](#docker-image)
- [API Gateway and Lambda Function](#api-gateway-and-lambda-function)
- [Configuration](#configuration)
- [Deployment](#deployment)
  - [test](#test)
  - [build-and-deploy](#build-and-deploy)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

## Overview

This project was developed to tackle pain points that where highlighted by the business with regards to pricing lending products. The solution that was built by the team is a REST API hosted on API Gateway with a Lambda Function acting as the backend. Any interaction with the API is logged in a dynamoDB table. The solution can handle various different products via config files. These config files are stored on S3 and visualised on rbCore where key steak holders can configure them as needed.

## Features
        
- Products: The API uses a config file s3 path, this file states which products are currently supported by the API and which are not. To support a new product you must fill in the Parameters and the Pricing_methods columns to state which params must be included so that a complete pricing run can be completed for that product and which pricing methods that particular product supports (model, market, market_simple).

- Pricing Types: The API supports three different pricing types, model, market and market_simple, for a given product the pricing method must be specified in the config file so that the API knows which products support wich pricing types. 

- Config File Store: All config files are supported at s3 path the API uses these to know which products are supported, what the params are that are needed for these products, the pricing methods that are supported for these products and certain metrics that are decided on by the business to actually price the products. 

- Audit: Each API run audit data is stored in a dynamoDB ("pricing_apirunlog") table at run time, this will include all output data that is produced by the api along with inputdata, date and time of run


## Getting Started

This project includes four components, to fully understand the pipeline you can review these components on the AWS Console

- API Gateway: API ID (g3232423), used to forward request params, envoke lambda function backend and finally show the payload back to the API client.

- Lambda Function: Lambda-V02, this Lambda function is envoked by the REST API mentioned above. It utilises various different parameteres (depending on the product) to understand which config files need to be open and used to calculate the price that would be offered for that product. 

- dynamoDB Table: dynamo_db, is used to store all audit data associated with the API run, this includes all output, user_id, date and time of invocation. This table should be used for caching logic within applications.

- S3 Bucket: s3 bucket is used to store all the configuration files that are needed to conduct the calculation and show a final price for a product.


### Prerequisites

To use the API a valid API key and usage plan must be created for the user - currently only nCino Usage plan and Key exist. For each consumer a Usage Plan and Key must be created so that there is a quota on amount of invocations for each client and there is monitoring enabled for that client on API Gateway console. 

## Usage

To use the API 

<pre>
```
def PricingAPI():

    headers = {'X-API-Key':{Your Api Key}}
    params = {'product': 'B',
              'amount':'3000',
              'term':'24',
              'loan_id': 'edefr4',
              'credit_risk': 'Strong',
              'pricing_type': 'model',
              'loan_to_value': '45',
              'source_name': 'ncino',
              'user_name': {Your name}}

    Response = requests.get(
        some link , params = params, headers = headers 
    )

    return Response.json()
```
</pre>

## Docker Image

The docker file used in this project is rather straight forward. requirements.txt file is copied over into the root directory and requirements installed. main.py is copied over into the root directory as this file is the one that includes all of the code used in the lambda function. 

Finally CMD ["main.handler"] states which function within the main.py file should be executed.

## API Gateway and Lambda Function

![Architecture diagram](./pricingAPI-Architecture.png)

1. API Gateway envokes the Lambda Function and sends all params that where included in the invocation.

2. The lambda invocation is logged on CloudWatch, any errors or critical logs invoke Error handler lambda function which is covered in its respective documentation.

3. The lambda function reads the params and opens the config files that are needed for the specified product and pricing method

4. The lambda function stores all audit data in the dynamoDB table mentioned above. 

5. Final pricing data and meta data is sent back to the client for consumption

## Configuration

Environment variables are used to store the S3 bucket name. The API Gateway is configured to include various different params depending on the product param that is passed through. For some clarity I have added some of these below,

* product - product type to be included with every API call made.
* credit_risk - credit risk, can be of two different types depending on product. Either rating between 1-10 or in a scale of ['Weak', 'Satisfactory', 'Good', 'Strong']
* term - term of the loan in months
* amount - amount that the loan will be for
* loan_id - ID of loan as shown on nCino
* loan_to_value - loan to value of loan to collateral, only needed for particular products
* pricing_type - type of pricing you would like to use for this particular API run (model, market or market_simple)
* source_name - source platform that is envoking the API for example nCino, portal, rbCore etc.
* user_name - email of user that is envoking the API from the source platform


Lambda Configuration:

* Memory - 10000 MB
* Ephemeral storage - 2000 MB 
* Timeout - 3 seconds

## Deployment

CI/CD has been developed for this project under the .github/workflows folder. Deployment is split into two jobs, test and build-and-deploy I will be going over both.

### test

Note - This job only executes when merging with the dev branch and skipped when merging with the main branch.

1. Checkout code and set up python.
2. Install dependencies and format code.
3. Run all pytests and set output variable "tests_passed" to indicate if all tests have passed or not.

### build-and-deploy

Note - this job runs when merging into the dev and main branches

1. checkout code and check previous steps output variable "tests_passed" to verify that all tests have passed, if they have not this steps exits. Note - This step only executes when merging with the dev branch.
Note - the following steps are executing on either dev or main AWS account depending on the branch that the code is being merged into.
2. Configure AWS credentials.
3. Login to Amazon ECR Repository.
4. Build and push your docer image to the ECR repository.
5. Deploy the image to the Lambda function. 

## Testing

Unit testing is built into the CI/CD pipeline and executed whenever a branch is merged with the dev branch. Explained in more detail above.

## Contributing

To contribute to this project you must have access to the private repository (which I assume you do as you are reading this document). Create a feature branch and when your changes are complete create a pull request into the dev branch. There is a compulsary peer review that needs to be completed before you are able to merge into the Dev or Main branches. Both of these branches are locked otherwise. Main branch is only to be merged with the Dev branch on a set period, for this project we are following Thursday deployments. 

Once any merge is complete, the CI/CD pipeline will either deploy the code into the Dev pipeline or the Prod pipeline. You can use the above section *Usage to view instructions on how to test your deployment in both prod and dev environments.

## License

This code must not be used or deployed on any other AWS account or outside of the Recognise Bank network. 

---

