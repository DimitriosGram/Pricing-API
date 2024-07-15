import pandas as pd
import json
from dotenv import load_dotenv
import boto3
import os
import datetime
from io import StringIO
import numpy as np
import time
from uuid import uuid4
from typing import Optional
import ast

load_dotenv()
AWS_BUCKET = os.environ.get("RB_AWS_S3_BUCKET")


def create_dataframe(product: str, credit_risk: str, term: str, amount: str, loan_id: str, run_id: str, date: str, price: str, user_name:str, source_name:str, pricing_type : str, loan_to_value: Optional[str] = 'None', de_run_id: Optional[str] = 'None'):
    """
    Function used to create payload for dynamoDB Put operation for metadata
    Args:
        product: product that we want to price for
        credit_risk: credit risk of the loan we want to price for
        term: term fo loan we want to price for
        amount: amount the loan is for
        loan_id: id of the loan
        run_id: run id for api run tracking
        date: date that the api was run on
        price: price that the api predicted
    Returns:
        df: dictionary structured as dynamoDB payload
    """
    df = {
        'product':{'S':product},
        'credit_risk':{'S':credit_risk},
        'term':{'S':term},
        'loan_to_value':{'S': loan_to_value},
        'amount':{'S':amount},
        'loan_id':{'S':loan_id},
        'run_id':{'S': run_id},
        'date': {'S': date},
        'price': {'S': price},
        'user_name': {'S': user_name},
        'source_name': {'S': source_name},
        'de_run_id': {'S': de_run_id},
        'pricing_type': {'S': pricing_type}
    }
    
    return df

def product_specification(s3, product: str):
    """
    Function used to verify that a product is supported by the API and if it is to set the params that are required for the given product
    Args:
        s3: s3 connection
        product: product that was selected at API invocation
    Returns:
        df_product_filter['Supported'].values[0]: 0, 1 value indicating whether the product is supported
        ast.literal_eval(df_product_filter['Parameters'].values[0]): list of params that are needed for invocation
        ast.literal_eval(df_product_filter['Pricing_Methods'].values[0]): list of pricing methods that are valid for the product
    """
    df_prodspec = pd.read_csv(StringIO(s3.get_object(Bucket = AWS_BUCKET, Key = "CPPricer/parquetfiles/product_specifications.csv")['Body'].read().decode('utf-8')))
    df_product_filter = df_prodspec[df_prodspec['Idx'] == product].reset_index()

    return df_product_filter['Supported'].values[0], ast.literal_eval(df_product_filter['Parameters'].values[0]), ast.literal_eval(df_product_filter['Pricing_Methods'].values[0])


def open_pricingband_model(s3):
    """
    Function used to open the tables needed for the pricing calculation
    Args:
        s3: s3 connection
    Returns:
        df_finance: dataframe of finance table including NIM per product
        df_fundingcurve: dataframe of funding curve table
        df_sizepremia: dataframe of size premia table
        df_termpremia: dataframe of term premia table
    """

    df_finance = pd.read_csv(StringIO(s3.get_object(Bucket = AWS_BUCKET, Key = "CPPricer/parquetfiles/finance.csv")['Body'].read().decode('utf-8')))
    df_fundingcurve = pd.read_csv(StringIO(s3.get_object(Bucket = AWS_BUCKET, Key = "CPPricer/parquetfiles/fundingcurve.csv")['Body'].read().decode('utf-8')))
    df_sizepremia = pd.read_csv(StringIO(s3.get_object(Bucket = AWS_BUCKET, Key = "CPPricer/parquetfiles/sizepremia.csv")['Body'].read().decode('utf-8')))
    df_termpremia = pd.read_csv(StringIO(s3.get_object(Bucket = AWS_BUCKET, Key = "CPPricer/parquetfiles/termpremia.csv")['Body'].read().decode('utf-8')))
    df_creditpremia = pd.read_csv(StringIO(s3.get_object(Bucket = AWS_BUCKET, Key = "CPPricer/parquetfiles/credit_premia.csv")['Body'].read().decode('utf-8')))

    return df_finance, df_fundingcurve, df_sizepremia, df_termpremia, df_creditpremia

def open_pricingband_market(s3):
    """
    Function used to open the tables needed for the pricing calculation
    Args:
        s3: s3 connection
    Returns:
        df_discount: dataframe of term discount table
    """

    df_discount = pd.read_csv(StringIO(s3.get_object(Bucket = AWS_BUCKET, Key = "CPPricer/parquetfiles/term_risk_discount.csv")['Body'].read().decode('utf-8')))

    return df_discount

def open_pricingband_market_simple(s3):
    """
    Function used to open the tables needed for the pricing calculation
    Args:
        s3: s3 connection
    Returns:
        df_market_simple: table of simple market price
    """

    df_market_simple = pd.read_csv(StringIO(s3.get_object(Bucket = AWS_BUCKET, Key = "CPPricer/parquetfiles/market_simple_table.csv")['Body'].read().decode('utf-8')))

    return df_market_simple

def pricing_calc_model(s3, product: str, credit_risk: str, term: int, amount: float, loan_to_value: Optional[int] = None):
    """
    Function that filters through tables to find the right parameters to calculate price
    Args:
        product: product that we want to price for
        credit_risk: credit risk of the loan we want to price for
        term: term fo loan we want to price for
        amount: amount the loan is for
        s3: s3 connections
    Returns:
        price: predicted price for the loan
    """
    credit_risk_model = ['Strong', 'Satisfactory', 'Good', 'Weak']
    if credit_risk not in credit_risk_model:
        return {'statusCode': 400, "body": f"Credit risk must be a value in {credit_risk_model} for this pricing type"}

    df_finance, df_fundingcurve, df_sizepremia, df_termpremia, df_creditpremia = open_pricingband_model(s3)

    NIM = df_finance.loc[df_finance["Idx"] == product, "NIM"].values[0] * 100

    FC_premia = df_fundingcurve.loc[(np.abs(df_fundingcurve["Time(in months)"] - int(term))).idxmin(), product]/100
    Term_Premia = df_termpremia.loc[(np.abs(df_termpremia["Time(in months)"] - int(term))).idxmin(), product]/100
    Size_Premia = df_sizepremia.loc[(np.abs(df_sizepremia["Size(in thousands)"] - int(amount)/1000)).idxmin(), product]/100
    
    df_creditpremia = df_creditpremia[df_creditpremia['Product'] == product]

    if loan_to_value is None:
        Credit_Premia = df_creditpremia.loc[(df_creditpremia['DimOneValMin'] <= float(term)) & (df_creditpremia['DimOneValMax'] >= float(term)) & (df_creditpremia['DimTwoVal'] == credit_risk), 'Value'].values[0]/100
    else:
        Credit_Premia = df_creditpremia.loc[(df_creditpremia['DimOneValMin'] <= float(loan_to_value)) & (df_creditpremia['DimOneValMax'] >= float(loan_to_value)) & (df_creditpremia['DimTwoVal'] == credit_risk), 'Value'].values[0]/100


    return NIM + FC_premia + Term_Premia + Size_Premia + Credit_Premia


def pricing_calc_market(s3, credit_risk: float, term: int):
    """
    Function that uses a linear relation ship between credit risk and rate with a term discount
    Args:
        credit_risk: credit risk of the loan we want to price for (must be a float here)
        term: term of the loan we want to price for
        s3: s3 connections
    Returns:
        price: predicted price for the loan
    """
    df_discount = open_pricingband_market(s3)

    
    if isinstance(credit_risk, str):
        return {'statusCode': 400, "body": f"Credit risk must be a value between 1 and 10 for this pricing type"}

    if credit_risk >= 7.5:
        risk_bucket = "Strong"
    elif (7.5 > credit_risk) & (credit_risk >= 5):
        risk_bucket = "Good"
    elif (5 > credit_risk) & (credit_risk >= 2.5):
        risk_bucket = "Satisfactory"
    else:
        risk_bucket = "Weak"

    term_discount = df_discount.loc[(df_discount["Term"] == int(term)), risk_bucket].values[0]/100
    
    return (- 1.3333333 * credit_risk + 28.333333) - term_discount

def pricing_calc_market_simple(s3, credit_risk: float, term: int):
    """
    Function used to price loans in a simple market manner so that linear relationships do not need to be calculated
    Args:
        credit_risk: credit risk of the loan we want to price for (must be a float here)
        term: term of the loan we want to price for
        s3: s3 connections
    Returns:
        Credit_Premia_Val: predicted price for the loan
    """
    if isinstance(credit_risk, str):
        return {'statusCode': 400, "body": f"Credit risk must be a value between 1 and 10 for this pricing type"}

    df_market_simple = open_pricingband_market_simple(s3)

    Credit_Premia = df_market_simple[(df_market_simple['DimOneValMin'] < int(term)) & (df_market_simple['DimOneValMax'] >= int(term))].reset_index()
    Credit_Premia_Val = Credit_Premia.loc[(np.abs(df_market_simple["DimTwoValue"] - float(credit_risk))).idxmin(), 'Value']/100
    
    return Credit_Premia_Val

# Connections to aws resources are made outside of the handler
# function so that connections can be pooled by concurrent 
# lambda executions
s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb', region_name = 'eu-west-2')


def handler(event, context):
    """
    the handler function is used to bring the orchestration of API together. The API is triggered -> a price is evaluated -> meta data is stored in dyanamoDB table -> result is returned to client
    Args:
        event: this is a json string passed through API Gateway when invocation occurs. It includes params passed through by API users 
    Returns:
        statusCode 200: returns run results and meta data on run
        statusCode 400: advises users that the correct params wherenot supplied with invocation
    """
    try: 
        supported, params, supported_pricing_methods = product_specification(s3, json.dumps(event['queryStringParameters']['product']).strip('\"'))
    except Exception as e:
        print(e)
        return {'statusCode': 400, "body": "Please make sure that you have selected a valid product"}

    if supported == 0:
        return {'statusCode': 400, "body": f"Selected product {event['queryStringParameters']['product']} is not currently supported"}
    
    idempotency_key = str(uuid4())

    if all(param in list(event['queryStringParameters'].keys()) for param in params):

        start_time = time.process_time()
        date = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")

        if event['queryStringParameters']['pricing_type'] not in supported_pricing_methods:
            return {'statusCode': 400, "body": f"The product {event['queryStringParameters']['product']} does not support pricing method {event['queryStringParameters']['pricing_type']}"}
    
        if event['queryStringParameters']['pricing_type'] == "market":

            if 'de_run_id' in list(event['queryStringParameters'].keys()):
                price  = pricing_calc_market(s3, float(json.dumps(event['queryStringParameters']['credit_risk']).strip('\"')), json.dumps(event['queryStringParameters']['term']).strip('\"'))
                payload = create_dataframe(json.dumps(event['queryStringParameters']['product']), json.dumps(event['queryStringParameters']['credit_risk']), json.dumps(event['queryStringParameters']['term']), json.dumps(event['queryStringParameters']['amount']), json.dumps(event['queryStringParameters']['loan_id']), idempotency_key, date, str(price), json.dumps(event['queryStringParameters']['user_name']), json.dumps(event['queryStringParameters']['source_name']), pricing_type = json.dumps(event['queryStringParameters']['pricing_type']), de_run_id = json.dumps(event['queryStringParameters']['de_run_id']))
                Response = {"statusCode": 200, "output": price, "input":[{"product": event['queryStringParameters']['product'], "credit_risk": event['queryStringParameters']['credit_risk'], "amount": event['queryStringParameters']['amount'], "term": event['queryStringParameters']['term'], "loan_id": event['queryStringParameters']['loan_id']}], "meta_data":{"run_id": idempotency_key,"run_date": date}}
            
            else:
                price  = pricing_calc_market(s3, float(json.dumps(event['queryStringParameters']['credit_risk']).strip('\"')), json.dumps(event['queryStringParameters']['term']).strip('\"'))
                payload = create_dataframe(json.dumps(event['queryStringParameters']['product']), json.dumps(event['queryStringParameters']['credit_risk']), json.dumps(event['queryStringParameters']['term']), json.dumps(event['queryStringParameters']['amount']), json.dumps(event['queryStringParameters']['loan_id']), idempotency_key, date, str(price), json.dumps(event['queryStringParameters']['user_name']), json.dumps(event['queryStringParameters']['source_name']), pricing_type = json.dumps(event['queryStringParameters']['pricing_type']))
                Response = {"statusCode": 200, "output": price, "input":[{"product": event['queryStringParameters']['product'], "credit_risk": event['queryStringParameters']['credit_risk'], "amount": event['queryStringParameters']['amount'], "term": event['queryStringParameters']['term'], "loan_id": event['queryStringParameters']['loan_id']}], "meta_data":{"run_id": idempotency_key,"run_date": date}}
        
        elif event['queryStringParameters']['pricing_type'] == "market_simple":

            if 'de_run_id' in list(event['queryStringParameters'].keys()):
                price  = pricing_calc_market_simple(s3, float(json.dumps(event['queryStringParameters']['credit_risk']).strip('\"')), json.dumps(event['queryStringParameters']['term']).strip('\"'))
                payload = create_dataframe(json.dumps(event['queryStringParameters']['product']), json.dumps(event['queryStringParameters']['credit_risk']), json.dumps(event['queryStringParameters']['term']), json.dumps(event['queryStringParameters']['amount']), json.dumps(event['queryStringParameters']['loan_id']), idempotency_key, date, str(price), json.dumps(event['queryStringParameters']['user_name']), json.dumps(event['queryStringParameters']['source_name']), pricing_type = json.dumps(event['queryStringParameters']['pricing_type']), de_run_id = json.dumps(event['queryStringParameters']['de_run_id']))
                Response = {"statusCode": 200, "output": price, "input":[{"product": event['queryStringParameters']['product'], "credit_risk": event['queryStringParameters']['credit_risk'], "amount": event['queryStringParameters']['amount'], "term": event['queryStringParameters']['term'], "loan_id": event['queryStringParameters']['loan_id']}], "meta_data":{"run_id": idempotency_key,"run_date": date}}
            
            else:
                price  = pricing_calc_market_simple(s3, float(json.dumps(event['queryStringParameters']['credit_risk']).strip('\"')), json.dumps(event['queryStringParameters']['term']).strip('\"'))
                payload = create_dataframe(json.dumps(event['queryStringParameters']['product']), json.dumps(event['queryStringParameters']['credit_risk']), json.dumps(event['queryStringParameters']['term']), json.dumps(event['queryStringParameters']['amount']), json.dumps(event['queryStringParameters']['loan_id']), idempotency_key, date, str(price), json.dumps(event['queryStringParameters']['user_name']), json.dumps(event['queryStringParameters']['source_name']), pricing_type = json.dumps(event['queryStringParameters']['pricing_type']))
                Response = {"statusCode": 200, "output": price, "input":[{"product": event['queryStringParameters']['product'], "credit_risk": event['queryStringParameters']['credit_risk'], "amount": event['queryStringParameters']['amount'], "term": event['queryStringParameters']['term'], "loan_id": event['queryStringParameters']['loan_id']}], "meta_data":{"run_id": idempotency_key,"run_date": date}}
        
        else:

            if "loan_to_value" in list(event['queryStringParameters'].keys()) and 'de_run_id' not in list(event['queryStringParameters'].keys()):
                price  = pricing_calc_model(s3, json.dumps(event['queryStringParameters']['product']).strip('\"'), json.dumps(event['queryStringParameters']['credit_risk']).strip('\"'), json.dumps(event['queryStringParameters']['term']).strip('\"'), json.dumps(event['queryStringParameters']['amount']).strip('\"'), loan_to_value = json.dumps(event['queryStringParameters']['loan_to_value']).strip('\"'))
                payload = create_dataframe(json.dumps(event['queryStringParameters']['product']), json.dumps(event['queryStringParameters']['credit_risk']), json.dumps(event['queryStringParameters']['term']), json.dumps(event['queryStringParameters']['amount']), json.dumps(event['queryStringParameters']['loan_id']), idempotency_key, date, str(price), json.dumps(event['queryStringParameters']['user_name']), json.dumps(event['queryStringParameters']['source_name']), pricing_type = json.dumps(event['queryStringParameters']['pricing_type']), loan_to_value = json.dumps(event['queryStringParameters']['loan_to_value']))
                Response = {"statusCode": 200, "output": price, "input":[{"product": event['queryStringParameters']['product'], "credit_risk": event['queryStringParameters']['credit_risk'], "amount": event['queryStringParameters']['amount'], "term": event['queryStringParameters']['term'], "loan_id": event['queryStringParameters']['loan_id'], "loan_to_value": event['queryStringParameters']['loan_to_value']}], "meta_data":{"run_id": idempotency_key,"run_date": date}}

            elif "loan_to_value" not in list(event['queryStringParameters'].keys()) and 'de_run_id' in list(event['queryStringParameters'].keys()):
                price  = pricing_calc_model(s3, json.dumps(event['queryStringParameters']['product']).strip('\"'), json.dumps(event['queryStringParameters']['credit_risk']).strip('\"'), json.dumps(event['queryStringParameters']['term']).strip('\"'), json.dumps(event['queryStringParameters']['amount']).strip('\"'), de_run_id = json.dumps(event['queryStringParameters']['de_run_id']).strip('\"'))
                payload = create_dataframe(json.dumps(event['queryStringParameters']['product']), json.dumps(event['queryStringParameters']['credit_risk']), json.dumps(event['queryStringParameters']['term']), json.dumps(event['queryStringParameters']['amount']), json.dumps(event['queryStringParameters']['loan_id']), idempotency_key, date, str(price), json.dumps(event['queryStringParameters']['user_name']), json.dumps(event['queryStringParameters']['source_name']), pricing_type = json.dumps(event['queryStringParameters']['pricing_type']), de_run_id = json.dumps(event['queryStringParameters']['de_run_id']))
                Response = {"statusCode": 200, "output": price, "input":[{"product": event['queryStringParameters']['product'], "credit_risk": event['queryStringParameters']['credit_risk'], "amount": event['queryStringParameters']['amount'], "term": event['queryStringParameters']['term'], "loan_id": event['queryStringParameters']['loan_id'], "de_run_id": event['queryStringParameters']['de_run_id']}], "meta_data":{"run_id": idempotency_key,"run_date": date}}
            
            elif "loan_to_value" in list(event['queryStringParameters'].keys()) and 'de_run_id' in list(event['queryStringParameters'].keys()):
                price  = pricing_calc_model(s3, json.dumps(event['queryStringParameters']['product']).strip('\"'), json.dumps(event['queryStringParameters']['credit_risk']).strip('\"'), json.dumps(event['queryStringParameters']['term']).strip('\"'), json.dumps(event['queryStringParameters']['amount']).strip('\"'), json.dumps(event['queryStringParameters']['loan_to_value']).strip('\"'))
                payload = create_dataframe(json.dumps(event['queryStringParameters']['product']), json.dumps(event['queryStringParameters']['credit_risk']), json.dumps(event['queryStringParameters']['term']), json.dumps(event['queryStringParameters']['amount']), json.dumps(event['queryStringParameters']['loan_id']), idempotency_key, date, str(price), json.dumps(event['queryStringParameters']['user_name']), json.dumps(event['queryStringParameters']['source_name']), pricing_type = json.dumps(event['queryStringParameters']['pricing_type']), loan_to_value = json.dumps(event['queryStringParameters']['loan_to_value']), de_run_id = json.dumps(event['queryStringParameters']['de_run_id']))
                Response = {"statusCode": 200, "output": price, "input":[{"product": event['queryStringParameters']['product'], "credit_risk": event['queryStringParameters']['credit_risk'], "amount": event['queryStringParameters']['amount'], "term": event['queryStringParameters']['term'], "loan_id": event['queryStringParameters']['loan_id'], "loan_to_value": event['queryStringParameters']['loan_to_value'], "de_run_id": event['queryStringParameters']['de_run_id']}], "meta_data":{"run_id": idempotency_key,"run_date": date}}

            else:
                price  = pricing_calc_model(s3, json.dumps(event['queryStringParameters']['product']).strip('\"'), json.dumps(event['queryStringParameters']['credit_risk']).strip('\"'), json.dumps(event['queryStringParameters']['term']).strip('\"'), json.dumps(event['queryStringParameters']['amount']).strip('\"'))
                payload = create_dataframe(json.dumps(event['queryStringParameters']['product']), json.dumps(event['queryStringParameters']['credit_risk']), json.dumps(event['queryStringParameters']['term']), json.dumps(event['queryStringParameters']['amount']), json.dumps(event['queryStringParameters']['loan_id']), idempotency_key, date, str(price), json.dumps(event['queryStringParameters']['user_name']), json.dumps(event['queryStringParameters']['source_name']), pricing_type = json.dumps(event['queryStringParameters']['pricing_type']))
                Response = {"statusCode": 200, "output": price, "input":[{"product": event['queryStringParameters']['product'], "credit_risk": event['queryStringParameters']['credit_risk'], "amount": event['queryStringParameters']['amount'], "term": event['queryStringParameters']['term'], "loan_id": event['queryStringParameters']['loan_id'] }], "meta_data":{"run_id": idempotency_key,"run_date": date}}
        
        dynamodb.put_item(
            TableName = "pricing_apirunlog",
            Item = payload
        )
        
        # End the timer
        end_time = time.process_time()

        # Calculate the runtime
        runtime = end_time - start_time
        Response['meta_data']['run_time'] = runtime
        print(Response)
        return {'statusCode': 200, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps(Response)}
    else:
        return {'statusCode': 400, "body": "missing required parameters " + str(set(params) - set(list(event['queryStringParameters'].keys())))}
    