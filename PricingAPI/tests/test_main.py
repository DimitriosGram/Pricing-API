import unittest.mock as mock
from io import BytesIO
import json
from main import (
    create_dataframe,
    product_specification,
    open_pricingband_model,
    open_pricingband_market,
    open_pricingband_market_simple,
    pricing_calc_model,
    pricing_calc_market,
    pricing_calc_market_simple
)

# Test create_dataframe function
def test_create_dataframe():
    df = create_dataframe(
        "product",
        "credit_risk",
        "term",
        "amount",
        "loan_id",
        "run_id",
        "date",
        "price",
        "user_name",
        "source_name",
        "pricing_type",
    )
    assert isinstance(df, dict)
    assert "product" in df
    assert "credit_risk" in df
 
@mock.patch('boto3.client')
def test_product_specification(mock_boto3_client):
    
    mock_s3_response = {"Price/parquet/specifications.csv": BytesIO(
        b'Idx,Supported,Parameters,Pricing_Methods\n'
        b'Product1,1,"[""param1"", ""param2""]","[""method1"", ""method2""]"\n'
        b'Product2,0,[],[]\n'
        )}
    # Create a mock S3 client and its get_object method
    mock_s3_client = mock.Mock()
    mock_boto3_client.return_value = mock_s3_client

    def get_object(Bucket, Key):
        body = mock_s3_response[Key]
        return {'Body': body}
    
    mock_s3_client.get_object.side_effect = get_object

    # Call the function with the mock S3 client
    supported, parameters, pricing_methods = product_specification(mock_s3_client, "Product1")

    # Assert that the function returns the expected values for Product1
    assert supported == 1
    assert parameters == ["param1", "param2"]
    assert pricing_methods == ["method1", "method2"]

@mock.patch('boto3.client')
def test_open_pricingband_model(mock_boto3_client):
    # Create mock S3 responses for the files as encoded bytes
    mock_s3_responses = {
        "Pricer/parquet/finance.csv": "Name,Age\nJohn,30\nAlice,25\nBob,35".encode(),
        "Pricer/parquet/fundingcurve.csv": "Curve,Rate\nA,0.05\nB,0.03\nC,0.04".encode(),
        "Pricer/parquet/sizepremia.csv": "Size,Premia\nSmall,0.02\nMedium,0.03\nLarge,0.04".encode(),
        "Pricer/parquet/termpremia.csv": "Term,Premia\nShort,0.01\nMedium,0.02\nLong,0.03".encode(),
        "Pricer/parquet/credit_premia.csv": "Credit,Premia\nGood,0.01\nAverage,0.02\nBad,0.03".encode(),
    }
    
    # Create a mock S3 client and its get_object method
    mock_s3_client = mock.Mock()
    mock_boto3_client.return_value = mock_s3_client

    def get_object(Bucket, Key):
        body = BytesIO(mock_s3_responses[Key])
        return {'Body': body}

    mock_s3_client.get_object.side_effect = get_object

    # Call the function with the mock S3 client
    df_finance, df_fundingcurve, df_sizepremia, df_termpremia, df_creditpremia = open_pricingband_model(mock_s3_client)

    # Assert that the dataframes are not empty
    assert not df_finance.empty
    assert not df_fundingcurve.empty
    assert not df_sizepremia.empty
    assert not df_termpremia.empty
    assert not df_creditpremia.empty

@mock.patch('boto3.client')
def test_open_pricingband_market(mock_boto3_client):
    # Create mock S3 responses for the files as encoded bytes
    mock_s3_responses = {
        "Pricer/parquet/term_risk_discount.csv": "Name,Age\nJohn,30\nAlice,25\nBob,35".encode()
    }
    
    # Create a mock S3 client and its get_object method
    mock_s3_client = mock.Mock()
    mock_boto3_client.return_value = mock_s3_client

    def get_object(Bucket, Key):
        body = BytesIO(mock_s3_responses[Key])
        return {'Body': body}

    mock_s3_client.get_object.side_effect = get_object

    # Call the function with the mock S3 client
    df_discount = open_pricingband_market(mock_s3_client)

    # Assert that the dataframes are not empty
    assert not df_discount.empty

@mock.patch('boto3.client')
def test_open_pricingband_market_simple(mock_boto3_client):
    # Create mock S3 responses for the files as encoded bytes
    mock_s3_responses = {
        "Pricer/parquet/market_simple_table.csv": "Name,Age\nJohn,30\nAlice,25\nBob,35".encode()
    }
    
    # Create a mock S3 client and its get_object method
    mock_s3_client = mock.Mock()
    mock_boto3_client.return_value = mock_s3_client

    def get_object(Bucket, Key):
        body = BytesIO(mock_s3_responses[Key])
        return {'Body': body}

    mock_s3_client.get_object.side_effect = get_object

    # Call the function with the mock S3 client
    df_market_simple = open_pricingband_market_simple(mock_s3_client)

    # Assert that the dataframes are not empty
    assert not df_market_simple.empty

@mock.patch('boto3.client')
def test_pricing_calc_model(mock_boto3_client):

    # Mock S3 response for open_pricingband_model
    mock_s3_responses = {
        "Pricer/parquetf/finance.csv": "Idx,NIM\nproduct1,0.05".encode(),
        "Pricer/parquet/fundingcurve.csv": "Time(in months),product1\n30, 0.02".encode(),
        "Pricer/parquet/sizepremia.csv": "Size(in thousands),product1\n100, 0.01".encode(),
        "Pricer/parquet/termpremia.csv": "Time(in months),product1\n30, 0.015".encode(),
        "Pricer/parquet/credit_premia.csv": "Product,DimOneValMin,DimOneValMax,DimTwoVal,Value\nproduct1, 10, 50,Good, 0.025".encode()
    }
    

    # Create a mock S3 client and its get_object method
    mock_s3_client = mock.Mock()
    mock_boto3_client.return_value = mock_s3_client

    def get_object(Bucket, Key):
        body = BytesIO(mock_s3_responses[Key])
        return {'Body': body}

    mock_s3_client.get_object.side_effect = get_object

    # Call pricing_calc_model with mock data
    result = pricing_calc_model(
        mock_s3_client, 'product1', 'Good', 30, 100000, loan_to_value=None
    )

    # Calculate the expected result based on mock data
    expected_result = 5 + 0.0002 + 0.00015 + 0.0001 + 0.00025

    assert result == round(expected_result, 4)
    

@mock.patch('boto3.client')
def test_pricing_calc_market(mock_boto3_client):

    # Mock S3 response for open_pricingband_model
    mock_s3_responses = {
        "Pricer/parquet/term_risk_discount.csv": "Term,Strong,Good,Satisfactory,Weak\n30,0,500,0,0".encode(),
    }
    

    # Create a mock S3 client and its get_object method
    mock_s3_client = mock.Mock()
    mock_boto3_client.return_value = mock_s3_client

    def get_object(Bucket, Key):
        body = BytesIO(mock_s3_responses[Key])
        return {'Body': body}

    mock_s3_client.get_object.side_effect = get_object

    # Call pricing_calc_model with mock data
    result = pricing_calc_market(
        mock_s3_client, 5.1, 30
    )

    # Calculate the expected result based on mock data
    expected_result = (- 1.3333333 * 5.1 + 28.333333) - 5

    assert result == expected_result

@mock.patch('boto3.client')
def test_pricing_calc_market_simple(mock_boto3_client):

    # Mock S3 response for open_pricingband_model
    mock_s3_responses = {
        "Pricer/parquet/market_simple_table.csv": "DimOneValMin,DimOneValMax,DimTwoValue,Value\n15,45,5.1,600".encode(),
    }
    

    # Create a mock S3 client and its get_object method
    mock_s3_client = mock.Mock()
    mock_boto3_client.return_value = mock_s3_client

    def get_object(Bucket, Key):
        body = BytesIO(mock_s3_responses[Key])
        return {'Body': body}

    mock_s3_client.get_object.side_effect = get_object

    # Call pricing_calc_model with mock data
    result = pricing_calc_market_simple(
        mock_s3_client, 5.1, 30
    )

    # Calculate the expected result based on mock data
    expected_result = 6

    assert result == expected_result
