import pandas as pd
import boto3

# Create a sample DataFrame
data = {
    'Date': pd.date_range('2022-01-01', '2022-01-31'),
    'Value': [10, 20, 30, 40, 50] * 6
}
df_input = pd.DataFrame(data)

# Save DataFrame to S3
s3 = boto3.client('s3', endpoint_url='http://localhost:4566')  # Use LocalStack endpoint URL
bucket_name = 'nyc-duration'
input_file = 'data.parquet'
options = {'key': 'value'}  # Add any additional storage options if required

df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)

s3.upload_file(input_file, bucket_name, input_file)
