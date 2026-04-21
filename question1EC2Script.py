

question A : python script : 

import boto3
import pandas as pd

bucket = "student-etl-<yourname>"
input_file = "sales_file.csv"
output_file = "output/filtered_sales.csv"

s3 = boto3.client('s3')

# download file from S3
s3.download_file(bucket, input_file, "sales.csv")

# read csv
df = pd.read_csv("sales.csv")

# filter data
filtered_df = df[df['amount'] > 1000]

# save locally
filtered_df.to_csv("filtered_sales.csv", index=False)

# upload back to S3
s3.upload_file("filtered_sales.csv", bucket, output_file)

print("ETL completed")


