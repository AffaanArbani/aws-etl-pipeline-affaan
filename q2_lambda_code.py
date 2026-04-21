import boto3
import csv
import urllib.parse
import pymysql
import os

s3 = boto3.client('s3')

# ---------------- ENV VARIABLES ----------------
RDS_HOST = os.environ['RDS_HOST']
RDS_USER = os.environ['RDS_USER']
RDS_PASSWORD = os.environ['RDS_PASSWORD']
RDS_DB = os.environ['RDS_DB']
# ---------------------------------------------

def lambda_handler(event, context):
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key']
    )

    print("Bucket:", bucket)
    print("Key:", key)

    input_file = "/tmp/input.csv"
    output_file = "/tmp/output.csv"

    # Download file from S3
    s3.download_file(bucket, key, input_file)

    # Connect to RDS
    connection = pymysql.connect(
        host=RDS_HOST,
        user=RDS_USER,
        password=RDS_PASSWORD,
        database=RDS_DB,
        port=3306
    )

    # Process CSV
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)

        fieldnames = reader.fieldnames + ["processed"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)

        writer.writeheader()

        for row in reader:
            try:
                user_id = row.get('id')
                name = row.get('name')
                email = row.get('email')

                print("Inserting:", user_id, name, email)

                # Insert into RDS
                with connection.cursor() as cursor:
                    sql = """
                    INSERT INTO processed_data (id, name, email, processed)
                    VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(sql, (
                        user_id,
                        name,
                        email,
                        True
                    ))

                # Add processed flag in output file
                row['processed'] = 'True'
                writer.writerow(row)

            except Exception as e:
                print("Row error:", e)

    # Commit DB changes
    connection.commit()
    connection.close()

    # Upload processed file back to S3
    output_key = "processed/" + key.split("/")[-1]
    s3.upload_file(output_file, bucket, output_key)

    return {
        'statusCode': 200,
        'body': 'ETL completed: S3 + RDS working'
    }