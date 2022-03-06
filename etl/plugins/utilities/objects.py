import os
import io


def upload_object(aws, data, tag):
    s3, bucket = aws
    df_meta, df = data
    keys = dict(df_meta.iloc[0, :])

    file_name = str(keys['start_date']) + f".csv"

    key = os.path.join(
        keys['symbol'], tag, keys['range'],
        keys['interval'], file_name)

    meta_key = os.path.join(
        keys['symbol'], 'metadata', keys['range'],
        keys['interval'], file_name
    )

    try:
        with io.StringIO() as csv_buffer:
            df_meta.to_csv(csv_buffer, index=False)
            s3.put_object(
                Bucket=bucket, Body=csv_buffer.getvalue(), Key=meta_key)
            print(
                f"Successfully uploaded an object to S3 @ s3://{bucket}/{meta_key}")

        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            s3.put_object(Bucket=bucket, Body=csv_buffer.getvalue(), Key=key)
            print(
                f"Successfully uploaded an object to S3 @ s3://{bucket}/{key}")

    except Exception as e:
      return e
    
    return 0

