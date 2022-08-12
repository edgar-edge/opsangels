import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

from tempfile import NamedTemporaryFile

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import date
import pandas as pd
from cryptography.fernet import Fernet
import os
import glob

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq

from airflow.models import Variable


AWS_S3_CONN_ID = "s3_con"
GCP_CONN_ID = "gcp_opsangels"

today = date.today()
local = Variable.get("local_path_vm")
bucket_aws = Variable.get("bucket_aws")
bucket_name = Variable.get("bucket_gcp")
key_data = Variable.get("key_data_path")
account_path = Variable.get("account_json")

def s3_extract2():
    s3 = S3Hook(AWS_S3_CONN_ID)
    s3_conn = s3.get_conn()
    keys = s3.list_keys(bucket_name=bucket_aws)
    for file in keys:
        # print("1::::", file)
        # print('path: ', local)
        s3.download_file(file, bucket_name=bucket_aws, local_path=local)
        name = ""
        name1 = str(glob.glob(local + "*")[0]).replace("['", "").replace("']", "")
        name2 = str(glob.glob(local + "*")[1]).replace("['", "").replace("']", "")
        if name1.endswith("Keys") == True:
            name = name2
        else:
            name = name1

        # print("---", name)
        os.rename(name, local + "data_encrypted.csv")


def desencrypt_file():
    with open(local + key_data, "rb") as filekey:
        key2 = filekey.read()
    fernet = Fernet(key2)

    with open(local + "data_encrypted.csv", "rb") as encrypted_file:
        encrypted = encrypted_file.read()
    # decrypting the file
    decrypted = fernet.decrypt(encrypted)

    # opening the file in write mode and
    # writing the decrypted data
    with open(local + "user_data.csv", "wb") as dec_file:
        dec_file.write(decrypted)


def moveGCS():
    gcs = GCSHook(GCP_CONN_ID)
    gcs_conn = gcs.get_conn()
    object_name = "user_data.csv"
    gcs.upload(bucket_name, object_name, filename=local + object_name)


def writeBQ():
    credentials = service_account.Credentials.from_service_account_file(
        account_path
    )
    project_id = "qwiklabs-gcp-04-dbdbe4ba5f3d"
    client = bigquery.Client(credentials=credentials, project=project_id)
    df = pd.read_csv(local + "user_data.csv")
    df = df.iloc[:, 1:]
    pandas_gbq.to_gbq(df, "opsangels_data.data_user", if_exists="append")


with DAG(
    dag_id="desencryp_data",
    start_date=datetime(2022, 2, 12),
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="s3_extract_task", python_callable=s3_extract2)
    t2 = PythonOperator(task_id="desencrypt_file", python_callable=desencrypt_file)
    t3 = PythonOperator(task_id="moveGCS", python_callable=moveGCS)
    t4 = PythonOperator(task_id="writeBQ", python_callable=writeBQ)

    t1 >> t2 >> t3 >> t4
