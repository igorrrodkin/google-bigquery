from google.cloud import bigquery
from google.oauth2 import service_account
import psycopg2
import io
import pandas as pd
from config import host, user, password, db_name


class Loader_to_GBQ:

    def __init__(self, key_path, project_id):
        self.key_path = key_path
        self.project_id = project_id

    def uploading_csv_to_gbq(self, dataset_id, table_id, filename):
        credentials = service_account.Credentials.from_service_account_file(
            self.key_path, scopes=[
                "https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(credentials=credentials,
                                 project=credentials.project_id)
        try:
            client.create_dataset(dataset_id)
        except:
            pass
        try:
            client.create_table(f"{self.project_id}.{dataset_id}.{table_id}")
        except:
            pass

        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True

        with open(filename, "rb") as source_file:
            job = client.load_table_from_file(
                source_file, table_ref, job_config=job_config)
        print(job.result())

    def uploading_json_to_gbq(self, dataset_id, table_id, json_file, table_schema):
        credentials = service_account.Credentials.from_service_account_file(
            self.key_path, scopes=[
                "https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(credentials=credentials,
                                 project=credentials.project_id)
        try:
            client.create_dataset(dataset_id)
        except:
            pass
        try:
            client.create_table(f"{self.project_id}.{dataset_id}.{table_id}")
        except:
            pass

        formatted_schema = []
        for row in table_schema:
            formatted_schema.append(bigquery.SchemaField(
                row['name'], row['type'], row['mode']))

        df = pd.read_json(json_file)
        json_df = df.to_json(orient='records', lines=True)

        stringio_data = io.StringIO(json_df)

        table_schema = {
            'name': 'currency',
            'type': 'STRING',
            'mode': 'NULLABLE'
        }, {
            'name': 'price',
            'type': 'FLOAT',
            'mode': 'NULLABLE'
        }
        dataset = client.dataset(dataset_id)
        table = dataset.table(table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.schema = formatted_schema
        job = client.load_table_from_file(
            stringio_data, table, job_config=job_config)
        print(job.result())


class Extractor_from_GBQ:
    def __init__(self, key_path, project_id):
        self.key_path = key_path
        self.project_id = project_id

    def get_data_from_gbq(self, dataset_id, table_id, schema):
        credentials = service_account.Credentials.from_service_account_file(
            self.key_path, scopes=[
                "https://www.googleapis.com/auth/cloud-platform"],
        )

        client = bigquery.Client(credentials=credentials,
                                 project=credentials.project_id)

        # Download a table.
        table = bigquery.TableReference.from_string(
            f"{self.project_id}.{dataset_id}.{table_id}"
        )
        rows = client.list_rows(
            table,
            schema,
        )
        dataframe = rows.to_dataframe(
            create_bqstorage_client=True,
        )
        print(dataframe)


class Exctractor_postgres():

    def __init__(self, connection):
        self.connection = connection

    def uploading_data_from_postgres(self, table_name, file_path):
        connection = self.connection
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(
                f"""COPY {table_name} TO '{file_path}' WITH DELIMITER '|' CSV HEADER;"""
            )


