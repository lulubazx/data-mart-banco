from google.cloud import bigquery


class BigQueryLoader:
    def __init__(self, project_id, raw_dataset, staging_dataset, meta_dataset, logger=None):
        self.client = bigquery.Client(project=project_id)
        self.raw_dataset = raw_dataset
        self.staging_dataset = staging_dataset
        self.meta_dataset = meta_dataset
        self.logger = logger

    def ensure_dataset(self, dataset_id):
        dataset_ref = bigquery.DatasetReference(self.client.project, dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
        except Exception:
            self.client.create_dataset(bigquery.Dataset(dataset_ref))

    def ensure_datasets(self):
        self.ensure_dataset(self.raw_dataset)
        self.ensure_dataset(self.staging_dataset)
        self.ensure_dataset(self.meta_dataset)

    def load_to_staging(self, df, table_name):
        table_id = f"{self.client.project}.{self.staging_dataset}.{table_name}"
        job = self.client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
        job.result()

    def table_exists(self, dataset_id, table_name):
        table_id = f"{self.client.project}.{dataset_id}.{table_name}"
        try:
            self.client.get_table(table_id)
            return True
        except Exception:
            return False

    def create_table_from_staging(self, table_name):
        query = (
            f"CREATE TABLE `{self.client.project}.{self.raw_dataset}.{table_name}` "
            f"AS SELECT * FROM `{self.client.project}.{self.staging_dataset}.{table_name}`"
        )
        self.client.query(query).result()

    def replace_from_staging(self, table_name):
        query = (
            f"CREATE OR REPLACE TABLE `{self.client.project}.{self.raw_dataset}.{table_name}` "
            f"AS SELECT * FROM `{self.client.project}.{self.staging_dataset}.{table_name}`"
        )
        self.client.query(query).result()

    def merge_from_staging(self, table_name, primary_key):
        staging = f"`{self.client.project}.{self.staging_dataset}.{table_name}`"
        target_sql = f"`{self.client.project}.{self.raw_dataset}.{table_name}`"
        target_table_id = f"{self.client.project}.{self.raw_dataset}.{table_name}"

        table = self.client.get_table(target_table_id)
        columns = [field.name for field in table.schema]
        update_set = ", ".join([f"T.{c}=S.{c}" for c in columns])
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"S.{c}" for c in columns])

        query = f"""
MERGE {target_sql} T
USING {staging} S
ON T.{primary_key} = S.{primary_key}
WHEN MATCHED THEN UPDATE SET {update_set}
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
"""
        self.client.query(query).result()

    def get_max_timestamp(self, table_name, timestamp_column):
        query = (
            f"SELECT MAX({timestamp_column}) AS max_ts "
            f"FROM `{self.client.project}.{self.raw_dataset}.{table_name}`"
        )
        result = self.client.query(query).result()
        for row in result:
            return row["max_ts"]
        return None
