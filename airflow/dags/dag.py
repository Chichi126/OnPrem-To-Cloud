from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from airflow.operators.email import EmailOperator

DBT_PROFILE_DIR = '/home/amdari/.dbt/profiles.yml'

DBT_PROJECT_DIR = '/home/amdari/apex-env/airflow/dags/dbt_project'

project_config = ProjectConfig(
    dbt_project_path = DBT_PROJECT_DIR
    )

# Define default arguments
default_args = {
    'owner': 'amdari',
    'depends_on_past': False,
    'email_on_retry': False,
    'email_on_failure': False,
    'email': ['chinyere@amdari.io'],
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 3, 18)  # Ensure start_date is included
}

# Define the DAG
with DAG(
    'data_migration',
    default_args=default_args,
    description='DAG to load data from GCS to BigQuery and perform dbt transformations',
    schedule_interval='@daily',  
    catchup=False
) as dag:

    # Define schema for transactions
    schema_transaction = [
        {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "region_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "product_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "transaction_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "discount_offered", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "payment_method", "type": "STRING", "mode": "NULLABLE"},
        {"name": "pricing_model", "type": "STRING", "mode": "NULLABLE"},
    ]

    # Define schema for customers
    schema_customers = [
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
        {"name": "country", "type": "STRING", "mode": "NULLABLE"},
        {"name": "industry", "type": "STRING", "mode": "NULLABLE"},
        {"name": "signup_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "last_interaction_date", "type": "DATE", "mode": "NULLABLE"}
    ]

    # Define table configurations
    tables = [
        {'task_id': 'ghana_customer', 'source_file': 'ghana_customers.csv.gz', 
         'destination_table': 'hybrid-flame-454115-t2.apex_db.ghana_customers', 
         'schema': schema_customers, 'write_disposition': 'WRITE_APPEND'},

        {'task_id': 'kenya_customer', 'source_file': 'kenya_customers.csv.gz', 
         'destination_table': 'hybrid-flame-454115-t2.apex_db.kenya_customers', 
         'schema': schema_customers, 'write_disposition': 'WRITE_APPEND'},

        {'task_id': 'nigeria_customer', 'source_file': 'nigeria_customers.csv.gz', 
         'destination_table': 'hybrid-flame-454115-t2.apex_db.nigeria_customers', 
         'schema': schema_customers, 'write_disposition': 'WRITE_APPEND'},

        {'task_id': 'ghana_transactions', 'source_file': 'ghana_transaction.csv.gz', 
         'destination_table': 'hybrid-flame-454115-t2.apex_db.ghana_transactions', 
         'schema': schema_transaction, 'write_disposition': 'WRITE_APPEND'},

        {'task_id': 'kenya_transactions', 'source_file': 'kenya_transaction.csv.gz', 
         'destination_table': 'hybrid-flame-454115-t2.apex_db.kenya_transactions', 
         'schema': schema_transaction, 'write_disposition': 'WRITE_APPEND'},

        {'task_id': 'nigeria_transactions', 'source_file': 'nigeria_transaction.csv.gz', 
         'destination_table': 'hybrid-flame-454115-t2.apex_db.nigeria_transactions', 
         'schema': schema_transaction, 'write_disposition': 'WRITE_APPEND'},

        {'task_id': 'loading_products', 'source_file': 'products.csv', 
         'destination_table': 'hybrid-flame-454115-t2.apex_db.products', 
         'schema': None, 'write_disposition': 'WRITE_TRUNCATE'},

        {'task_id': 'loading_regions', 'source_file': 'regions.csv', 
         'destination_table': 'hybrid-flame-454115-t2.apex_db.regions', 
         'schema': None, 'write_disposition': 'WRITE_TRUNCATE'},
    ]

    # Create GCS to BigQuery tasks
    load_table = []

    for table in tables:
        task = GCSToBigQueryOperator(
            task_id=table['task_id'],
            bucket='apexamdaribucket',
            source_objects=[table['source_file']],
            destination_project_dataset_table=table['destination_table'],
            source_format='CSV',
            compression='GZIP' if table['source_file'].endswith('.gz') else 'NONE',
            autodetect=(table['schema'] is None),
            schema_fields=table['schema'] if table['schema'] else None,
            write_disposition=table.get('write_disposition', 'WRITE_TRUNCATE'),
            skip_leading_rows=1,
            allow_jagged_rows=True,
            ignore_unknown_values=True,
            max_bad_records=10,
        )

        load_table.append(task)


    # Task to run the dbt transformation

    dbt_task_group = DbtTaskGroup(
        group_id = 'dbt_run',
        project_config = project_config,
        profile_config = ProfileConfig (
            profile_name = 'dbt_project',
            target_name = 'dev',
            profiles_yml_filepath = DBT_PROFILE_DIR
        )
    )

    # Email to send a notification for an  update

    email_notification = EmailOperator (
        task_id = 'send_notification',
        to = ['chinyere@amdari.io'],
        subject = 'AIRFLOW DATA UPLOAD NOTIFICATION -{{ds}}',
        html_content = '''
        <p> New data has been uploaded into the tables:
        <br> for both transactions and customers with their various countries
        <br> products table
        <br> regions table </p>

        <p> dbt transformation also has been done</p>
        '''
    )

    



load_table >> dbt_task_group >> email_notification
