# Exercise 1 

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
from datetime import datetime

default_arguments = {
    'owner': 'valens',
    'start_date': days_ago(0),
    'email': ['mahatma@gmail.com'],
    'email_on_failure' : True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_toll_data',
    schedule = timedelta(days=1),
    default_args = default_arguments,
    description = "Apache Airflow Final Assignment"
)

# Exercise 2 

# unzip_data
unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -xzvf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags' ,
    dag = dag,
)   

# Extact from CSV
extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d"," -f1-4 /home/project/airflow/dags/vehicle-data.csv > /home/project/airflow/dags/csv_data.csv',
    dag = dag,
)

# Extract from TSV 
extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'tr "\t" "," < /home/project/airflow/dags/tollplaza-data.tsv | cut -d"," -f5,6,7 > /home/project/airflow/dags/tsv_data.csv',
    dag=dag,
)

# Extract from fixed witdh
extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = 'cut -c 59-68 /home/project/airflow/dags/payment-data.txt | tr " " "," > /home/project/airflow/dags/fixed_width_data.csv',
    dag=dag,
)

consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste -d "," /home/project/airflow/dags/csv_data.csv /home/project/airflow/dags/tsv_data.csv /home/project/airflow/dags/fixed_width_data.csv > /home/project/airflow/dags/extracted_data.csv',
    dag=dag,
)

transform_data = BashOperator(
    task_id = 'transform',
    bash_command = 'awk -F, -v OFS=, \'{$4 = toupper($4); print}\' /home/project/airflow/dags/extracted_data.csv > /home/project/airflow/dags/transformed_data.csv',
    dag=dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data


