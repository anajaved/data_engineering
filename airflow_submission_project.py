################## Exercise 1: Create imports, DAG argument, and Defintion
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

# Task 1.1: Define DAG arguments 
default_args = {
    'owner': 'Ana',
    'start_date': datetime(2024, 7, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Task 1.2: Define the DAG
dag = DAG(
    'final_project_DAG',
    default_args=default_args,
    description='final DAG project for coursework',
    schedule = timedelta(seconds=5)
)


################## Exercise 2: Create the tasks using BashOperator

# Task 2.1: Create a task to unzip data.
task1 = BashOperator(
    task_id='unzip_file',
    bash_command='unzip ecommerce.zip',
    dag=dag,
)

# Task 2.2: Create a task to extract data from csv file
task2 = BashOperator(
    task_id='extract_csv',
    bash_command='cat Ecommerce_data.csv',
    dag=dag,
)

# Task 2.3: Create a task to extract data from tsv file
task3 = BashOperator(
    task_id='extract_tsv',
    bash_command='cat Ecommerce_data_tsv.txt',
    dag=dag,
)

# Task 2.4: Create a task to extract data from fixed width file
task4 = BashOperator(
    task_id='extract_txt',
    bash_command='cat Ecommerce_data_fw.txt',
    dag=dag,
)

# Task 2.5: Create a task to consolidate data extracted from previous tasks
task5 = BashOperator(
    task_id='combine_data',
    bash_command='cat Ecommerce_data.csv Ecommerce_data_tsv.txt Ecommerce_data_fw.txt >> all.txt',
    dag=dag,
)

# Task 2.6: Transform the data
task6 = BashOperator(
    task_id='transform_data',
    bash_command='sort all.txt >> sorted.txt',
    dag=dag,
)

# Task 2.7: Define the task pipeline
task1 >> task2 >> task3 >> task4 >> task5 >> task6



################## Exercise 3 
# # Task 3.1: Submit the DAG 
# export AIRFLOW_HOME=/home/project/airflow
# echo $AIRFLOW_HOME

# # Task3.2: Unpause and trigger the DAG 
# export AIRFLOW_HOME=/home/project/airflow
# cp airflow_submission_project.py $AIRFLOW_HOME/dags

# # Task 3.3: List the DAG tasks
# airflow tasks list airflow_submission_project

# # Task 3.4: Monitor the DAG 
# Done via UI