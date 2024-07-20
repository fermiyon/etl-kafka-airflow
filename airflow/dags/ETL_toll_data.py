from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Task 1.1: Define DAG arguments
default_args = {
    "owner": "selmank",
    "start_date": datetime.today(),
    "email": "info@apache.org",
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Task 1.2: Define the DAG
dag = DAG(
    "ETL_toll_data",  # DAG id
    default_args=default_args,  # Use the default_args defined above
    description="Apache Airflow Final Assignment",  # Description of the DAG
    schedule_interval="@daily",  # Schedule the DAG to run once daily
    catchup=False,  # If False, DAG will not backfill runs before the start_date
)

# Task 2.1: Create a task to unzip data task.
# tar -xzvf tolldata.tgz -C /tolldata
unzip_data = BashOperator(
    task_id="unzip_data",
    bash_command="tar -xvf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment",
    dag=dag,
)

# Task 2.2: Submit screenshot for the Create a task to extract data from csv file task.
extract_data_from_csv = BashOperator(
    task_id="extract_data_from_csv",
    bash_command=(
        "cut -d',' -f1,2,3,4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > "
        "/home/project/airflow/dags/finalassignment/csv_data.csv"
    ),
    dag=dag,
)

# Task 2.3: Submit screenshot for the Create the extract data from tsv file task.
# cut -f5,6,7 tollplaza-data.tsv | sed 's/\t/,/g' | tr -d '\r' > tsv_data.csv
# tr -d removes a character from file \r gives error in paste command thus deleted.
# od -c -> command that display file contents, -c means characters or backlslash escapes
extract_data_from_tsv = BashOperator(
    task_id="extract_data_from_tsv",
    bash_command=(
        "cut -f5,6,7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv | "
        "sed 's/\\t/,/g' |"
        "tr -d '\\r' > /home/project/airflow/dags/finalassignment/tsv_data.csv"
    ),
    dag=dag,
)

# Task 2.4: Submit screenshot for the Create a task to extract data from fixed width file task.
# Extract the fields Type of Payment code, and Vehicle Code
# cut -c 59-67 payment-data.txt > fixed_width_data.csv | tr " " "," > fixed_width_data.csv
extract_data_from_fixed_width = BashOperator(
    task_id="extract_data_from_fixed_width",
    bash_command=(
        "cut -c 59-67 /home/project/airflow/dags/finalassignment/payment-data.txt | "
        "sed 's/ /,/g' > /home/project/airflow/dags/finalassignment/fixed_width_data.csv"
    ),
    dag=dag,
)

# Task 2.5: Submit screenshot for the Create a task to consolidate data extracted from previous tasks
# Define the consolidate_data task using the BashOperator
# paste -d',' csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data2.csv
consolidate_data = BashOperator(
    task_id="consolidate_data",
    bash_command=(
        "paste -d',' /home/project/airflow/dags/finalassignment/csv_data.csv "
        "/home/project/airflow/dags/finalassignment/tsv_data.csv "
        "/home/project/airflow/dags/finalassignment/fixed_width_data.csv > "
        "/home/project/airflow/dags/finalassignment/extracted_data.csv"
    ),
    dag=dag,
)


# Task 2.6: Submit screenshot for the Transform the data task.
# awk -F',' 'NR==1{print; next} {print $1 "," $2 "," $3 "," toupper($4) "," $5 "," $6 "," $7 "," $8 "," $9}' extracted_data.csv > transformed_data.csv
transform_data = BashOperator(
    task_id="transform_data",
    bash_command="""\
        awk -F',' '
        NR==1 {
            print;
            next
        }
        {
            print $1 "," $2 "," $3 "," toupper($4) "," $5 "," $6 "," $7 "," $8 "," $9
        }
        ' /home/project/airflow/dags/finalassignment/extracted_data.csv \
        > /home/project/airflow/dags/finalassignment/transformed_data.csv
    """,
    dag=dag,
)

# Task 2.7: Submit screenshot for the Define the task pipeline task.

(
    unzip_data
    >> extract_data_from_csv
    >> extract_data_from_tsv
    >> extract_data_from_fixed_width
    >> consolidate_data
    >> transform_data
)


# Task 3.1: Submit screenshot for the Submit the DAG task.
