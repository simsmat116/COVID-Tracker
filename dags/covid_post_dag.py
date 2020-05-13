import os
import requests
from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException

args = {
    'owner': 'simsmat',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['simsmat11697@gmail.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id="covid_dag",
    default_args=args,
    schedule_interval=None
)


def retrieve_summary(**kwargs):
    # Setup a hook to the API endpoint
    http = HttpHook(method="GET", http_conn_id="covid_api")
    aws_rds_hook = PostgresHook(postgres_conn_id="covid_aws_db", schema="postgres")
    rds_conn = aws_rds_hook.get_conn()

    resp = http.run("summary")

    if http.check_response(resp):
        raise AirflowException("COVID API summary endpoint returned bad response")

    # Global case is separate from the Countries
    global_summary = resp.json()["Global"]
    # Add in the Country, Slug and Date fields
    global_summary["Country"] = "Global"
    global_summary["Slug"] = "global"
    global_summary["Date"] = date.today().strftime("%Y-%m-%d") + "T00:00:00Z"

    # Get the countries list and add in Global
    countries = resp.json()["Countries"]
    countries.append(global_summary)

    # Fields to be extracted from each country dictionary
    fields = [
        "Country", "Slug", "NewConfirmed", "TotalConfirmed", "NewDeaths",
        "TotalDeaths", "NewRecovered", "TotalRecovered"
    ]

    data = []
    for country in countries:
        new_entry = []
        # Iterate the fields to extract from the country dictionary
        for field in fields:
            new_entry.append(country[field])

        # Date is special case since only the date needs to be extracted from datetime
        new_entry.append(country["Date"][:10])
        # Conver the list to tuple and add to list of data to be inserted into db
        data.append(tuple(new_entry))

    # Establish database cursor
    cursor = rds_conn.cursor()
    # Insert all of the data into the covid_summary_data table
    cursor.executemany(
        "INSERT INTO " + kwargs["summary_table"] + "" \
        "(country, slug, new_confirmed, total_confirmed, new_deaths, total_deaths, new_recovered, total_recovered, date)" \
        "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)", tuple(data)
    )

    rds_conn.commit()
    rds_conn.close()


t1 = PythonOperator(
    task_id="summary_api_retrieval",
    python_callable=retrieve_summary,
    op_kwargs={"summary_table": "covid_summary_data"},
    dag=dag
)


def retrieve_countries(**kwargs):
    # Setup a hook to the API endpoint
    http = HttpHook(method='GET', http_conn_id="covid_api")
    # Send request to the countries endpoint
    resp = http.run("countries")

    # Raise exception to cause task to fail if API provides bad response
    if http.check_response(resp):
        raise AirflowException("API Countries endpoint failure.")

    # Iterate through each country in the response
    countries = resp.json()
    block_size, index = len(countries) // int(Variable.get("country_splits")) + 1, 0

    while index < len(countries):
        # Generate the current file number for the block
        file_num = index // block_size

        # Erase contents of the existing file
        with open(os.getcwd() + kwargs["path"] + "country" + str(file_num) + ".txt", "w"):
            pass

        # Placing the appropriate date into the file
        with open(os.getcwd() + kwargs["path"] + "country" + str(file_num) + ".txt", "a") as file:
            # Iterate the current block of countries
            for country in countries[index:min(index+block_size, len(countries))]:
                file.write(country["Slug"] + "\n")

        index = index+block_size


t2 = PythonOperator(
    task_id="country_api_retrieval",
    python_callable=retrieve_countries,
    op_kwargs={"path": "/dags/staging/" },
    dag=dag
)


def country_cases(**kwargs):
    # Setup a hook to the API endpoint
    http = HttpHook(method="GET", http_conn_id="covid_api")
    aws_rds_hook = PostgresHook(postgres_conn_id="covid_aws_db", schema="postgres")
    rds_conn = aws_rds_hook.get_conn()

    with open(os.getcwd() + kwargs["path"] + kwargs["filename"]) as file:
        for line in file:
            country = line.rstrip("\n")

            # Read the latest date that is stored in Airflow Variable
            from_date = Variable.get("last_date_found") + "T00:00:00Z"
            to_date = date.today().strftime("%Y-%m-%d") + "T00:00:00Z"
            # Create the endpoint that specifies range of dates
            endpoint = "country/" + country + "?from=" + from_date + "&to=" + to_date
            resp = http.run(endpoint)

            fields = [
                "Country", "Province", "Confirmed", "Deaths", "Recovered", "Active"
            ]

            # Dictionary for storing dates that map to dictionary of province results
            data = {}
            for case in resp.json():
                record_date = case["Date"][:10]
                if record_date not in data:
                    data[record_date] = {}

                province = case["Province"] if case["Province"] else "CTRY"
                # Storing the counts in a list to make adding info into dictionary easier
                counts = [case["Confirmed"], case["Deaths"], case["Recovered"], case["Active"]]
                if province not in data[record_date]:
                    data[record_date][province] = counts
                else:
                    for i in range(4):
                        data[record_date][province][i] += counts[i]

            db_data = []
            for record_date, province_data in data.items():
                for province, counts in province_data.items():
                    new_entry = [resp.json()[0]["Country"], province]
                    new_entry.extend(counts)
                    new_entry.append(record_date)
                    db_data.append(new_entry)


            # Insert the entries found into the country_cases table
            cursor = rds_conn.cursor()
            cursor.executemany(
                "INSERT INTO " + kwargs["table_name"] + " " \
                "(country, province, confirmed, deaths, recovered, active_cases, record_date) " \
                "VALUES(%s, %s, %s, %s, %s, %s, %s)", tuple(db_data))

            rds_conn.commit()

    rds_conn.close()

parallel_tasks = []
for i in range(0, int(Variable.get("country_splits"))):
    cases_task = PythonOperator(
        task_id="retrieve_country_cases" + str(i),
        python_callable=country_cases,
        op_kwargs={
            "path": "/dags/staging/",
            "filename": "country" + str(i) + ".txt",
            "table_name": "country_cases"
        },
        dag=dag
    )

    parallel_tasks.append(cases_task)


def set_latest_date(**kwargs):
    # Establish connection to AWS database
    aws_rds_hook = PostgresHook(postgres_conn_id="covid_aws_db", schema="postgres")
    rds_conn = aws_rds_hook.get_conn()
    cursor = rds_conn.cursor()
    # Query the latest record inserted and find its date
    cursor.execute("SELECT record_date FROM " + kwargs["db"] + " ORDER BY record_date DESC LIMIT 1")
    result = cursor.fetchone()
    # Set the airflow variable to be used in next process
    Variable.set('last_date_found', result[0].strftime('%Y-%m-%d'))

t4 = PythonOperator(
    task_id="set_latest_date",
    python_callable=set_latest_date,
    op_kwargs={"db": "country_cases"},
    dag=dag
)


t1 >> t2 >> parallel_tasks >> t4
