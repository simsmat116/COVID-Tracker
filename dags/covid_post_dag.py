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
    schedule_interval=timedelta(days=1)
)


def retrieve_summary():
    # Setup a hook to the API endpoint
    http = HttpHook(method="GET", http_conn_id="covid_api")
    aws_rds_hook = PostgresHook(postgres_conn_id="covid_aws_db", schema="postgres")
    rds_conn = aws_rds_hook.get_conn()

    resp = http.run("summary")

    if http.check_response(resp):
        raise AirflowException("COVID API summary endpoint returned bad resposne")

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
    cursor.executemany("""
        INSERT INTO covid_summary_data
        (country, slug, new_confirmed, total_confirmed, new_deaths, total_deaths, new_recovered, total_recovered, date)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(data))

    rds_conn.commit()
    rds_conn.close()


t1 = PythonOperator(
    task_id="summary_api_retrieval",
    python_callable=retrieve_summary,
    dag=dag
)


def retrieve_countries():
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
        with open(os.getcwd() + "/staging/country" + str(file_num) + ".txt", "w"):
            pass

        # Placing the appropriate date into the file
        with open(os.getcwd() + "/staging/country" + str(file_num) + ".txt", "a") as file:
            # Iterate the current block of countries
            for country in countries[index:min(index+block_size, len(countries))]:
                file.write(country["Slug"] + "\n")

        index = index+block_size


t2 = PythonOperator(
    task_id="country_api_retrieval",
    python_callable=retrieve_countries,
    dag=dag
)


def country_cases(**kwargs):
    # Setup a hook to the API endpoint
    http = HttpHook(method="GET", http_conn_id="covid_api")
    aws_rds_hook = PostgresHook(postgres_conn_id="covid_aws_db", schema="postgres")
    rds_conn = aws_rds_hook.get_conn()

    with open("/staging/country" + kwargs["file_num"] + ".txt") as file:
        for line in file:
            country = line.rstrip("\n")

            # Read the latest date that is stored in Airflow Variable
            from_date = Variable.get("last_date_found")
            to_date = date.today().strftime("%Y-%m-%d") + "T00:00:00Z"
            print(to_date)
            # Create the endpoint that specifies range of dates
            endpoint = "country/" + country + "?from=" + from_date + "&to=" + to_date
            resp = http.run(endpoint)

            cursor = rds_conn.cursor()
            for case in resp.json():
                country = case["Country"]
                latitude = case["Lat"]
                longitude = case["Lon"]
                confirmed = case["Confirmed"]
                deaths = case["Deaths"]
                recovered = case["Recovered"]
                active = case["Active"]
                record_date = case["Date"][:10]

                # Insert the information found into the country_cases table
                cursor.execute("""
                    INSERT INTO country_cases
                    (country, latitude, longitude, confirmed, deaths, recovered, active_cases, record_date)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (country, latitude, longitude, confirmed, deaths, recovered, active, record_date))
                rds_conn.commit()

    rds_conn.close()


parallel_tasks = []
for i in range(0, int(Variable.get("country_splits"))):
    cases_task = PythonOperator(
        task_id="retrieve_country_cases" + str(i),
        python_callable=country_cases,
        op_kwargs={ "file_num": str(i) },
        dag=dag
    )

    parallel_tasks.append(cases_task)


def set_latest_date():
    # Establish connection to AWS database
    aws_rds_hook = PostgresHook(postgres_conn_id="covid_aws_db", schema="postgres")
    rds_conn = aws_rds_hook.get_conn()
    cursor = rds_conn.cursor()
    # Query the latest record inserted and find its date
    cursor.execute("SELECT record_date FROM country_cases ORDER BY record_date DESC LIMIT 1")
    result = cursor.fetchone()
    # Set the airflow variable to be used in next process
    print(result[0].strftime('%Y-%m-%d'))
    Variable.set('last_date_found', result[0].strftime('%Y-%m-%d'))

t4 = PythonOperator(
    task_id="set_latest_date",
    python_callable=set_latest_date,
    dag=dag
)

t1
t2 >> parallel_tasks >> t4
