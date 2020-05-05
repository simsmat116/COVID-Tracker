import requests
from datetime import datetime, date
from airflow import DAG
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException

# countries = requests.get("https://api.covid19api.com/summary").json()["Countries"]

# for country in countries:
#     country_name = country["Country"]
#     new_confirmed = country["NewConfirmed"]
#     total_confirmed = country["TotalConfirmed"]
#     new_deaths = country["NewDeaths"]
#     total_deaths = country["TotalDeaths"]
#     new_recovered = country["NewRecovered"]
#     total_recovered = country["TotalRecovered"]
#     date = datetime.strptime(country["Date"][0:10], "%Y-%m-%d").date()

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
    block_size, index = len(countries) // 3 + 1, 0

    while index < len(countries):
        # Generate the current file number for the block
        file_num = index // block_size

        # Erase contents of the existing file
        open("../staging/country" + str(file_num) + ".txt", "w").close()

        # Placing the appropriate date into the file
        with open("../staging/country" + str(file_num) + ".txt", "a") as file:
            # Iterate the current block of countries
            for country in countries[index:min(index+block_size, len(countries))]:
                file.write(country["Slug"] + "\n")

        index = index+block_size

def country_cases(**kwargs):
    # Setup a hook to the API endpoint
    http = HttpHook(method="GET", http_conn_id="covid_api")
    aws_rds_hook = PostgresHook(postgres_conn_id="covid_aws_db", schema="postgres")
    rds_conn = aws_rds_hook.get_conn()

    with open("../staging/country" + kwargs["file_num"] + ".txt") as file:
        for line in file:
            country = line.rstrip("\n")

            # Read the latest date that
            date_file = open("../staging/last_date.txt")
            from_date = date_file.readline()
            date_file.close()
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







#retrieve_countries()
country_cases(file_num="0")
