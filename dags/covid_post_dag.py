import requests
from datetime import datetime
from airflow import DAG
from airflow.hooks.http_hook import HttpHook
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
    print(resp)

    # Raise exception to cause task to fail if API provides bad response
    if http.check_response(resp):
        raise AirflowException("API Countries endpoint failure.")

    # Iterate through each country in the response
    countries = resp.json()
    block_size, index = len(countries) // 3 + 1, 0

    while index < len(countries):
        # Generate the current file number for the block
        file_num = index // block_size
        # Overwriting any existing data and placing new csv header
        with open("../staging/country" + str(file_num) + ".csv", "w") as file:
            file.write("country, slug\n")

        # Placing the appropriate date into the file
        with open("../staging/country" + str(file_num) + ".csv", "a") as file:
            # Iterate the current block of countries
            for country in countries[index:min(index+block_size, len(countries))]:
                file.write(country["Country"] + ", " + country["Slug"] + "\n")

        index = index+block_size


retrieve_countries()
