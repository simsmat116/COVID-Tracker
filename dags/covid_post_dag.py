import requests
from datetime import datetime

countries = requests.get("https://api.covid19api.com/summary").json()["Countries"]

for country in countries:
    country_name = country["Country"]
    new_confirmed = country["NewConfirmed"]
    total_confirmed = country["TotalConfirmed"]
    new_deaths = country["NewDeaths"]
    total_deaths = country["TotalDeaths"]
    new_recovered = country["NewRecovered"]
    total_recovered = country["TotalRecovered"]
    date = datetime.strptime(country["Date"][0:10], "%Y-%m-%d").date()
    
