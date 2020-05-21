import os
import requests
import psycopg2


def country_cases():
    db_conn = psycopg2.connect(user="*INSERT USER*", password='*INSERT PASSWORD*', host="covid.cm5sr8tqgvlt.us-east-2.rds.amazonaws.com", port="5432", database="postgres")
    country = "united-states"

    from_dates = ['2020-01-01', '2020-01-20', '2020-02-02', '2020-02-20', '2020-03-02', '2020-03-20', '2020-04-02', '2020-04-20', '2020-05-02']
    to_dates = ['2020-01-19' , '2020-02-01', '2020-02-19', '2020-03-01', '2020-03-19', '2020-04-01', '2020-04-19', '2020-05-01', '2020-05-15']

    for from_, to_ in zip(from_dates, to_dates):

        from_date = from_ + "T00:00:00Z"
        to_date = to_ + "T00:00:00Z"


        # Create the endpoint that specifies range of dates
        endpoint =  "https://api.covid19api.com/country/" + country + "?from=" + from_date + "&to=" + to_date
        resp = requests.get(endpoint)

        fields = [
            "Country", "Province", "Confirmed", "Deaths", "Recovered", "Active"
        ]

        # Dictionary for storing dates that map to dictionary of province results
        data = {}
        print(type(resp.json()))
        print(len(resp.json()))
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
        cursor = db_conn.cursor()
        cursor.executemany(
            "INSERT INTO country_cases " \
            "(country, province, confirmed, deaths, recovered, active_cases, record_date) " \
            "VALUES(%s, %s, %s, %s, %s, %s, %s)" \
            "ON CONFLICT (country, province, record_date) DO NOTHING", tuple(db_data))

        db_conn.commit()
        print("INSERTED")

    db_conn.close()

country_cases()
