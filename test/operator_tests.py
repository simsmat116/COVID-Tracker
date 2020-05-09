import warnings
import unittest
import datetime
import requests
import os, shutil
from airflow.settings import Session
from airflow.models import Variable
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.dagbag import DagBag
from airflow.models.taskinstance import TaskInstance, clear_task_instances

class TestPythonOperators(unittest.TestCase):
    def setUp(self):
        self.dag = DagBag().get_dag(dag_id='covid_dag')
        self.db_hook = PostgresHook(postgres_conn_id="covid_aws_db", schema="postgres")
        if not os.path.isdir(os.getcwd() + '/staging'):
            os.mkdir(os.getcwd() + '/staging')
        warnings.simplefilter("ignore", ResourceWarning)



    def test_countries_retrieval_task(self):
        task = self.dag.get_task('country_api_retrieval')
        current = datetime.datetime.now()
        task_instance = TaskInstance(task=task, execution_date=current)
        # Execute the countries task
        task.execute(task_instance.get_template_context())

        num_files = int(Variable.get("country_splits"))

        # Iterate number of files supposed to be created to ensure they exist
        for i in range(num_files):
            with open(os.getcwd() + "/staging/country" + str(i) + ".txt") as file:
                first_line = file.readline()
                # Ensure that the line is not blank
                self.assertNotEqual(first_line, "")


    def test_summary_task(self):
        # Determine number of countries from the /countries endpoint of the API
        resp = requests.get("https://api.covid19api.com/countries")
        num_countries = len(resp.json())
        task = self.dag.get_task('summary_api_retrieval')
        # Change the database to the testing one
        task.op_kwargs = {"summary_table": "test_covid_summary"}
        current = datetime.datetime.now()
        # Execute the summary task
        task_instance = TaskInstance(task=task, execution_date=current)
        task.execute(task_instance.get_template_context())

        # Estblish connection to AWS RDS
        conn = self.db_hook.get_conn()
        cursor = conn.cursor()

        # Retrieve entries inserted into the database
        cursor.execute("SELECT * FROM test_covid_summary")
        entries = cursor.fetchall()

        # Check that the number of countries + 1 (for global) were inserted into the db
        self.assertEqual(num_countries + 1, len(entries))

        cursor.execute("DELETE FROM test_covid_summary")
        conn.commit()

    def test_country_cases_task(self):
        with open(os.getcwd() + "/staging/country_test.txt", "w+") as file:
            file.write("sweden\nbelgium\ngermany")

        # The tasks are created dynamically - the first one suffices for testing
        # because they are all essentially the same
        task = self.dag.get_task("retrieve_country_cases0")
        print(task)
        task.op_kwargs = {"filename": "country_test.txt", "table_name": "test_country_cases"}
        current = datetime.datetime.now()
        task_instance = TaskInstance(task=task, execution_date=current)
        task.execute(task_instance.get_template_context())

        conn = self.db_hook.get_conn()
        cursor = conn.cursor()

        # Retrieve the results that were added into the database
        cursor.execute("SELECT DISTINCT country FROM test_country_cases ORDER BY country")
        results = cursor.fetchall()

        # Ensure that the correct entries were added
        self.assertEqual(len(results), 3)
        self.assertIn("Belgium", results[0])
        self.assertIn("Germany", results[1])
        self.assertIn("Sweden", results[2])

        cursor.execute("SELECT MAX(record_date), MIN(record_date) FROM test_country_cases")
        max_date, min_date = cursor.fetchone()

        latest_date = Variable.get("last_date_found")
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        yesterday = yesterday.strftime("%Y-%m-%d")

        self.assertEqual(latest_date, min_date.strftime("%Y-%m-%d"))
        # The endpoints get updated every night - so maximum date will be yesterday
        self.assertEqual(yesterday, max_date.strftime("%Y-%m-%d"))

        cursor.execute("DELETE FROM test_country_cases")
        conn.commit()


    def tearDown(self):
        if os.path.isdir(os.getcwd() + '/staging'):
            shutil.rmtree(os.getcwd() + '/staging')
