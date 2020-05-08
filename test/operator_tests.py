import warnings
import unittest
import datetime
import os, shutil
from airflow.settings import Session
from airflow.models import Variable
from airflow.hooks.http_hook import HttpHook
from airflow.models.dagbag import DagBag
from airflow.models.taskinstance import TaskInstance, clear_task_instances

class TestPythonOperators(unittest.TestCase):
    def setUp(self):
        self.dag = DagBag().get_dag(dag_id='covid_dag')
        if not os.path.isdir(os.getcwd() + '/staging'):
            os.mkdir(os.getcwd() + '/staging')
        warnings.simplefilter("ignore", ResourceWarning)

    def test_countries_retrieval_task(self):
        task = self.dag.get_task('country_api_retrieval')
        current = datetime.datetime.now()
        task_instance = TaskInstance(task=task, execution_date=current)
        task.execute(task_instance.get_template_context())

        num_files = int(Variable.get("country_splits"))

        for i in range(num_files):
            with open(os.getcwd() + "/staging/country" + str(i) + ".txt") as file:
                first_line = file.readline()
                # Ensure that the first line is not blank
                self.assertNotEqual(first_line, "")


    def tearDown(self):
        if os.path.isdir(os.getcwd() + '/staging'):
            shutil.rmtree(os.getcwd() + '/staging')
