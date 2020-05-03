# COVID-Tracker

### Setup the virtual environment and install necessary dependencies
This script will check if virtual-env already exists, and will just activate it if so

<pre>
<code> sudo su </code>
<code> source ./setup.sh </code>
</pre>


### Should your heart desire and you want to learn about AWS RDS, set up an instance and connect it to Airflow
Learn how to do this using this [tutorial](https://medium.com/@klogic/apache-airflow-create-etl-pipeline-like-a-boss-e491e0b8db16)

### While in the shell, initalize the database (the previous sets the AIRFLOW_HOME variable to the location of airflow in the virtual env)

This will create the database and the airflow.cfg configuration file. You can make changes to the airflow.cfg file (such as connection to AWS rather than sqlite). Documentation for this is [here](https://airflow.apache.org/docs/stable/howto/set-config.html)

<pre><code>airflow initdb</code></pre>
