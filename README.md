# COVID-Tracker

### Setup the virtual environment and install necessary dependencies
This script will check if virtual-env already exists, and will just activate it if so

<pre>
<code> sudo su </code>
<code> source ./setup.sh </code>
</pre>

### While in the shell, initalize the database (the previous sets the AIRFLOW_HOME variable to the location of airflow in the virtual env)

This will create the database and the airflow.cfg configuration file

<pre><code>airflow initdb</code></pre>
