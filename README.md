# airflow-examples
Airflow installation steps:
**Verify python3.6+ is installed.**
  python3 --version
**Install pip3 tool**
  sudo apt install python3-pip
  --pip gets installed in “ .local/bin “ so add this path in the bash profile
  pip3 install --upgrade pip==20.2.4 (airflow requires 20.2.4)
**Create directory for Airflow installation and set the PATH**
  mkdir airflow
  cd airflow
  export AIRFLOW_HOME=~/airflow
**Install Airflow-Mysql depedency**
    sudo apt-get install libmysqlclient-dev
**Install Airflow inside the Airflow installation directory**
  pip3 install apache-airflow[mysql]
**Verify installation**
  airflow version
**Initialize the database**
  airflow db init (default database sqlite is used)
**Start Airflow webserver on port 8080**
  airflow webserver -p 8080
**Start Airflow scheduler**
  airflow scheduler
**Create Airflow user**
  airflow users create --username admin --firstname Niitn --lastname Rane --role Admin --email admin@localhost

Browse the Airflow using http://0.0.0.0:8080/
