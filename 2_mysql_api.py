"""
Author Nitin Rane
Sample ETL process which takes data from from Tab1, modifies the data and inserts into Tab2
Prerequisites : 
Step - 1 
DB User creation
	Create MySQL user as below:
	CREATE USER 'airflow'@'localhost' IDENTIFIED BY 'airflow';
	GRANT ALL PRIVILEGES ON * . * TO 'airflow'@'localhost';
Step - 2 
DB Structure
	Execute following queries in mysql.
	1. create database workflowtest;
	2. use workflowtest;
	3. create table tab1( name VARCHAR(100) NOT NULL);
	4. create table tab2( name VARCHAR(100) NOT NULL);
	execute following INSERT query with the data of your choice and create multiple records
	5. INSERT INTO tab1 VALUES('')
Step - 3
Create new MySQL connection in airflow:
Open Airflow in browser 
Open Admin->Connections
Click + button to add new connection and enter following values:
Conn Id	- mysql_con
Host		- localhost
Schema		- workflowtest
Login		- << user created in Step -1 >>
Password	- << password created in Step -1 >>	
Port		- 3306
Step - 4
Move this file to <<airflow/dags>> directory.
Refresh the DAGS.
ETL_mysql will get added in the DAGs

"""

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pendulum
import json
from airflow.utils.email import send_email_smtp
from airflow.hooks.http_hook import HttpHook

def task_success_alert(context):
    subject = "[Airflow] DAG {0} - Task {1}: Success".format(
        context['task_instance_key_str'].split('__')[0], 
        context['task_instance_key_str'].split('__')[1]
        )
    html_content = """
    DAG: {0}<br>
    Task: {1}<br>
    Succeeded on: {2}
    """.format(
        context['task_instance_key_str'].split('__')[0], 
        context['task_instance_key_str'].split('__')[1], 
        datetime.now()
        )
    send_email_smtp("ranenitin@gmail.com", subject, html_content)


local_tz=pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'ETL_API',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    start_date=datetime(2021, 3, 21, 8, 57,tzinfo=local_tz),
    #on_success_callback=task_success_alert,
    tags=['example'],
)

# [START howto_operator_mysql]
#sql="INSERT INTO tab1 VALUES('Root')"
#insert_table_mysql_task = MySqlOperator(task_id='insert_table_mysql_task', mysql_conn_id='mysql_con', sql=sql, dag=dag)


############ EXTRACT ##################

sqlSelect="SELECT * FROM tab1"
#select_table_mysql_task = MySqlOperator(task_id='select_table_mysql_task', mysql_conn_id='mysql_con', sql=sqlSelect, dag=dag)

def get_records(ds, **kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='mysql_con',schema="workflowtest") 
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sqlSelect)
    records = cursor.fetchall()
    for rows in records:
        print("records "+rows[0]+" ")
    return records

get_records = PythonOperator(
    task_id='task_get_records',
    provide_context=True,
    python_callable=get_records,
    email_on_failure=True,
    email='ranenitin@gmail.com',
    dag=dag,
)

############ TRANSFORM ##################

def transform_records(ds, **kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(key=None, task_ids='task_get_records')
    for rows in records:
        print("records-from-xcom "+rows[0]+" ")
        rows[0]=rows[0]+" tab-2"
    return records

transform_records = PythonOperator(
    task_id='task_transform_records',
    provide_context=True,
    python_callable=transform_records,
    email_on_failure=True,
    email='ranenitin@gmail.com',
    dag=dag,
)

############ LOAD ##################

def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError

def load_records(ds, **kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(key=None, task_ids='task_transform_records')
    http_hook = HttpHook(http_conn_id='http_con',method="POST")
    for rows in records:
        http_hook.run(endpoint="users/",data=json.dumps(rows[0],default=set_default), headers={"Content-Type": "application/json"})
        print(" Inserted in Tab2 "+rows[0]+" ")
    return True

load_records = PythonOperator(
    task_id='task_load_records',
    provide_context=True,
    python_callable=load_records,
    #email_on_failure=True,
    #email='ranenitin@gmail.com',
    dag=dag,
)


get_records >> transform_records >> load_records
