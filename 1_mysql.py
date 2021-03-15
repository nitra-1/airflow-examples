"""
Author Nitin Rane

Sample ETL process which takes data from from Tab1, modifies the data and inserts into Tab2
Move this file to airflow/dags

Prerequisites : 
Step - 1 
DB Structure
	Execute following queries in mysql.

	1. create database workflowtest;
	2. use workflowtest;
	3. create table tab1( name VARCHAR(100) NOT NULL);
	4. create table tab2( name VARCHAR(100) NOT NULL);
	execute following INSERT query with the data of your choice and create multiple records
	5. INSERT INTO tab1 VALUES('')

Step - 2 
DB User creation
	I have created MySQL user as below:

	CREATE USER 'airflow'@'localhost' IDENTIFIED BY 'airflow';
	GRANT ALL PRIVILEGES ON * . * TO 'airflow'@'localhost';

Step - 3
Create new MySQL connection in airflow
Open Airflow in browser 
Open Admin->Connections
Click + buttong to add new connection and enter following values:

Conn Id	- mysql_con
Host		- localhost
Schema		- workflowtest
Login		- << user created in Step -2 >>
Password	- << password created in Step -2 >>		

"""

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'ETL_mysql',
    default_args=default_args,
    start_date=days_ago(0),
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
    dag=dag,
)

############ LOAD ##################

def load_records(ds, **kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(key=None, task_ids='task_transform_records')
    mysql_hook = MySqlHook(mysql_conn_id='mysql_con',schema="workflowtest")
    connection = mysql_hook.get_conn()
    for rows in records:
        sql = 'INSERT INTO tab2(name) VALUES (%s)'
        mysql_hook.run(sql, autocommit=True, parameters=[rows[0]])
        print(" Inserted in Tab2 "+rows[0]+" ")
    return True

load_records = PythonOperator(
    task_id='task_load_records',
    provide_context=True,
    python_callable=load_records,
    dag=dag,
)


get_records >> transform_records >> load_records

