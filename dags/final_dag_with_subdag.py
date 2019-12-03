#Import inbuilt module
import os

#Import MySQL module for mysql connectivity
import MySQLdb
import pandas as pd
from pandas.core.common import flatten

#Import Airflow module and Operator
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta,datetime
from airflow.models import Variable
from subdag import subdag_factory_load
from airflow.operators.subdag_operator import SubDagOperator


parent_dag_name = 'GDM_Sneha_SUBDAG'
child_dag_name = 'subdag_LOAD'

default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime.today(),
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        		}

dag = DAG(
        dag_id=parent_dag_name,
        default_args=default_args,
        description='A SUBDAG Example',
        schedule_interval=timedelta(days=1))


df = pd.read_csv("~/test_gdm.csv",keep_default_na = False)
etl_task_type_df = pd.read_csv("~/test2.csv",keep_default_na = False)

extract_merge_df = df[df['ETL_TASK_TYPE'] == 'EXTRACT|MERGE']
load_df = df[df['ETL_TASK_TYPE'] == 'LOAD']


EXTRACT_COMPLETE = DummyOperator(
                         	task_id='EXTRACT_COMPLETE',
                         	dag=dag)

MERGE_COMPLETE = DummyOperator(
                         	  task_id='MERGE_COMPLETE',
                              dag=dag)

LOAD_COMPLETE = DummyOperator(
                              task_id='LOAD_COMPLETE',
                              dag=dag)

ssh_conn_id = "ssh_emr"



subdag_load_task = SubDagOperator(
                          subdag = subdag_factory_load(parent_dag_name,child_dag_name,load_df,etl_task_type_df,ssh_conn_id,default_args),
                          task_id = child_dag_name,
			  dag=dag
                          )

EXTRACT_COMPLETE >> MERGE_COMPLETE >> subdag_load_task >> LOAD_COMPLETE
