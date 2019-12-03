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

default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'start_date': datetime.today(),
	'retries': 1,
	'retry_delay': timedelta(minutes=1),
	}

dag = DAG(
	dag_id='GDM_Ashish2',
	default_args=default_args,
	description='A simple Dynamic DAG Example',
	schedule_interval=timedelta(days=1))

#task_table_name="gdm_tasks_load1"
#flag_table_name="flags"
#etl_type_table_name="etl_type"
#metadata_host = Variable.get("gdm_host")
#metadata_user = Variable.get("gdm_user")
#metadata_passwd = Variable.get("gdm_passwd")
#metadata_db = Variable.get("gdm_db")
ssh_conn_id = "ssh_emr"

#db = MySQLdb.connect(host=metadata_host, user=metadata_user, passwd=metadata_passwd, db=metadata_db)
#df = pd.read_csv("select * from tml_etl_metadata."+task_table_name, con=db)
#etl_task_type_df = pd.read_sql("select * from tml_etl_metadata."+etl_type_table_name, con=db)
	
df = pd.read_csv("~/test3.csv",keep_default_na = False)
etl_task_type_df = pd.read_csv("~/test2.csv",keep_default_na = False)

t1 = DummyOperator(
				task_id='EXTRACT_COMPLETE',
				dag=dag)

t2 = DummyOperator(
				task_id='MERGE_COMPLETE',
				dag=dag)
					
t1.set_downstream(t2)


for idx,row in df.iterrows():

	table_name = row['TABLE_NAME']
	table_type = row['TABLE_TYPE']
	etl_task_type = row['ETL_TASK_TYPE'].split('|')
	etl_proc_wid = row['ETL_PROC_WID']
	last_warehouse = row['CURRENT_DB']
	staging_db = row['STAGING_DB']
	driver_cores = row['DRIVER_CORES']
	driver_mem = row['DRIVER_MEM']
	executor_cores = row['EXECUTOR_CORES']
	executor_mem = row['EXECUTOR_MEM']
	num_executor = row['NUM_EXECUTOR']
	additional_param = row['ADDITIONAL_PARAM']
	dependencies = row['DEPENDENCIES'].split('|')
	partitions = row['PARTITIONS']
	
	for e in etl_task_type:
		if e == 'EXTRACT':
			script_loc = etl_task_type_df.loc[etl_task_type_df['ETL_TASK_TYPE'].str.contains('EXTRACT'), 'SCRIPT_LOC'][0]
			script_name = etl_task_type_df.loc[etl_task_type_df['ETL_TASK_TYPE'].str.contains('EXTRACT'), 'SCRIPT_NAME'][0]
			complete_script_path = script_loc+script_name
			
			t3 = SSHOperator(
					ssh_conn_id=ssh_conn_id,
					task_id=str(table_name)+'_'+str(e),					
					command= 'spark-submit --num-executors '+str(partitions)+' '+complete_script_path+' '+table_name+' '+str(partitions), 	
					dag=dag)
			t3 >> t1
		
		if e == 'MERGE':
			script_loc = etl_task_type_df['SCRIPT_LOC'][1]
			script_name = etl_task_type_df['SCRIPT_NAME'][1]
			complete_script_path = script_loc+script_name
			
			t3 = SSHOperator(
					ssh_conn_id=ssh_conn_id,
					task_id=str(table_name)+'_'+str(e),					
					command= 'spark-submit --num-executors '+str(num_executor)+' --executor-cores '+str(executor_cores)+' --executor-memory '+executor_mem+' --driver-memory '+driver_mem+' --driver-cores '+str(driver_cores)+' '+complete_script_path+' '+table_name  , 	
					dag=dag)
					
			t3 >> t2
			t3 << t1
		
		if e =='LOAD':
			script_loc = etl_task_type_df['SCRIPT_LOC'][1]
			script_name = etl_task_type_df['SCRIPT_NAME'][1]
			complete_script_path = script_loc+script_name
	
			t3 = SSHOperator(
					ssh_conn_id=ssh_conn_id,
					task_id=str(table_name)+'_'+str(e),					
					command= 'spark-submit --num-executors '+str(num_executor)+' --executor-cores '+str(executor_cores)+' --executor-memory '+executor_mem+' --driver-memory '+driver_mem+' --driver-cores '+str(driver_cores)+' '+complete_script_path+' '+table_name  , 	
					dag=dag)
	
			TML_dependencies = [t for t in dependencies if t.startswith('TML_')]
			
			if len(TML_dependencies) == 0:
				t3.set_upstream(t2)
				continue
			else:
				for d in TML_dependencies:
					t4 = SSHOperator(
							ssh_conn_id=ssh_conn_id,
							task_id= d +'_'+str(e),					
							command= 'spark-submit --num-executors '+str(num_executor)+' --executor-cores '+str(executor_cores)+' --executor-memory '+executor_mem+' --driver-memory '+driver_mem+' --driver-cores '+str(driver_cores)+' '+complete_script_path+' '+table_name  , 	
							dag=dag)
					t3.set_upstream(t4)
					t4.set_upstream(t2)
