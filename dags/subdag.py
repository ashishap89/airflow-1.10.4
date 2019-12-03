#Import Airflow module and Operator
import airflow
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import timedelta,datetime
from airflow.models import Variable
from datetime import date
from airflow.executors import LocalExecutor

def subdag_factory_load(parent_dag_name,child_dag_name,loa_df,etl_task_type_df,ssh_conn_id,args):
    subdag = DAG(
			dag_id = '{0}.{1}'.format(parent_dag_name,child_dag_name) ,
			default_args = args ,
			#catchup = False
			#executor = LocalExecutor()
			concurrency = 2
            )
	
    with subdag:

        for idx,row in loa_df.iterrows():

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

            script_loc = etl_task_type_df['SCRIPT_LOC'][1]
            script_name = etl_task_type_df['SCRIPT_NAME'][1]
            complete_script_path = script_loc+script_name

	    #t1 = DummyOperator(task_id=str(table_name)+'_LOAD')

            t1 = DummyOperator(task_id=str(table_name)+'_LOAD')

#            t1 = SSHOperator(
#                    ssh_conn_id=ssh_conn_id,
#                    task_id=str(table_name) + '_LOAD',
#                    command= 'spark-submit --num-executors '+str(num_executor)+' --executor-cores '+str(executor_cores)+' --executor-memory '+executor_mem+' --driver-memory '+driver_mem+' --driver-cores '+str(driver_cores)+' '+complete_script_path+' '+table_name  ,
#                    dag=subdag)

#default_args=args,
#dag=subdag
			       


            TML_dependencies = [t for t in dependencies if t.startswith('TML_')]

            if len(TML_dependencies) == 0:
                #t9 << t3 << t2
        	    continue
            else:
                for d in TML_dependencies:
#                    t2 = SSHOperator(
#        			        ssh_conn_id=ssh_conn_id,
#                            task_id= d +'_LOAD', 
#                            command= 'spark-submit --num-executors '+str(num_executor)+' --executor-cores '+str(executor_cores)+' --executor-memory '+executor_mem+' --driver-memory '+driver_mem+' --driver-cores '+str(driver_cores)+' '+complete_script_path+' '+table_name  ,
#                            dag=subdag)

                    t2 = DummyOperator(task_id= d + '_LOAD' )
                        	#default_args=args,
                                #dag=subdag
                               
                    t1 << t2
                    #t4 << t2

    return subdag
