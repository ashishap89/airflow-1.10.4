import airflow
#from airflow.example_dags.subdags.subdag import subdag
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

DAG_NAME = 'Ashish_example_subdag1'

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=args,
    schedule_interval="@once",
)
def subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=args,concurrency=3
    )

    for i in range(5):
        DummyOperator(
            task_id='%s-task-%s' % (child_dag_name, i + 1),
            default_args=args,
            dag=dag_subdag,
        )

    return dag_subdag



start = DummyOperator(
    task_id='start',
    dag=dag,
)

section_1 = SubDagOperator(
    task_id='section-1',
    subdag=subdag(DAG_NAME, 'section-1', args),
    dag=dag,
)

some_other_task = DummyOperator(
    task_id='some-other-task',
    dag=dag,
)

section_2 = SubDagOperator(
    task_id='section-2',
    subdag=subdag(DAG_NAME, 'section-2', args),
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> section_1 >> some_other_task >> section_2 >> end
