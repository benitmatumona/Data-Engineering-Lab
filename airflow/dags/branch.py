from airflow.sdk import dag, task


@dag(
        dag_id= branch
)
def first_dag():
    
    @task.python
    def first_task(**kwargs):
        ti = kwargs["ti"]
        print("this is the first task")
        fetched_data = {"data": [1, 2, 3]}
        ti.xcom_push(key="returned result", value=fetched_data)
    
    @task.python
    def second_task():
        print("this is the first task")
    
    @task.python
    def third_task():
        print("this is the first task")
    
    first = first_task()
    second = second_task()
    third = third_task()
    first >> second >> third