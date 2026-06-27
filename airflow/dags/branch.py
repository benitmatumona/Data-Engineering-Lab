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
    def second_task(**kwargs):
        ti = kwargs["ti"]
        print("this is the second task")
        fetched_data = ti.xcom_pull(task_id=first_task, key="returned result")
        processed_data = [num * 2 for num in fetched_data["data"]]
        ti.xcom_push(key="result", value={"data": processed_data})
        
    
    @task.python
    def third_task(**kwargs):
        ti = kwargs["ti"]
        print("this is the third task")
        fetched_data = ti.xcom_pull(task_id=second_task, key="result")
        processed_data = [num ** 3 for num in fetched_data["data"]]
        ti.xcom_push(key="result", value={"data": processed_data})

    
    first = first_task()
    second = second_task()
    third = third_task()
    first >> second >> third