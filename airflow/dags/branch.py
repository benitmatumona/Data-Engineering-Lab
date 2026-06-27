from airflow.sdk import dag, task


@dag(
        dag_id= branch
)
def branch():
    
    @task.python
    def first_task(**kwargs):
        ti = kwargs["ti"]
        print("this is the first task")
        fetched_data = {"data": [1, 2, 3], "process": "true"}
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

    @task.python
    def fourth_task(**kwargs):
        ti = kwargs["ti"]
        print("this is the fourth task")
        fetched_data = ti.xcom_pull(task_id=third_task, key="result")
        processed_data = [num for num in fetched_data["data"]]
        ti.xcom_push(key="result", value={"data": processed_data})

    @task.branch
    def decider(**kwargs):
        ti = kwargs["ti"]
        task1_value = ti.xcom_pull(task_id=first_task, key="returned result")
        if task1_value["processed"] == "true":
            return "fifth_task"
        return "six_task"

    @task.python
    def fifth_task(**kwargs):
        ti = kwargs["ti"]
        print("this is the fifth task")
        fetched_data1 = ti.xcom_pull(task_id=second_task, key="result")
        fetched_data2 = ti.xcom_pull(task_id=third_task, key="result")
        fetched_data3 = ti.xcom_pull(task_id=fourth_task, key="result")
        processed_data = [fetched_data1, fetched_data2, fetched_data3]
        ti.xcom_push(key="result", value={"data": processed_data})

    @task.python
    def sixth_task(**kwargs):
        ti = kwargs["ti"]
        print("this is the sixth task")
        processed_data = ["process was false"]
        ti.xcom_push(key="result", value={"data": processed_data})

    first = first_task()
    second = second_task()
    third = third_task()
    fourth =fourth_task()
    fifth = fifth_task()
    sixth = sixth_task()


branch()