from airflow.sdk import dag, task



def first_dag():
    
    @task.python
    def first_task():
        print("this is the first task")
    
    def second_task():
        print("this is the first task")
    
    def third_task():
        print("this is the first task")
    
    first = first_task()