"""
## My Astro Best Practices DAG

This is a skeleton for a best practices DAG.
"""

from airflow.decorators import dag, task
from pendulum import datetime
import logging 

# modularize your code by importing functions from a separate file
from include.utils import add23

# use the Airflow task logger to log from within custom code
task_logger = logging.getLogger("airflow.task")

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,  # Convert the docstring to a DAG Doc
    default_args={
        "retries": 3,  # Set retries to > 0
        "owner": "Astro"  # Define an owner
    },
    owner_links={
        "Astro": "mailto:my_email@my_company.com"  # Define a link for the owner
    },
    tags=["example", "best-practices"],  # Use tags to categorize your DAGs
)
def my_dag():
    
    @task(
        queue="my-astro-worker-queue"
    )
    def my_task(num1: int = 1) -> int:
        num2 = add23(x=num1)
        task_logger.info(f"num2: {num2}")
        return num2

    my_task(19)

my_dag()