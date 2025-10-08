from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

project = ProjectConfig(
    dbt_project_path="/usr/local/airflow/include/pos-ia-mod-2-main",
)

profile = ProfileConfig(
    profile_name="pos-ia-mod-2-main",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_dev", # usa a Connection criada manualmente no Airflow
        profile_args={
            "database": "LAB_ONS",
            "schema": "CORE_ONS",
            "warehouse": "LAB_WH",
            "role": "ONS_DEV",
        },
    ),
)

dag = DbtDag(
    dag_id="pos-ia-mod-2-main",
    project_config=project,
    profile_config=profile,
    start_date=datetime(2025, 1, 1),
    schedule=None,  # ou "@daily"
)
