# scripts/check_stage.py

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging

log = logging.getLogger(__name__)

def check_stage_exists():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    result = hook.get_first("SELECT 1 FROM MANAGE_DB.INFORMATION_SCHEMA.STAGES WHERE STAGE_NAME = 'aws_stage'")
    if result:
        log.info("Snowflake stage 'aws_stage' already exists.")
        return 'stage_already_exists'
    else:
        log.info("Snowflake stage 'aws_stage' does not exist. Creating...")
        return 'create_stage'



def stage_already_exists():
    log.info("Stage already exists. Nothing to do here.")