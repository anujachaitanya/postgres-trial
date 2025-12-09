import os
import time
from typing import List, Dict, Any
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from src.postgres_trial.clients.sql_client import PostgresClient
import logging

logger = logging.getLogger(__name__)

QUERY_TIMEOUT_SECONDS = 15
ROW_LIMIT = 1000

FORBIDDEN_KEYWORDS = [
    "ALTER", "CREATE", "DELETE", "DROP", "INSERT",
    "TRUNCATE", "UPDATE", "GRANT", "REVOKE",
    "SET SESSION AUTHORIZATION"
]


def execute_read_query(raw_sql: str, execution_session) -> List[Dict[str, Any]]:
    upper_sql = raw_sql.upper()

    # --- 1. Security Check ---
    for keyword in FORBIDDEN_KEYWORDS:
        if keyword in upper_sql:
            raise ValueError(f"Query contains forbidden keyword: {keyword}")
    modified_sql = raw_sql

    try:
        logger.info(f"Executing query: {modified_sql}")
        result = execution_session.execute(text(modified_sql))
        logger.info(result)
        logger.info(
            f"Query completed successfully. Total rows: {result.rowcount}"
        )

        rows = result.fetchall()
        columns = result.keys()

        return [dict(zip(columns, row)) for row in rows]

    except SQLAlchemyError as e:
        raise RuntimeError(f"Database error: {e}")

    except Exception as e:
        raise e