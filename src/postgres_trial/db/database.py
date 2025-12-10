import os
import time
import sqlparse
from typing import List, Dict, Any
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from src.postgres_trial.clients.sql_client import PostgresClient
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

QUERY_TIMEOUT_SECONDS = 15
ROW_LIMIT = 1000

FORBIDDEN_KEYWORDS = [
    "ALTER", "CREATE", "DELETE", "DROP", "INSERT",
    "TRUNCATE", "UPDATE", "GRANT", "REVOKE",
    "SET SESSION AUTHORIZATION"
]

def get_statement_type(token_list) :
    for token in token_list:
        if token.is_whitespace:
            continue
        if token.ttype is sqlparse.tokens.DML:
            return token.value.upper()
        if token.ttype is Keyword and str(token).upper() == 'SELECT':
            return 'SELECT'
    return None


def execute_read_query(raw_sql: str, execution_session) -> List[Dict[str, Any]]:
    upper_sql = raw_sql.upper()

    #stage 1 : check for forbidden keywords and multiple statements
    for keyword in FORBIDDEN_KEYWORDS:
        if keyword in upper_sql:
            raise ValueError(f"Query contains forbidden keyword: {keyword}")

    if raw_sql.count(';') > 1:
        raise ValueError("Multiple statements are not allowed.")


    try :
        parsed_statement = sqlparse.parse(raw_sql.strip())
        if not parsed_statements:
            raise ValueError("Invalid or empty SQL statement provided.")

        # 1. Enforce SELECT Statement Type
        statement_type = get_statement_type(parsed_statements[0])
        if statement_type != 'SELECT':
            raise ValueError(f"Only SELECT statements are allowed. Found: {statement_type}")

        # 2. Whitelist Table/Schema Access (Conceptual)
        # This is where you would traverse the AST (parsed_statements[0])
        # to find all table names and check them against your ALLOWED_TABLES list.
        # Example: check_table_whitelist(parsed_statements[0])

    except Exception as e :
        raise ValueError(f"SQL Parsing Error: Query structure is invalid or ambiguous. {e}")

    cleaned_sql = raw_sql.rstrip().rstrip(';')
    if "LIMIT" not in upper_sql:
        modified_sql = f"{cleaned_sql} LIMIT {ROW_LIMIT}"
    else:
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
        execution_session.rollback()

        if "statement_timeout" in str(e):
            raise RuntimeError("Query Execution Failed: Query timed out.")

        raise RuntimeError(f"Database error: {e}")