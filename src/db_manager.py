import sqlite3
import json
from pathlib import Path
from logger import get_logger

logger = get_logger()

DB_PATH = Path("/data/s4.db")

def get_db_connection():
    db_path = Path("/data/s4.db")
    if not db_path.exists():
        logger.info(f"Database file {db_path} does not exist. Will create a new one.")

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    
    return con, cur

def execute_query(query, params=None):
    con, cur = get_db_connection()
    if params is None:
        cur.execute(query)
    else:
        cur.execute(query, params)
    con.commit()
    return cur.fetchall()

def create_collection_table(table_name):
    """
    Create a generic collection table with the specified name.
    Will only have an ID column and a json payload column.
    :param table_name: Name of the table to create.
    """
    con, cur = get_db_connection()
    query = f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY, deleted BOOL, payload JSON)"
    cur.execute(query)
    con.commit()
    logger.info(f"Collection table {table_name} created")


def insert_into_collection(table_name, payload):
    """
    Insert a JSON payload into the specified collection table.
    :param table_name: Name of the table to insert into.
    :param payload: JSON payload to insert.
    """
    con, cur = get_db_connection()
    query = f"INSERT INTO {table_name} (deleted, payload) VALUES (?, ?)"
    cur.execute(query, (False, json.dumps(payload)))
    con.commit()
    logger.info(f"Inserted into {table_name}: {payload}")

def get_collection(table_name, deleted=False):
    """
    Retrieve all records from the specified collection table.
    :param table_name: Name of the table to retrieve from.
    :param deleted: If False, will only retrieve records that are not soft deleted.
    :return: List of records in the collection.
    """
    con, cur = get_db_connection()
    query = f"SELECT * FROM {table_name} WHERE deleted = ? ORDER BY id DESC"
    cur.execute(query, (deleted,))
    rows = cur.fetchall()
    
    return [{"id": row[0], "deleted": row[1], "payload": json.loads(row[2])} for row in rows]

def query_collection(table_name, filters=[], sort_field=None, sort_direction="ASC", filter_combination="AND", limit=10, include_deleted=False):
    """
    Query a collection table with a specific query.
    :param table_name: Name of the table to query.
    :param filters: Array of filtering clauses, joined by `filter_combination`
    :param filter_combination: Method in which to combinethe filters "AND" or "OR"
    :param limit: Number of records to return
    :param include_deleted: Include soft-deleted items.

    :return: List of records matching the query.
    """
    con, cur = get_db_connection()
    query = f"SELECT * FROM {table_name} WHERE "
    if not include_deleted:
        query += "deleted = 0 "
    
    if filters and len(filters) > 0:
        filter_clause = f" {filter_combination} ".join(filters)
        query += f" AND ({filter_clause}) "

    if sort_field:
        query += f" ORDER BY {sort_field} {sort_direction} "

    query += f"LIMIT {limit}"
    
    logger.debug(f"[query_collection] Executing query: {query}")
    cur.execute(query)
    rows = cur.fetchall()
    
    return [{"id": row[0], "deleted": row[1], "payload": json.loads(row[2])} for row in rows]

def does_collection_exist(table_name):
    """
    Check if a collection table exists.
    :param table_name: Name of the table to check.
    :return: True if the table exists, False otherwise.
    """
    con, cur = get_db_connection()
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    cur.execute(query, (table_name,))
    return cur.fetchone() is not None