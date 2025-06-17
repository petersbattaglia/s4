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

def insert_into_collection(table_name, payload, id=None):
    """
    Insert a JSON payload into the specified collection table.
    :param table_name: Name of the table to insert into.
    :param payload: JSON payload to insert.
    :param id: Optional ID for the record. If not provided, will auto-increment.
    """
    con, cur = get_db_connection()
    
    if id is None:
        query = f"INSERT INTO {table_name} (deleted, payload) VALUES (0, ?)"
        cur.execute(query, (json.dumps(payload),))
    else:
        query = f"INSERT INTO {table_name} (id, deleted, payload) VALUES (?, 0, ?)"
        cur.execute(query, (id, json.dumps(payload)))
    
    con.commit()
    logger.info(f"Inserted record into {table_name} with payload: {payload}")

def does_item_exist(table_name, record_id):
    """
    Check if an item exists in the specified collection table.
    :param table_name: Name of the table to check.
    :param record_id: ID of the record to check.
    :return: True if the item exists, False otherwise.
    """
    logger.debug(f"Checking existence of record {record_id} in {table_name}")
    con, cur = get_db_connection()
    query = f"SELECT 1 FROM {table_name} WHERE id = ? AND deleted = 0"
    cur.execute(query, (record_id,))
    return cur.fetchone() is not None

def modify_item_in_collection(table_name, record_id, payload):
    """
    Modify an existing record in the specified collection table.
    :param table_name: Name of the table to modify.
    :param record_id: ID of the record to modify.
    :param payload: JSON payload to update the record with.
    """
    con, cur = get_db_connection()

    if not does_item_exist(table_name, record_id):
        logger.info(f"Record {record_id} does not exist in {table_name}, inserting new record.")
        insert_into_collection(table_name, payload, id=record_id)
    else:
        logger.info(f"Modifying record {record_id} in {table_name} with payload: {payload}")
        query = f"UPDATE {table_name} SET payload = ? WHERE id = ?"
        cur.execute(query, (json.dumps(payload), record_id))
        con.commit()

def delete_from_collection(table_name, record_id):
    """
    Soft delete a record from the specified collection table.
    :param table_name: Name of the table to delete from.
    :param record_id: ID of the record to delete.
    """
    con, cur = get_db_connection()
    query = f"UPDATE {table_name} SET deleted = 1 WHERE id = ?"
    cur.execute(query, (record_id,))
    con.commit()
    logger.info(f"Soft deleted record {record_id} from {table_name}")

def count_collection(table_name, include_deleted=False):
    """
    Count the number of records in a collection table.
    :param table_name: Name of the table to count records in.
    :param include_deleted: Include soft-deleted items in the count.
    :return: Count of records in the collection.
    """
    con, cur = get_db_connection()
    query = f"SELECT COUNT(*) FROM {table_name}"
    
    if not include_deleted:
        query += " WHERE deleted = 0"
    
    cur.execute(query)
    count = cur.fetchone()[0]
    
    logger.debug(f"[count_collection] Counted {count} records in {table_name}")
    return count

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

