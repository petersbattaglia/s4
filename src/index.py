import csv
import json
import os
import requests
import sqlite3

from pathlib import Path

from flask import Flask, redirect, request, url_for, render_template, send_from_directory

from logger import get_logger
from db_manager import get_db_connection, execute_query, create_collection_table, insert_into_collection, get_collection, query_collection, does_collection_exist

logger = get_logger()

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY") or os.urandom(24)


## Initialize SQLLite SB
con, cur  = get_db_connection()


@app.route('/healthcheck', methods=['GET'])
def healthcheck():
    return {"status": "ok"}

@app.route('/healthcheck/deep', methods=['GET'])
def healthcheck_deep():

    create_collection_table("cats")
    create_collection_table("dogs")

    result = execute_query("SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';")
    tables = [row[0] for row in result]
    return {"status": "ok", "collections": tables}

@app.route('/')
def index():
    return {"response": "ok"}


@app.route('/collections/<collection_name>', methods=['GET', 'POST'])
def collection(collection_name):
    """
    Generic collection endpoint to handle GET and POST requests for any collection.
    :param collection_name: Name of the collection to interact with.
    """
    logger.info(f"Collection endpoint for: {collection_name}")
    if not does_collection_exist(collection_name):
        create_collection_table(collection_name)

    if request.method == 'POST':
        # Insert a new record into the specified collection
        payload = request.json
        try:
            logger.info(f"Inserting into {collection_name} collection: {payload}")
            insert_into_collection(collection_name, payload)
        except sqlite3.Error as e:
            logger.error(f"Error inserting into {collection_name} collection: {e}")
            return {"error": "Failed to insert record"}, 500
        return {"response": f"Record inserted into {collection_name} successfully"}, 201
    elif request.method == 'GET':
        # Retrieve records from the specified collection
        try:
            result = get_collection(collection_name, False)
            logger.info(f"Retrieved {len(result)} records from {collection_name} collection")
            return {"response": result}
        except sqlite3.Error as e:
            logger.error(f"Error retrieving from {collection_name} collection: {e}")
            return {"error": "Failed to retrieve records"}, 500


@app.route('/collections/<collection_name>/query', methods=['GET'])
def collection_query(collection_name):
    """
    Generic collection query endpoint to handle GET requests for querying a collection.
    :param collection_name: Name of the collection to query.
    """

    def normalize_field(_field_name):
        if _field_name.startswith("$."):
            return f"json_extract(payload, '{_field_name}')"
        else:
            return _field_name


    logger.info(f"Collection query endpoint for: {collection_name}")
    if not does_collection_exist(collection_name):
        create_collection_table(collection_name)

    limit = request.args.get("limit", 10)
    sort_field = normalize_field(request.args.get("sort_field", None))
    sort_direction = request.args.get("sort_direction", "ASC")
    filter_combination = request.args.get("filter_combination", "AND")

    if filter_combination not in ["AND", "OR"]:
        return {"error": f"Invalid filter_combination {filter_combination}. Must be `AND` or `OR`."}, 400

    if sort_direction not in ["ASC", "DESC"]:
        return {"error": f"Invalid sort_direction {sort_direction}. Must be `ASC` or `DESC`."}, 400

    filters = [r for r in request.args if r.startswith("filter__")]

    operators = {
        "eq": "=",
        "ne": "!=",
        "gt": ">",
        "lt": "<",
        "gte": ">=",
        "lte": "<="
    }

    filter_clauses = []

    for filter in filters:
        logger.info(f"Processing filter: {filter}")
        # Validate filter format
        filter_parts = filter.split("__")
        if len(filter_parts) < 3 or len(filter_parts) > 5:
            return {"error": f"Invalid filter format: {filter}. Expected format is 'filter__field__operator' or 'filter__field__operator__type'."}, 400
        
        filter_name = filter_parts[1]
        # Need to handle sql injection here.

        filter_operator = filter_parts[2]
        if filter_operator not in operators:
            return {"error": f"Invalid operator {filter_operator} for filter {filter_name}"}, 400

        filter_value = request.args.get(filter)
        if not filter_value:
            return {"error": f"Filter value for {filter_name} is required"}, 400
        
        filter_data_type = str

        if len(filter_parts) == 4:
            if filter_parts[3] == "str":
                filter_data_type = str
            elif filter_parts[3] == "int":
                filter_data_type = int
                try:
                    int(filter_value)
                except ValueError:
                    return {"error": f"Invalid integer value for filter {filter_name}"}, 400
            elif filter_parts[3] == "float":
                filter_data_type = float
                try:
                    float(filter_value)
                except ValueError:
                    return {"error": f"Invalid float value for filter {filter_name}"}, 400
            else:
                return {"error": f"Invalid data type {filter_parts[3]} for filter {filter_name}. Must be 'str', 'int', or 'float'"}, 400

        filter_field = normalize_field(filter_name)

        if filter_data_type == str:
            filter_value = f"'{filter_value}'"
        
        filter_clauses.append(f"{filter_field} {operators[filter_operator]} {filter_value}")

    result = query_collection(collection_name,
                                filter_clauses,
                                sort_field=sort_field,
                                sort_direction=sort_direction,
                                filter_combination=filter_combination,
                                limit=limit)

    return {"response": result}


@app.route('/collections/<collection_name>/backup', methods=['GET'])
def collection_backup(collection_name):
    """
    Backup the entire collection
    :param collection_name: Name of the collection to backup.

    Returns a dump of all records as a compressed json file.
    """

    return {"error": "Not Implemented"}, 501

if __name__ == '__main__':
    app.run(host='0.0.0.0')
