import csv
import json
import os
import requests
import sqlite3

from pathlib import Path

from flask import Flask, redirect, request, url_for, render_template, send_from_directory

from logger import get_logger
from db_manager import get_db_connection, execute_query, create_collection_table, insert_into_collection, get_collection, query_collection

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

@app.route('/collections/cats', methods=['GET', 'POST'])
def collections():
    logger.info("cats collection")
    logger.info(f"Request method: {request.method}")
    if request.method == 'POST':
        # Insert a new cat into the collection
        payload = request.json
        try:
            logger.info(f"Inserting into cats collection: {payload}")
            insert_into_collection("cats", payload)
        except sqlite3.Error as e:
            logger.error(f"Error inserting into cats collection: {e}")
            return {"error": "Failed to insert cat"}, 500
        return {"response": "Cat inserted successfully"}, 201
    elif request.method == 'GET':
        #result = get_collection("cats")
        result = query_collection("cats", "json_extract(payload, '$.details.gender') = 'F'")
        return {"response": result}



if __name__ == '__main__':
    app.run(host='0.0.0.0')
