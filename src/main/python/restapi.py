#!/usr/bin/python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import json

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol

from accumulo import AccumuloProxy
from accumulo.ttypes import *

from flask import Flask
from flask import request

app = Flask(__name__)

client = None
login = None

def connect():
    global client
    global login
    
    transport = TSocket.TSocket('localhost', 42424)
    transport = TTransport.TFramedTransport(transport)
    protocol = TCompactProtocol.TCompactProtocol(transport)
    client = AccumuloProxy.Client(protocol)
    transport.open()

    login = client.login('root', {'password':'password'})


@app.route('/tables', methods=['GET'])
def listTables():

    tables = client.listTables(login)
    return json.dumps({'tables': list(tables)})


@app.route('/tables/<table>', methods=['POST'])
def createTable(table):
        
    if not client.tableExists(login, table):
        client.createTable(login, table, True, TimeType.MILLIS)
    return json.dumps({'success': True})


@app.route('/tables/<table>', methods=['DELETE'])
def deleteTable(table):
    
    if client.tableExists(login, table):
        client.deleteTable(login, table)
    return json.dumps({'success': True})


# parameters:
#
# e.g. /myTable/insert?row=a&colFam=b&colQual=c&colVis=public&value=value
#
#
@app.route('/tables/<table>/rows', methods=['POST'])
def insert(table):

    row = request.form.get('row', '')
    colFam = request.form.get('colFam', '')
    colQual = request.form.get('colQual', '')
    colVis = request.form.get('colVis', '')
    value = request.form.get('value', '')
    
    m = {row: [ColumnUpdate(colFam, colQual, colVis, value=value)]}
    try:
        client.updateAndFlush(login, table, m)
    except Exception as e:
        return json.dumps({'success': False, 'message': str(e)})
    
    return json.dumps({'success': True})

def kvpairToMap(e):
    key = e.key
    return {'row': key.row, 'colFam': key.colFamily, 'colQual': key.colQualifier, 'timestamp': key.timestamp, 'value': e.value}

def kvpairToTuple(e):
    key = e.key
    return (key.row, key.colFamily, key.colQualifier, key.timestamp, e.value)


# parameters:
#
# start - string
# stop - string
# cols - comma-separated list of fam:qual pairs
#
# e.g. /myTable/scan?start=a&stop=b&cols=attr:age,:expired
#
@app.route('/tables/<table>/rows', methods=['GET'])
def scan(table):

    rnge = request.args.get('range', None)
    columns = request.args.get('cols', None)

    format = request.args.get('format', 'jsontup')

    if not rnge is None:
        start, stop = rnge.split(':')
        rnge = Range(Key(row=start), True, Key(row=stop), False)

    if not columns is None:
        scols = []        
        for col in columns.split(','):
            parts = col.split(':')
            if len(parts) == 1:
                scols.append(ScanColumn(parts[0], ''))
            else:
                scols.append(ScanColumn(parts[0], parts[1]))
        columns = scols

    opts = ScanOptions(range=rnge, columns=columns)
    results = []

    try:
        cookie = client.createScanner(login, table, opts)
        batch = client.nextK(cookie, 100)

        while(len(batch.results) > 0):
            results.extend(batch.results)    
            batch = client.nextK(cookie, 100)

    except Exception as e:
        return json.dumps({'success': False, 'message': str(e)})

    if format == 'jsonmap':
        return json.dumps({'results': map(kvpairToMap, results)})
    return json.dumps({'results': map(kvpairToTuple, results)})


if __name__ == "__main__":
    connect()
    app.run(debug=True)

