from flask import Flask, jsonify, request, render_template_string
import pandasdmx as sdmx
import pandas as pd
import json
from datetime import datetime
import time


pd.set_option('display.max_colwidth', 1000)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


app = Flask(__name__)
app.config['TIMEOUT'] = 100
app.json.sort_keys = False


@app.route('/GetDatasources')
def GetDatasources():
    response = None
    current_datetime = datetime.utcnow().isoformat() + 'Z'
    timestamp = time.time()

    try:
        DataSources = sdmx.list_sources()
        response = {
            "ServerDateTime": current_datetime,
            "TimeStamp": timestamp,
            "Result": [
                {
                    "Status": 200,
                    "Details": DataSources
                }
            ]
        }

    except Exception as e:
        response = {
            "ServerDateTime": current_datetime,
            "TimeStamp": timestamp,
            "Errors": [
                {
                    "Status": 204,
                    "Details": f"{e}"
                }
            ]
        }
    return jsonify({"response": response})


@app.route('/GetDataflows', methods=["GET"])
def GetDataflows():
    response = None
    current_datetime = datetime.utcnow().isoformat() + 'Z'
    timestamp = time.time()

    try:
        DataSource = request.args.get("DataSource")
        ecb = sdmx.Request(DataSource)
        flow_msg = ecb.dataflow()
        dataflows_series = sdmx.to_pandas(flow_msg.dataflow)
        dataflows = dataflows_series.index.to_list()
        descriptions = dataflows_series.to_list()

        output = []
        for dataflow, dataflow_description in zip(dataflows, descriptions):
            output.append({
                "Name": dataflow,
                "Description": dataflow_description,
            })

        response = {
            "ServerDateTime": current_datetime,
            "TimeStamp": timestamp,
            "Result": [
                {
                    "Status": 200,
                    "Details": output
                }
            ]
        }

    except Exception as e:
        response = {
            "ServerDateTime": current_datetime,
            "TimeStamp": timestamp,
            "Errors": [
                {
                    "Status": 401,
                    "Details": f"{e}"
                }
            ]
        }

    return jsonify({"response": response})


@app.route('/GetDatasets', methods=["GET"])
def GetDatasets():
    response = None
    current_datetime = datetime.utcnow().isoformat() + 'Z'
    timestamp = time.time()

    try:
        DataSource = request.args.get("DataSource")
        DataFlow = request.args.get("DataFlow")
        key = dict(CURRENCY=['USD', 'JPY', 'EUR'])
        params = dict(startPeriod='2016')
        ecb = sdmx.Request(DataSource)
        data = ecb.data(DataFlow, key=key, params=params)
        data = data.data[0]
        daily = [s for sk, s in data.series.items() if sk.FREQ == 'D']
        cur_df = pd.concat(sdmx.to_pandas(daily)).unstack()
        json_data = cur_df.to_json(orient="records")
        parsed = json.loads(json_data)

        response = {
            "ServerDateTime": current_datetime,
            "TimeStamp": timestamp,
            "Result": [
                {
                    "Status": 200,
                    "Details": parsed
                }
            ]
        }

    except Exception as e:
        response = {
            "ServerDateTime": current_datetime,
            "TimeStamp": timestamp,
            "Errors": [
                {
                    "Status": 401,
                    "Details": f"{e}"
                }
            ]
        }
        # Return the HTML response
    return jsonify({"response": response})


@app.route('/GetDatasetValues')
def GetDatasetValues():
    return jsonify({"response": 'need to code here'})


app.run(debug=True)
