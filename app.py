from flask import Flask, jsonify, request, render_template_string
import pandasdmx as sdmx
import pandas as pd
import json
pd.set_option('display.max_colwidth', 1000)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


app = Flask(__name__)
app.config['TIMEOUT'] = 100


@app.route('/SDMXGetAllDatasources')
def GetAllDataSourcesMD():
    DataSources = sdmx.list_sources()
    print(DataSources)
    return jsonify({"response": DataSources})


@app.route('/SDMXGetAllDataflows', methods=["POST"])
def SDMXGetAllDataflowsMD():
    data = request.get_json()
    DataSource = data["DataSource"]
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
    return jsonify({"response": output})


@app.route('/SDMXGetDataflowMD', methods=["GET"])
def SDMXGetDataflowMD():
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

    # Return the HTML response
    return jsonify(parsed)


@app.route('/SDMXGetDatasetValues')
def SDMXGetDatasetValues():
    return jsonify({"response": 'pass'})


app.run(debug=True)
