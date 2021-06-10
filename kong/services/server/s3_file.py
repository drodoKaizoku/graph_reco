from flask import Flask, request
import json
from flask import make_response
from flask_cors import CORS

import datetime
import subprocess
import os

app = Flask(__name__)
CORS(app)

@app.route("/", methods = ['POST'])
def start_pipeline():
    response = make_response("ok")
  
    date = request.data.decode('UTF-8')
    
    print(date)
    json_o = json.loads(date)
    date_to_convert = json_o['date']
    new_date = transform_date(date_to_convert)
    print(new_date)
    bash_command(new_date)

    return response

def transform_date(date):
    new_date = datetime.datetime.strptime(date,"%Y-%m-%d").strftime("%Y%m%d")
    return new_date

def bash_command(date):
    bash_command = 'docker exec -it e738a4a8f5ee sh -c "airflow trigger_dag -c \'{\\"date\\":%s}\' DATE_TRACKER " ' %date
    print(bash_command)
    os.system(bash_command)

   


if __name__ == "__main__":
    app.run(port=3001, debug=True)