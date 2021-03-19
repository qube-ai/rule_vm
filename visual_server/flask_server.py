import json
from flask import Flask, render_template
import redis
from pprint import pprint

app = Flask(__name__)
r = redis.Redis(host='localhost', port=6379, db=0)


@app.route("/")
def index():
    global r
    finalData = {}
    finalData["listOfRules"] = json.loads(r.get('list_of_rules').decode('utf-8'))
    finalData["futureTasks"] = json.loads(r.get('future_task_awaiting').decode('utf-8'))
    finalData["runningTasks"] = r.get("running_tasks").decode('utf-8')
    finalData["futureTaskCount"] = r.get("future_tasks_count").decode('utf-8')
    return render_template("index.html", data=finalData);


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000, debug=True)