# W210 Police Deployment
# DATA LOAD Microservices

import numpy as np
import pandas as pd
from flask import Flask
from flask_restful import Resource, Api

app = Flask(__name__)
api = Api(app)

class checkService(Resource):
    def get(self):
        # Test if the service is up
        return {'result': 'success'}

class runJob(Resource):
    def get(self):
        # Run background worker to read from S3, transform and write back to S3
        return {'result': 'success'}

class getJobStatus(Resource):
    def get(self):
        # Check if the background worker is running and how much of the work is completed
        return {'result': 'success'}

class killJob(Resource):
    def get(self):
        # Check if the worker is running and kill it
        return {'result': 'success'}

api.add_resource(checkService, '/')
api.add_resource(runJob, '/runJob')
api.add_resource(getJobStatus, '/getJobStatus')
api.add_resource(getJobStatus, '/killJob')

if __name__ == '__main__':
    app.run(debug=True)
