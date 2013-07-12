import dao_redis as dao
from flask import Flask, request
app = Flask(__name__)

import redis
r = redis.StrictRedis(host = 'localhost', port = 6379, db = 0)

@app.route('/<api_key>/customer', methods = ['GET'])
def list_customers(api_key):
    return dao.list_customer(request.args.get('offset', 0),
                             request.args.get('limit', 10),
                             request.args.get('status'))

@app.route('/<api_key>/customer/<customer_id>', methods = ['GET'])
def get_customer(api_key, customer_id):
    return dao.get_customer(customer_id)

@app.route('/<api_key>/customer/<customer_id>', methods = ['DELETE'])
def get_customer(api_key, customer_id):
    return dao.remove_customer(customer_id)

@app.route('/<api_key>/customer/<customer_id>', methods = ['POST'])
def create_customer(api_key, customer_id):
    dao.create_customer(customer_id)

if __name__ == '__main__':
    app.run()


