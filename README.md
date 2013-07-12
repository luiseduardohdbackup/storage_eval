# Analysis of storage alternatives for OTCS (and potentially symphony)

Testing different alternatives for session storage for OTCS (and potentially symphony).

I will remove this repo after finishing the tests.

Extra doc: https://docs.google.com/a/tokbox.com/document/d/1S3IcJvwG6HsFcNQw4jF-zv2jNBdSBfkIx1F5ZlRFr38/edit?usp=sharing

## Prerequirements

Not tested but should be something like this:
```
brew install python mongodb redis
pip install flask pymongo redis
```

# Run tests

Execute all the OTCS operations on REDIS:
```
python dao_redis.py customer_id rep_id
```

Execute all the OTCS operations on MongoDB:
```
python dao_mongo.py customer_id rep_id
```

Run a HTTP Server with the GUM API and those DAOs (just for fun, not tested):
```
python server.py
```
