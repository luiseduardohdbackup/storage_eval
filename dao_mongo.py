import sys
from functools import partial

import pymongo
client = pymongo.MongoClient()
db = client.otcs_database
customers = db.customers_collection
reps = db.reps_collection

#customers.create_index('participant_id', unique=True)
#reps.create_index('participant_id', unique=True)

def create_participant(participant_id, data, collection):
    data = dict(data.items() + { 'participant_id': participant_id, 'version': 0 }.items())
    try:
        collection.insert(data, w=1)
        return True
    except pymongo.errors.DuplicateKeyError:
        return False

def get_participant(participant_id, collection):
    return collection.find_one({ 'participant_id': participant_id })

def delete_participant(participant_id, collection):
    collection.remove({ 'participant_id': participant_id }, w=1)

def list_participants(offset, size, collection, status='all'):
    filter = { 'status': status } if status != 'all' else None
    return list(collection.find(filter, skip=offset, limit=size))

create_customer = partial(create_participant, collection=customers)
get_customer = partial(get_participant, collection=customers)
delete_customer = partial(delete_participant, collection=customers)
list_customer = partial(list_participants, collection=customers)

create_rep = partial(create_participant, collection=reps)
get_rep = partial(get_participant, collection=reps)
delete_rep = partial(delete_participant, collection=reps)
list_rep = partial(list_participants, collection=reps)

def match(customer_id, rep_id):
    customer = get_customer(customer_id)
    rep = get_rep(rep_id)

    if customer['status'] != 'available' or rep['status'] != 'available':
        return False

    result = customers.update({ '_id': customer['_id'], 'version': customer['version'] },
                     { '$set': { 'status': 'busy' }, '$inc': { 'version': 1 } }, w=1)
    if result['n'] != 1:
        return False

    result = reps.update({ '_id': rep['_id'], 'version': rep['version'] },
                { '$set': { 'status': 'busy' }}, w=1)

    if result['n'] != 1:  # Rollback customer update
        result = customers.update({ '_id': customer['_id'], 'version': customer['version'] + 1 },
                         { '$set': { 'status': 'available' }, '$inc': { 'version': 1 } }, w=1)
        return False

    return True

if __name__ == '__main__':
    customer_id = sys.argv[1]
    rep_id = sys.argv[2]

    delete_customer('aaa')
    print create_customer(customer_id, {'name': 'Customer', 'status': 'available'})
    print get_customer(customer_id)
    print list_customer(0, 2)

    print create_rep(rep_id, {'name': 'Rep', 'status': 'available'})
    print get_rep(rep_id)
    print list_rep(0, 2)

    print match(customer_id, rep_id)
    print match(customer_id, rep_id)

    print list_customer(0, 2, status='available')
    print list_rep(0, 2, status='available')

    delete_customer(customer_id)
    delete_rep(rep_id)


