import sys
import redis
from functools import partial

r = redis.StrictRedis(host = 'localhost', port = 6379, db = 0)

def create_participant(participant_id, data, key_function):
    key = key_function(participant_id)

    with r.pipeline(transaction=True) as pipe:
        pipe.watch(key)

        if pipe.exists(key):
            return False

        pipe.multi()
        pipe.hmset(key, data)
        pipe.zadd(key_function('@all'), 0, participant_id)  # time as score
        pipe.zadd(key_function('@available'), 0, participant_id)  # time as score

        pipe.execute()

    return True

def get_participant(participant_id, key_function):
    key = key_function(participant_id)

    return r.hgetall(key)

def delete_participant(participant_id, key_function):
    key = key_function(participant_id)

    r.zrem(key_function('@all'), participant_id)
    r.delete(key)

def list_participants(offset, size, key_function, status='all'):
    participants = r.zrange(key_function('@' + status), offset, offset + size)
    with r.pipeline(transaction=False) as pipe:
        for participant_id in participants:
            key = key_function(participant_id)
            pipe.hgetall(key)
        return pipe.execute()

def customer_key(customer_id):
    return 'customer:' + customer_id

def rep_key(rep_id):
    return 'rep:' + rep_id

create_customer = partial(create_participant, key_function=customer_key)
get_customer = partial(get_participant, key_function=customer_key)
delete_customer = partial(delete_participant, key_function=customer_key)
list_customer = partial(list_participants, key_function=customer_key)

create_rep = partial(create_participant, key_function=rep_key)
get_rep = partial(get_participant, key_function=rep_key)
delete_rep = partial(delete_participant, key_function=rep_key)
list_rep = partial(list_participants, key_function=rep_key)

def match(customer_id, rep_id):
    ckey = customer_key(customer_id)
    rkey = rep_key(rep_id)

    with r.pipeline(transaction=True) as pipe:
        pipe.watch(ckey)
        pipe.watch(rkey)
        customer_status = pipe.hget(ckey, 'status')
        rep_status = pipe.hget(rkey, 'status')
        if customer_status != 'available' or rep_status != 'available':
            return False

        pipe.multi()
        pipe.hset(ckey, 'status', 'busy')
        pipe.hset(rkey, 'status', 'busy')
        pipe.zrem(customer_key('@available'), customer_id)
        pipe.zrem(rep_key('@available'), rep_id)

        pipe.execute()

    return True

if __name__ == '__main__':
    customer_id = sys.argv[1]
    rep_id = sys.argv[2]

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


