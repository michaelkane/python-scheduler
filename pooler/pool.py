"""
Pool
====

We use a single ZSET to represent the pool.

Each element of the zset represents an 'item' and has an associated score
representing the unix timestamp at which that token can next be used.

When choosing an item from the pool, we update it's score to a time in the
future according to how long we want to lock this item for.  Choosing an item
does not remove it from the pool, but makes it unavailable to be chosen again
till it's lock has expired or it has been "replaced" back in the pool.

Returning an item to the pool simply involves updating it's score to either the
current (or a past) timestamp.  Making it immediately available to be chosen
again.  Alternatively an item being returned may have it's score set to a time
in the future representing when it should next be available for choice.

Item choice is done using ZRANGEBYSCORE which selects the least recently used
(ceteris paribus).

We use Lua scripts for speed, atomicity, and fun.
"""

import time
import uuid

from itertools import repeat, chain
from redis import StrictRedis

from .exceptions import NoItemAvailableError


class Pool(object):

    def __init__(self, host, port, db, pool_key, lock_timeout):
        """
        Pool is a class to interface with a redis "pool" of items.

        `host`, `port`, and `db` are the redis connection details for this pool.

        `pool_key` is the name of the redis key holding the ZSET that represents
        this pool.

        `lock_timeout` is the number of seconds an item may be checked-out from
        the pool (using `choose()`) before it is made available again for use
        (if not `replaced()` or `removed()`).

        On initialization we register some lua scripts with redis to ensure they
        are available for later use.  These provide us with our atomicity
        guarantees when interacting with redis.
        """
        # Set up defaults
        self.pool_key = pool_key
        self.lock_timeout = lock_timeout

        # Set up redis client
        self.redis = StrictRedis(host=host, port=port, db=db)

        # Register the lua scripts we'll be using with redis
        self.zadd_if_new_script = self.redis.register_script(
            """
            local current_score = redis.call('zscore', KEYS[1], ARGV[1])
            if current_score == nil then
              return 1
            else
              return redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])
            end
            """
        )

        self.zset_element_choose_and_alter_script = self.redis.register_script(
            """
            local min = ARGV[1]
            local max = ARGV[2]
            local new_score = ARGV[3]
            local element = redis.call('zrangebyscore', KEYS[1], min, max, 'limit', 0, 1)[1]
            if element == nil then
              return nil
            end
            redis.call('zadd', KEYS[1], new_score, element)
            return element
            """
        )


    def choose(self):
        """
        Pick an item from the pool.

        Returns an item from the pool.
        Raises NoItemAvailableError.
        """
        now = time.time()
        lock_till = now + self.lock_timeout

        min_score = '-inf'
        max_score = now
        new_score = lock_till

        element = self.zset_element_choose_and_alter_script(
            [self.pool_key],
            [min_score, max_score, new_score]
        )

        if element is None:
            raise NoItemAvailableError('Pool "%s" has no available items.' % self.pool_key)

        return self.deserialize(element)


    def replace(self, item, lock_till=None):
        """
        Return an item to the pool.

        Makes the item available for choice from the pool immediately by
        default.  Optionally the item may be locked until the unix timestamp
        given in `lock_till`.
        """
        new_score = _just_before_now() if lock_till is None else lock_till
        element = self.serialize(item)
        self.redis.zadd(self.pool_key, new_score, element)


    def add(self, item):
        """
        Add a new item to the pool.

        Safely allows the same item to be added multiple times without messing
        with it's current score in the pool.

        Returns True if item was newly added, False if the item already existed
        in the pool.
        """
        default_score = 0
        element = self.serialize(item)
        result = self.zadd_if_new_script(
            [self.pool_key],
            [default_score, element]
        )
        return bool(result)


    def remove(self, item):
        """
        Remove an item from pool.

        Returns True if item was removed, False if it wasn't even in the pool.
        """
        element = self.serialize(item)
        result = self.redis.zrem(self.pool_key, element)
        return bool(result)


    def reset(self, items, redis_batch_size=500, preserve_scores=False):
        """
        Reset the entire pool using the given iterable of items.

        This will leave the current pool intact until it has finished
        processing, at which point it switches out the old pool for the new.

        By default all items are given a score of 0, making them immediately
        available.  To preserve the score for any item in the current pool that
        will also be in the new pool, pass preserve_scores=True.  Note this will
        add some time to the operation (perhaps a few seconds for a pool of
        500000).

        Returns the number of items in the new pool.
        """
        temp_pool_key = '%s-%s' % (self.pool_key, uuid.uuid4().hex)

        # Helper to add a batch of elements to the new token pool as a single
        # redis command.
        def add_batch(elements):
            if not elements:
                return

            default_score = 0
            score_element_pairs = zip(repeat(default_score), elements)
            flattened_score_elements = chain.from_iterable(score_element_pairs)
            self.redis.zadd(
                temp_pool_key,
                *flattened_score_elements
            )

        # Iterate through the given items and add to redis in batches.
        item_count = 0
        batch_elements = []
        for item in items:
            batch_elements.append(self.serialize(item))
            item_count = item_count + 1
            if len(batch_elements) >= redis_batch_size:
                add_batch(batch_elements)
                batch_elements = []

        # Make sure we add any last elements.
        add_batch(batch_elements)

        # Do we need to ensure any scores are carried over from the old pool to
        # the new one?  Use a bit of set theory if so.
        if preserve_scores and self.redis.exists(self.pool_key):
            intersect_key = '%s-intersect' % temp_pool_key
            self.redis.zinterstore(intersect_key, [temp_pool_key, self.pool_key])
            self.redis.zunionstore(temp_pool_key, [intersect_key, temp_pool_key])
            self.redis.delete(intersect_key)

        # Replace the old pool with the new one.
        self.redis.rename(temp_pool_key, self.pool_key)

        return item_count


    def serialize(item):
        """
        TODO
        """
        return item

    def deserialize(element):
        """
        TODO
        """
        return element


def _just_before_now():
    return time.time() - 1
