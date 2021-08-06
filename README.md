<p align="center">
  <img width="128" src="https://user-images.githubusercontent.com/58973699/128501380-f3f9b918-9cdd-4329-a849-4faa645f3ab9.png">  
</p>

<p align="center">
  <a href="LICENSE.md" target="_blank">
    <img src="https://badgen.net/badge/license/MIT/blue" alt="">
  </a>
  <a href="https://github.com/hnimminh/distlock/releases" target="_blank">
    <img src="https://badgen.net/github/tag/hnimminh/distlock" alt="">
  </a>
  <a href="https://pypi.org/project/distlock" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/distlock" alt="">
  </a>
  <a href="https://pypi.org/project/distlock" target="_blank">
    <img src="https://img.shields.io/badge/download- xyz- red" alt="">
  </a>
</p>

<p align="center">
  <br>
  <strong>DistLock</strong>
  <br>
  <code>Distributed locks with Redis and Python</code>
  <br><br>
</p>


## Why DistLock ?
This library implements the DistLock algorithm introduced by [@antirez](http://antirez.com/), Due to the origin fork from [redlock](https://github.com/glasslion/redlock) has been inactive since 2015, Then this repo was born with additional implementations:

* Bug fix
* Python3 syntax improvements
* New tweak for adaptation


### Yet another ...
There are already a few redis based lock implementations in the Python world, e.g.  [retools](https://github.com/bbangert/retools),  [redis-lock](https://pypi.python.org/pypi/redis-lock/0.2.0).

However, these libraries can only work with *single-master* redis server. When the Redis master goes down, your application has to face a single point of failure. We can't rely on the master-slave replication, because Redis replication is asynchronous.

> This is an obvious race condition with the master-slave replication model :
>  1. Client A acquires the lock into the master.
>  2. The master crashes before the write to the key is transmitted to the slave.
>  3. The slave gets promoted to master.
>  4. Client B acquires the lock to the same resource A already holds a lock for. SAFETY VIOLATION!

### A quick introduction to the DistLock algorithm
To resolve this problem, the Distlock algorithm assume we have `N` Redis masters. These nodes are totally independent (no replications). In order to acquire the lock, the client will try to acquire the lock in all the N instances sequentially. If and only if the client was able to acquire the lock in the majority (`(N+1)/2`)of the instances, the lock is considered to be acquired.

The detailed description of the DistLock algorithm can be found in the Redis documentation: [Distributed locks with Redis](http://redis.io/topics/distlock).

### APIs

The `distlock.DistLock` class shares a similar API with the `threading.Lock` class in the  Python Standard Library.

#### Basic Usage

```python
from distlock import DistLock
# By default, if no redis connection details are
# provided, DistLock uses redis://127.0.0.1:6379/0
lock =  DistLock("distributed_lock")
lock.acquire()
do_something()
lock.release()
```

#### With Statement / Context Manager

As with `threading.Lock`, `distlock.DistLock` objects are context managers thus support the [With Statement](https://docs.python.org/2/reference/datamodel.html#context-managers). This way is more pythonic and recommended.

```python
from distlock import DistLock
with DistLock("distributed_lock"):
    do_something()
```

#### Specify multiple Redis nodes

```python
from distlock import DistLock
with DistLock("distributed_lock",
              connection_details=[
                {'host': 'xxx.xxx.xxx.xxx', 'port': 6379, 'db': 0},
                {'host': 'xxx.xxx.xxx.xxx', 'port': 6379, 'db': 0},
                {'host': 'xxx.xxx.xxx.xxx', 'port': 6379, 'db': 0},
                {'host': 'xxx.xxx.xxx.xxx', 'port': 6379, 'db': 0},
              ]
            ):
    do_something()
```

The `connection_details` parameter expects a list of keyword arguments for initializing Redis clients.
Other acceptable Redis client arguments  can be found on the [redis-py doc](http://redis-py.readthedocs.org/en/latest/#redis.StrictRedis).

#### Reuse Redis clients with the DistLockFactory

Usually the connection details of the Redis nodes are fixed. `DistLockFactory` can help reuse them, create multiple DistLocks but only initialize the clients once.

```python
from distlock import DistLockFactory
factory = DistLockFactory(
    connection_details=[
        {'host': 'xxx.xxx.xxx.xxx'},
        {'host': 'xxx.xxx.xxx.xxx'},
        {'host': 'xxx.xxx.xxx.xxx'},
        {'host': 'xxx.xxx.xxx.xxx'},
    ])

with factory.create_lock("distributed_lock"):
    do_something()

with factory.create_lock("another_lock"):
    do_something()
```
