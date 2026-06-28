import time
import asyncio
import functools
import time

def time_profiler(f):
    if asyncio.iscoroutinefunction(f):
        @functools.wraps(f)
        async def async_inner(*args, **kwargs):
            start_time = time.perf_counter()
            res = await f(*args, **kwargs)
            end_time = time.perf_counter() - start_time
            print(f"the time of Async function {f.__name__} is: {end_time}.")
            return res
        return async_inner
    else:
        @functools.wraps(f)
        def sync_inner(*args, **kwargs):
            start_time = time.perf_counter()
            res = f(*args, **kwargs)
            end_time = time.perf_counter() - start_time
            print(f"the time of Sync function {f.__name__} is: {end_time}.")
            return res
        return sync_inner


def sleeping(f):
    @functools.wraps(f)
    async def inner(self, *args, **kwargs):
        async with self._rlock:
            sleep_time = getattr(self, "sleep_time", 0)
            sleep_time = 0 if sleep_time < 0 else sleep_time

            await asyncio.sleep(sleep_time)
            if asyncio.iscoroutinefunction(f):
                return await f(self, *args, **kwargs)
            else:
                return f(self, *args, **kwargs)
    return inner


class _DBAsyncIterator:
    def __init__(self, db_instance):
        self._keys_iter = iter(db_instance._DB__db)
        self.sleep_time = db_instance.sleep_time

        self._rlock = db_instance._rlock

    @sleeping
    def __anext__(self):
        try:
            return next(self._keys_iter)
        except StopIteration:
            raise StopAsyncIteration


class AsyncRLock:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._owner_task = None
        self._count = 0

    async def acquire(self):
        current_task = asyncio.current_task()
        if self._owner_task is current_task:
            self._count += 1
            return
        
        await self._lock.acquire()
        self._owner_task = current_task
        self._count = 1
    
    def release(self):
        current_task = asyncio.current_task()
        if self._owner_task is not current_task:
            raise RuntimeError("Cannot release a lock you don't own.")
        
        self._count -= 1
        if self._count == 0:
            self._owner_task = None
            self._lock.release()
        
    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.release()



class DB():
    "semulation for DB with I/O Block"
    def __init__(self, db=None, sleep_time=10):
        if isinstance(db, dict):
            self.__db = db
        elif db is None:
            self.__db = {}
        else:
            raise TypeError("the DataBase nust be Dict type only or None")

        self.sleep_time = sleep_time if sleep_time > 0 else 0 
        self._keys_iter = None
        self._rlock = AsyncRLock()
    
    @sleeping
    def __getitem__(self, key):
        return self.__db[key]
    
    #await db.__setitem__(key, val)
    @sleeping
    def __setitem__(self, key, value):
        self.__db[key] = value
    @sleeping
    def __delitem__(self, key):
        del self.__db[key]
    @sleeping
    def __contains__(self, key):
        return key in self.__db
    @sleeping
    def __len__(self):
        return len(self.__db)
    
    def __aiter__(self):
        return _DBAsyncIterator(self)
    
    async def __aenter__(self):
        await self._rlock.acquire()
        await asyncio.sleep(self.sleep_time)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._rlock.release()
        await asyncio.sleep(self.sleep_time)
        return False


class Metacls(type):
    _instances = {}
    def __call__(cls, *args, **kwds):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwds)

        return cls._instances[cls]



"""
self.routes = {
    "/api/users": {
        "GET": fget_users,
        "POST": fpost_user
    },
    "/api/status": {
        "GET": fget_status
    }
}
"""
class Routing_Registry(metaclass=Metacls): #object of Metacls "a type"
    def __init__(self, routes=None):
        if routes and isinstance(routes, dict):
            self.routes = routes
        else:
            self.routes = {}
        self.handlers = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}


    def register_route_sync(self, path, methods, handler):
        time.sleep(7)
        if not methods.issubset(self.handlers):
            raise ValueError("there is no support for the set {} Just {}".format(methods, self.handlers))

        if path not in self.routes:
            self.routes[path] =  dict.fromkeys(methods, handler)
        else:
            self.routes[path].update(dict.fromkeys(methods, handler))

    def resolve_route_sync(self, path, method: str):
        time.sleep(7)
        if path not in self.routes:
            raise KeyError("Path not found")
        if method not in self.routes[path]:
            raise ValueError("Method not allowed")
        return self.routes[path][method]


    #Redis
    async def register_route_async(self, path, methods, handler):
        await asyncio.sleep(7)
        if not methods.issubset(self.handlers):
            raise ValueError("there is no support for the set {} Just {}".format(methods, self.handlers))

        if path not in self.routes:
            self.routes[path] =  dict.fromkeys(methods, handler)
        else:
            self.routes[path].update(dict.fromkeys(methods, handler))

    async def resolve_route_async(self, path, method: str):
        await asyncio.sleep(7)
        if path not in self.routes:
            raise KeyError("Path not found")
        if method not in self.routes[path]:
            raise ValueError("Method not allowed")
        return self.routes[path][method]
    
    def __str__(self):
        return str(self.routes)


def fget():
    print("get method")


def fpost():
    print("post method")



def get_user_profile_s(**kwargs):
    if "user_id" not in kwargs:
        raise ValueError("no user_id exists")
    time.sleep(3)
    return {"status": "success", "data": "profile_data"}



def process_transaction_s(**kwargs):
    needs = {'user_id', 'amount', 'currency'}
    if not needs.issubset(kwargs):
        raise ValueError("no {} exists".format(needs - kwargs.keys()))
    time.sleep(5)
    return {"status": "success", "transaction_id": 12345}



async def get_user_profile_as(**kwargs):
    if "user_id" not in kwargs:
        raise ValueError("no user_id exists")
    await asyncio.sleep(3)
    return {"status": "success", "data": "profile_data"}




async def process_transaction_as(**kwargs):
    needs = {'user_id', 'amount', 'currency'}
    if not needs.issubset(kwargs):
        raise ValueError("no {} exists".format(needs - kwargs.keys()))
    await asyncio.sleep(5)
    return {"status": "success", "transaction_id": 12345}



def create_sync_rate_limiter(capacity: int, refill_rate: float):
    tokens = capacity
    last_time = time.monotonic()
    def consume(amount=1):
        nonlocal tokens, last_time
        ctime = time.monotonic()
        t = ctime - last_time
        tokens = min(capacity, tokens + t * refill_rate)
        last_time = ctime
    
        if amount <= tokens:
            tokens -= amount
            return True
        return False
    return consume

class RateLimitExceeded(Exception):
    pass

class AsyncRateLimiter:
    def __init__(self, capacity, refill_rate, *args):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.users = {}

    async def acquire(self, user_id: str, amount: int = 1):
        if user_id not in self.users:
            self.users[user_id] = {
            "tokens": self.capacity,\
            "last_time": time.monotonic(),\
            "lock": asyncio.Lock()\
            }
        async with self.users[user_id]["lock"]:
            await asyncio.sleep(0.05)
            ctime = time.monotonic()
            t = ctime - self.users[user_id]["last_time"]
            
            current_tokens = self.users[user_id]["tokens"]
            new_tokens = min(self.capacity, current_tokens + t * self.refill_rate)
            
            self.users[user_id]["tokens"] = new_tokens
            self.users[user_id]["last_time"] = ctime
            
            if amount <= self.users[user_id]["tokens"]:
                self.users[user_id]["tokens"] -= amount
                return True
        
        raise RateLimitExceeded(f"User {user_id} Exceeded")


        
async def worker(limiter, user_id):
    try:
        x = await limiter.acquire(user_id)
        print("Limit")
    except RateLimitExceeded:
        print("RateLimitExceeded")

        
import uuid
import random
def request_generator(limit: int):
    for i in range(limit):
        yield {"req_id": str(uuid.uuid4())[:8], "user_id": random.randint(1, 3), "path": random.choice(["/2", "/3"]), "method": "POST", "payload": {"amount": 2, "currency": 2}}

class RequestLifecycle:
    def __init__(self, request_dict):
        self.req = request_dict
    async def __aenter__(self):
        print("[STARTED] Request {} from User {}".format(self.req["req_id"], self.req["user_id"]))
        return self
    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is RateLimitExceeded:
            print("[DROPPED] Request {} - Rate Limited".format(self.req["req_id"]))
            return True
        elif exc_type is ValueError:
            print("[FAILED] Request {} - Bad Payload: {}".format(self.req["req_id"], exc))
            return True
        elif exc_type is None:
            print("[SUCCESS] Request {} Completed".format(self.req["req_id"]))


async def handle_request(request_dict, limiter, rout):
    async with RequestLifecycle(request_dict) as r:
        await limiter.acquire(request_dict["user_id"])
        handler = await rout.resolve_route_async(request_dict["path"], request_dict["method"])
        payload = request_dict.get("payload", {})
        await handler(user_id=request_dict["user_id"], **payload)


@time_profiler
async def asmain():
    rout = Routing_Registry()
    await asyncio.gather(rout.register_route_async("/0", {"GET"}, fget),\
                          rout.register_route_async("/1", {"GET"}, fget),\
                          rout.register_route_async("/2", {"GET"}, fget),\
                          rout.register_route_async("/3", {"GET"}, fget),\
                          rout.register_route_async("/2", {"POST"}, fpost))
    
    await asyncio.gather(rout.resolve_route_async("/3", "GET"))
    print(rout)

    data = [{'user_id': 1, 'amount': 3, 'currency': 7},\
         {'user_id': 2, 'amount': 8, 'currency': 10}, \
            {'user_id': 1}, {'user_id': 1, 'amount': 3}]
    await asyncio.gather(get_user_profile_as(**data[0]),process_transaction_as(**data[0]),\
                        get_user_profile_as(**data[1]),process_transaction_as(**data[1]),\
                        get_user_profile_as(**data[2]),process_transaction_as(**data[2]),\
                        get_user_profile_as(**data[3]),process_transaction_as(**data[3]), return_exceptions=True)
        
    
    limiter = AsyncRateLimiter(capacity=3, refill_rate=1)
    tasks = [worker(limiter, "user_123") for _ in range(15)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    limiter = AsyncRateLimiter(capacity=3, refill_rate=0.5)
    tasks = []
    for req in request_generator(20):
        tasks.append(handle_request(req, limiter, rout))
    await asyncio.gather(*tasks) 


@time_profiler
def smain():
    rout = Routing_Registry()
    rout.register_route_sync("/0", {"GET"}, fget)
    rout.register_route_sync("/1", {"GET"}, fget)
    rout.register_route_sync("/2", {"GET"}, fget)
    rout.register_route_sync("/3", {"GET"}, fget)
    rout.resolve_route_sync("/3", "GET")
    rout.register_route_sync("/2", {"POST"}, fpost)
    print(rout)

    data = [{'user_id': 1, 'amount': 3, 'currency': 7}, {'user_id': 2, 'amount': 8, 'currency': 10}, {'user_id': 1}, {'user_id': 1, 'amount': 3}]    
    for i in data:
        try:
            get_user_profile_s(**i)
            process_transaction_s(**i)
        except ValueError:
            pass


if __name__ == "__main__":
    asyncio.run(asmain(), debug=True)
    smain()

    limiter = create_sync_rate_limiter(capacity=5.0, refill_rate=1.0)
    for i in range(8):
        print(limiter(1))
    print("-------------------")
    time.sleep(2)
    for i in range(3):
        print(limiter(1))
