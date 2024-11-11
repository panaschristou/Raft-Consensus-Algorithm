# utils.py

import threading
import time

def synchronized(lock):
    """Decorator to lock methods for thread safety."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            with lock:
                return func(*args, **kwargs)
        return wrapper
    return decorator

class RepeatingTimer(threading.Thread):
    """Timer that runs a function repeatedly at specified intervals."""
    def __init__(self, interval, function, *args, **kwargs):
        threading.Thread.__init__(self)
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.finished = threading.Event()

    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)

    def cancel(self):
        self.finished.set()
