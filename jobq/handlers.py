"""Built-in job handlers. A handler is called as handler(job, conn).

`job` is the claimed row as a dict. `conn` is the worker's autocommit
connection. Raising an exception fails the job; returning completes it.
"""


def noop(job, conn):
    pass


HANDLERS = {"noop": noop}
