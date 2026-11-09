"""Distributed locks on ZooKeeper (Part A).

Three lock classes with the same interface:
    lock = LockV2(zk, "demo")
    lock.acquire()          # blocks until this client holds the lock
    lock.release()
    lock.wakeups            # how many watch notifications this client received (herd effect)

NoLock  : no locking at all (baseline)
LockV1  : the simple recipe, Lecture 10 slide "Lock v1: Simple Lock" (given, complete)
LockV2  : the recipe on Lecture 10 slide "Lock v2: Sequential Ephemeral Znodes"  <-- YOU IMPLEMENT THIS

Rules for LockV2: do not use kazoo.recipe.lock or any other Kazoo recipe.
Use only create / get_children / exists / get / delete and watches.
"""
import threading

from kazoo.exceptions import NodeExistsError, NoNodeError


class NoLock:
    def __init__(self, zk, name):
        self.wakeups = 0

    def acquire(self):
        pass

    def release(self):
        pass


class LockV1:
    """Simple lock: everyone tries to create the same ephemeral znode /locks/<name>/lock."""

    def __init__(self, zk, name):
        self.zk = zk
        self.dir = f"/locks/{name}"
        self.path = f"{self.dir}/lock"
        self.zk.ensure_path(self.dir)
        self.wakeups = 0

    def acquire(self):
        while True:
            try:
                self.zk.create(self.path, ephemeral=True)
                return
            except NodeExistsError:
                changed = threading.Event()

                def on_change(event):
                    self.wakeups += 1
                    changed.set()

                # exists() checks AND sets the watch in one call (avoids the race on the Lock v1 slide)
                if self.zk.exists(self.path, watch=on_change):
                    changed.wait()
                # node is gone (or just changed): try again

    def release(self):
        try:
            self.zk.delete(self.path)
        except NoNodeError:
            pass


class LockV2:
    """Lock with ephemeral sequential znodes; each waiter watches only its predecessor.

    TODO (Part A2): implement acquire() and release().

    Recipe (Apache ZooKeeper docs, "Recipes and Solutions: Locks"):
      1. create  <dir>/lock-  with ephemeral=True and sequence=True; remember the path
      2. children = get_children(<dir>)   (no watch here)
      3. if my node has the lowest sequence number -> I hold the lock, return
      4. else: exists(<the node just before mine>, watch=...)
           - if it already disappeared, go to 2
           - otherwise wait for the notification, then go to 2
    Release: delete my node.

    Also count every watch notification you receive in self.wakeups.
    """

    def __init__(self, zk, name):
        self.zk = zk
        self.dir = f"/locks/{name}"
        self.zk.ensure_path(self.dir)
        self.my_path = None
        self.wakeups = 0

    def acquire(self):
        raise NotImplementedError("Part A2: implement LockV2.acquire()")

    def release(self):
        raise NotImplementedError("Part A2: implement LockV2.release()")


LOCKS = {"none": NoLock, "v1": LockV1, "v2": LockV2}
