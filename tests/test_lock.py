from distlock import DistLock, ReentrantDistLock, DistLockError
from distlock.distlock import CLOCK_DRIFT_FACTOR
import mock
import time
import unittest


def test_default_connection_details_value():
    """
    Test that DistLock instance could be created with
    default value of `connection_details` argument.
    """
    DistLock("test_simple_lock")


def test_simple_lock():
    """
    Test a DistLock can be acquired.
    """
    lock = DistLock("test_simple_lock", [{"host": "localhost"}], ttl=1000)
    locked = lock.acquire()
    lock.release()
    assert locked is True


def test_lock_is_locked():
    lock = DistLock("test_lock_is_locked")
    # Clear possible initial states
    [node.delete(lock.resource) for node in lock.redis_nodes]

    assert lock.locked() is False

    lock.acquire()
    assert lock.locked() is True

    lock.release()
    assert lock.locked() is False


def test_locked_span_lock_instances():
    lock1 = DistLock("test_locked_span_lock_instances")
    lock2 = DistLock("test_locked_span_lock_instances")
    # Clear possible initial states
    [node.delete(lock1.resource) for node in lock1.redis_nodes]

    assert lock1.locked() == lock2.locked() is False
    lock1.acquire()

    assert lock1.locked() == lock2.locked() is True

    lock1.release()
    assert lock1.locked() == lock2.locked() is False


def test_lock_with_validity():
    """
    Test a DistLock can be acquired and the lock validity is also retruned.
    """
    ttl = 1000
    lock = DistLock("test_simple_lock", [{"host": "localhost"}], ttl=ttl)
    locked, validity = lock.acquire_with_validity()
    lock.release()
    assert locked is True
    assert 0 < validity < ttl - ttl * CLOCK_DRIFT_FACTOR - 2


def test_from_url():
    """
    Test a DistLock can be acquired via from_url.
    """
    lock = DistLock("test_from_url", [{"url": "redis://localhost/0"}], ttl=1000)
    locked = lock.acquire()
    lock.release()
    assert locked is True


def test_context_manager():
    """
    Test a DistLock can be released by the context manager automically.

    """
    ttl = 1000
    with DistLock("test_context_manager", [{"host": "localhost"}], ttl=ttl) as validity:
        assert 0 < validity < ttl - ttl * CLOCK_DRIFT_FACTOR - 2
        lock = DistLock("test_context_manager", [{"host": "localhost"}], ttl=ttl)
        locked = lock.acquire()
        assert locked is False

    lock = DistLock("test_context_manager", [{"host": "localhost"}], ttl=ttl)
    locked = lock.acquire()
    assert locked is True

    # try to lock again within a with block
    try:
        with DistLock("test_context_manager", [{"host": "localhost"}]):
            # shouldn't be allowed since someone has the lock already
            assert False
    except DistLockError:
        # we expect this call to error out
        pass

    lock.release()


def test_fail_to_lock_acquired():
    lock1 = DistLock("test_fail_to_lock_acquired", [{"host": "localhost"}], ttl=1000)
    lock2 = DistLock("test_fail_to_lock_acquired", [{"host": "localhost"}], ttl=1000)

    lock1_locked = lock1.acquire()
    lock2_locked = lock2.acquire()
    lock1.release()

    assert lock1_locked is True
    assert lock2_locked is False


def test_lock_expire():
    lock1 = DistLock("test_lock_expire", [{"host": "localhost"}], ttl=500)
    lock1.acquire()
    time.sleep(1)

    # Now lock1 has expired, we can accquire a lock
    lock2 = DistLock("test_lock_expire", [{"host": "localhost"}], ttl=1000)
    locked = lock2.acquire()
    assert locked is True

    lock1.release()
    lock3 = DistLock("test_lock_expire", [{"host": "localhost"}], ttl=1000)
    locked = lock3.acquire()
    assert locked is False


class TestLock(unittest.TestCase):
    def setUp(self):
        super(TestLock, self).setUp()
        self.distlock = mock.patch.object(DistLock, '__init__', return_value=None).start()
        self.distlock_acquire = mock.patch.object(DistLock, 'acquire').start()
        self.distlock_release = mock.patch.object(DistLock, 'release').start()
        self.distlock_acquire.return_value = True

    def tearDown(self):
        mock.patch.stopall()

    def test_passthrough(self):
        test_lock = ReentrantDistLock('')
        test_lock.acquire()
        test_lock.release()

        self.distlock.assert_called_once_with('')
        self.distlock_acquire.assert_called_once_with()
        self.distlock_release.assert_called_once_with()

    def test_reentrant(self):
        test_lock = ReentrantDistLock('')
        test_lock.acquire()
        test_lock.acquire()
        test_lock.release()
        test_lock.release()

        self.distlock.assert_called_once_with('')
        self.distlock_acquire.assert_called_once_with()
        self.distlock_release.assert_called_once_with()

    def test_reentrant_n(self):
        test_lock = ReentrantDistLock('')
        for _ in range(10):
            test_lock.acquire()
        for _ in range(10):
            test_lock.release()

        self.distlock.assert_called_once_with('')
        self.distlock_acquire.assert_called_once_with()
        self.distlock_release.assert_called_once_with()

    def test_no_release(self):
        test_lock = ReentrantDistLock('')
        test_lock.acquire()
        test_lock.acquire()
        test_lock.release()

        self.distlock.assert_called_once_with('')
        self.distlock_acquire.assert_called_once_with()
        self.distlock_release.assert_not_called()


def test_lock_with_multi_backend():
    """
    Test a DistLock can be acquired when at least N/2+1 redis instances are alive.
    Set redis instance with port 6380 down or debug sleep during test.
    """
    lock = DistLock("test_simple_lock", connection_details=[
        {"host": "localhost", "port": 6379, "db": 0, "socket_timeout": 0.2},
        {"host": "localhost", "port": 6379, "db": 1, "socket_timeout": 0.2},
        {"host": "localhost", "port": 6380, "db": 0, "socket_timeout": 0.2}], ttl=1000)
    locked = lock.acquire()
    lock.release()
    assert locked is True
