from typing import Union
from typing import Sequence

import numpy as np

from . import exceptions
from .lineage import get_future_root_ids


class RootLock:
    """Attempts to lock the requested root IDs using a unique operation ID.
    :raises exceptions.LockingError: throws when one or more root ID locks could not be
        acquired.
    """

    __slots__ = ["cg", "root_ids", "locked_root_ids", "lock_acquired", "operation_id"]
    # FIXME: `locked_root_ids` is only required and exposed because `cg.client.lock_roots`
    #        currently might lock different (more recent) root IDs than requested.

    def __init__(
        self,
        cg,
        root_ids: Union[np.uint64, Sequence[np.uint64]],
        *,
        operation_id: np.uint64 = None,
    ) -> None:
        self.cg = cg
        self.root_ids = np.atleast_1d(root_ids)
        self.locked_root_ids = None
        self.lock_acquired = False
        self.operation_id = operation_id

    def __enter__(self):
        if not self.operation_id:
            self.operation_id = self.cg.id_client.create_operation_id()
        future_root_ids_d = {}
        for id_ in self.root_ids:
            future_root_ids_d[id_] = get_future_root_ids(self.cg, id_)
        self.lock_acquired, self.locked_root_ids = self.cg.client.lock_roots(
            root_ids=self.root_ids,
            operation_id=self.operation_id,
            future_root_ids_d=future_root_ids_d,
            max_tries=7,
        )
        if not self.lock_acquired:
            raise exceptions.LockingError("Could not acquire root lock")
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if self.lock_acquired:
            for locked_root_id in self.locked_root_ids:
                self.cg.client.unlock_root(locked_root_id, self.operation_id)


class IndefiniteRootLock:
    """
    Attempts to lock the requested root IDs using a unique operation ID.
    Assumes the root IDs have already been locked temporally.
    Also renews temporal lock before creating locking indefinitely,
    fails to lock indefinitely if the temporal lock cannot be re-acquired.

    :raises exceptions.LockingError:
    when a root ID lock cannot be renewed
    or when it has already been locked indefinitely.
    """

    __slots__ = ["cg", "root_ids", "acquired", "operation_id"]

    def __init__(
        self,
        cg,
        operation_id: np.uint64,
        root_ids: Union[np.uint64, Sequence[np.uint64]],
    ) -> None:
        self.cg = cg
        self.operation_id = operation_id
        self.root_ids = np.atleast_1d(root_ids)
        self.acquired = False

    def __enter__(self):
        if not self.cg.client.renew_locks(self.root_ids, self.operation_id):
            raise exceptions.LockingError("Could not renew locks before writing.")

        future_root_ids_d = {}
        for id_ in self.root_ids:
            future_root_ids_d[id_] = get_future_root_ids(self.cg, id_)
        self.acquired, self.root_ids, failed = self.cg.client.lock_roots_indefinitely(
            root_ids=self.root_ids,
            operation_id=self.operation_id,
            future_root_ids_d=future_root_ids_d,
        )
        if not self.acquired:
            raise exceptions.LockingError(f"{failed} has been locked indefinitely.")
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if self.acquired:
            for locked_root_id in self.root_ids:
                self.cg.client.unlock_indefinitely_locked_root(
                    locked_root_id, self.operation_id
                )
