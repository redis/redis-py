from typing import List

from redis.event import EventListenerInterface, OnCommandsFailEvent
from redis.multidb.config import Databases
from redis.multidb.failure_detector import FailureDetector


class RegisterCommandFailure(EventListenerInterface):
    """
    Event listener that registers command failures and passing it to the failure detectors.
    """
    def __init__(self, failure_detectors: List[FailureDetector], databases: Databases):
        self._failure_detectors = failure_detectors
        self._databases = databases

    def listen(self, event: OnCommandsFailEvent) -> None:
        matching_database = None

        for database, _ in self._databases:
            if event.client == database.client:
                matching_database = database
                break

        if matching_database is None:
            return

        for failure_detector in self._failure_detectors:
            failure_detector.register_failure(matching_database, event.exception, event.command)
