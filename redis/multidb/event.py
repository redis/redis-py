from typing import List

from redis.event import EventListenerInterface, OnCommandFailEvent
from redis.multidb.failure_detector import FailureDetector


class RegisterCommandFailure(EventListenerInterface):
    """
    Event listener that registers command failures and passing it to the failure detectors.
    """
    def __init__(self, failure_detectors: List[FailureDetector]):
        self._failure_detectors = failure_detectors

    def listen(self, event: OnCommandFailEvent) -> None:
        for failure_detector in self._failure_detectors:
            failure_detector.register_failure(event.exception, event.command)
