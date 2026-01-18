import threading
from typing import Dict, List, Callable, Optional, Any

from opentelemetry.metrics import Observation


class ObservablesRegistry:
    """
    Global registry for storing callbacks for observable metrics.
    """
    def __init__(self, registry: Dict[str, List[Callable[[], List[Observation]]]] = None):
        self._registry = registry or {}
        self._lock = threading.Lock()

    def register(self, name: str, callback: Callable[[], List[Observation]]) -> None:
        """
        Register a callback for an observable metric.
        """
        with self._lock:
            self._registry.setdefault(name, []).append(callback)

    def get(self, name: str) -> List[Callable[[], List[Observation]]]:
        """
        Get all callbacks for an observable metric.
        """
        with self._lock:
            return self._registry.get(name, [])

    def clear(self) -> None:
        """
        Clear the registry.
        """
        with self._lock:
            self._registry.clear()

    def __len__(self) -> int:
        """
        Get the number of registered callbacks.
        """
        return len(self._registry)

# Global singleton instance
_observables_registry_instance: Optional[ObservablesRegistry] = None

def get_observables_registry_instance() -> ObservablesRegistry:
    """
    Get the global observables registry singleton instance.

    This is the Pythonic way to get the singleton instance.

    Returns:
        The global ObservablesRegistry singleton

    Example:
        >>>
        >>> registry = get_observables_registry_instance()
        >>> registry.register('my_metric', my_callback)
    """
    global _observables_registry_instance

    if _observables_registry_instance is None:
        _observables_registry_instance = ObservablesRegistry()

    return _observables_registry_instance