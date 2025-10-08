from dataclasses import dataclass
from typing import Any, Dict, List, Union


@dataclass
class HybridResult:
    total_results: int
    results: List[Dict[str, Any]]
    warnings: List[Union[str, bytes]]
    execution_time: float


class HybridCursorResult:
    def __init__(self, search_cursor_id: int, vsim_cursor_id: int) -> None:
        self.search_cursor_id = search_cursor_id
        self.vsim_cursor_id = vsim_cursor_id
