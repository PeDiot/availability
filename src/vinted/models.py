from typing import Dict, Optional
from dataclasses import dataclass


@dataclass
class VintedResponse:
    status_code: int
    data: Optional[Dict] = None
