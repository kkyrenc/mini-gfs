from dataclasses import dataclass
from enum import Enum, unique


@unique
class ChunkServerStatus(Enum):
    HEALTHY = 0
    SUSPECT = 1
    FAILED = 2

@dataclass
class ChunkServerInfo:
    address: str
    status: ChunkServerStatus
    remains: int