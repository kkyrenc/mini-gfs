from dataclasses import dataclass
from enum import Enum, unique


@unique
class ChunkServerStatus(Enum):
    INITIAL = -1
    HEALTHY = 0
    SUSPECT = 1
    FAILED = 2

@dataclass
class ChunkServerInfo:
    address: str
    status: ChunkServerStatus
    remains: int
    last_update: float

    def __hash__(self) -> int:
        return hash(self.address)
    
    def __eq__(self, other: 'ChunkServerInfo') -> bool:
        if isinstance(other, ChunkServerInfo):
            return self.address == other.address
        return False