import json
from pathlib import Path
from typing import Any, Dict, Protocol


class ArtifactStore(Protocol):
    def write_json(self, relative_path: str, data: Dict[str, Any]) -> str:
        """Write JSON data and return the absolute/usable path to the stored artifact."""
        ...


class LocalArtifactStore:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def write_json(self, relative_path: str, data: Dict[str, Any]) -> str:
        path = self.base_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return str(path)
