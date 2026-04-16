"""Pydantic request/response models for the AV1 Pipeline Dashboard API."""

from pydantic import BaseModel


class PauseRequest(BaseModel):
    """Request to pause pipeline by type."""

    type: str  # "all" | "fetch" | "encode"


class PathListRequest(BaseModel):
    """Request containing a list of file paths."""

    paths: list[str]


class PriorityRequest(BaseModel):
    """Request to set priority queue for pipeline processing."""

    force: list[str] = []
    paths: list[str] = []
    patterns: list[str] = []


class GentleRequest(BaseModel):
    """Request to configure gentle encoding offsets."""

    paths: dict = {}
    patterns: dict = {}
    default_offset: int = 0


class ReencodeRequest(BaseModel):
    """Request to configure re-encoding targets."""

    files: dict = {}
    patterns: dict = {}


class KeywordListRequest(BaseModel):
    """Request containing a list of keywords."""

    keywords: list[str]


class ForceRequest(BaseModel):
    """Request to add/remove a file from force-priority tier."""

    path: str
    action: str = "add"  # "add" | "remove"


class DeleteFileRequest(BaseModel):
    """Request to delete a single file."""

    path: str


class VmafRequest(BaseModel):
    """Request to run a VMAF quality check."""

    path: str
    duration: int = 30
