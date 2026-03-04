import time
from typing import Any, Dict, List, Optional, Union

from .db_utils import get_exposed_run_id, get_exposed_task_id


class FlowRow(object):
    flow_id: str
    user_name: str
    ts_epoch: int
    tags: Optional[List[str]]
    system_tags: Optional[List[str]]

    def __init__(
        self,
        flow_id: str,
        user_name: str,
        ts_epoch: Optional[int] = None,
        tags: Optional[List[str]] = None,
        system_tags: Optional[List[str]] = None,
    ) -> None:
        self.flow_id = flow_id
        self.user_name = user_name
        if ts_epoch is None:
            ts_epoch = int(round(time.time() * 1000))
        self.ts_epoch = ts_epoch
        self.tags = tags
        self.system_tags = system_tags

    def serialize(self, expanded: bool = False) -> Dict[str, Any]:
        return {
            "flow_id": self.flow_id,
            "user_name": self.user_name,
            "ts_epoch": self.ts_epoch,
            "tags": self.tags,
            "system_tags": self.system_tags,
        }


class RunRow(object):
    flow_id: str
    run_number: Optional[int]
    run_id: Optional[str]
    user_name: str
    ts_epoch: int
    tags: Optional[List[str]]
    system_tags: Optional[List[str]]
    last_heartbeat_ts: Optional[int]

    def __init__(
        self,
        flow_id: str,
        user_name: str,
        run_number: Optional[int] = None,
        run_id: Optional[str] = None,
        ts_epoch: Optional[int] = None,
        tags: Optional[List[str]] = None,
        system_tags: Optional[List[str]] = None,
        last_heartbeat_ts: Optional[int] = None,
    ) -> None:
        self.flow_id = flow_id
        self.user_name = user_name
        self.run_number = run_number
        self.run_id = run_id
        self.tags = tags
        self.system_tags = system_tags
        if ts_epoch is None:
            ts_epoch = int(round(time.time() * 1000))

        self.ts_epoch = ts_epoch
        self.last_heartbeat_ts = last_heartbeat_ts

    def serialize(self, expanded: bool = False) -> Dict[str, Any]:
        if expanded:
            return {
                "flow_id": self.flow_id,
                "run_number": self.run_number,
                "run_id": self.run_id,
                "user_name": self.user_name,
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
                "last_heartbeat_ts": self.last_heartbeat_ts
            }
        else:
            return {
                "flow_id": self.flow_id,
                "run_number": get_exposed_run_id(self.run_number, self.run_id),
                "user_name": self.user_name,
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
                "last_heartbeat_ts": self.last_heartbeat_ts
            }


class StepRow(object):
    flow_id: str
    run_number: int
    run_id: Optional[str]
    step_name: str
    user_name: str
    ts_epoch: int
    tags: Optional[List[str]]
    system_tags: Optional[List[str]]

    def __init__(
        self,
        flow_id: str,
        run_number: int,
        run_id: Optional[str],
        user_name: str,
        step_name: str,
        ts_epoch: Optional[int] = None,
        tags: Optional[List[str]] = None,
        system_tags: Optional[List[str]] = None,
    ) -> None:
        self.flow_id = flow_id
        self.run_number = run_number

        if run_id is None:
            run_id = str(run_number)
        self.run_id = run_id

        self.step_name = step_name
        self.user_name = user_name
        if ts_epoch is None:
            ts_epoch = int(round(time.time() * 1000))

        self.ts_epoch = ts_epoch
        self.tags = tags
        self.system_tags = system_tags

    def serialize(self, expanded: bool = False) -> Dict[str, Any]:
        if expanded:
            return {
                "flow_id": self.flow_id,
                "run_number": self.run_number,
                "run_id": self.run_id,
                "step_name": self.step_name,
                "user_name": self.user_name,
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
            }
        else:
            return {
                "flow_id": self.flow_id,
                "run_number": get_exposed_run_id(self.run_number, self.run_id),
                "step_name": self.step_name,
                "user_name": self.user_name,
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
            }


class TaskRow(object):
    flow_id: str
    run_number: int
    run_id: Optional[str]
    step_name: str
    task_id: Optional[int]
    task_name: Optional[str]
    user_name: str
    ts_epoch: int
    tags: Optional[List[str]]
    system_tags: Optional[List[str]]
    last_heartbeat_ts: Optional[int]

    def __init__(
        self,
        flow_id: str,
        run_number: int,
        run_id: Optional[str],
        user_name: str,
        step_name: str,
        task_id: Optional[int] = None,
        task_name: Optional[str] = None,
        ts_epoch: Optional[int] = None,
        tags: Optional[List[str]] = None,
        system_tags: Optional[List[str]] = None,
        last_heartbeat_ts: Optional[int] = None,
    ) -> None:
        self.flow_id = flow_id
        self.run_number = run_number
        self.run_id = run_id
        self.step_name = step_name
        self.task_id = task_id
        self.task_name = task_name
        self.user_name = user_name
        if ts_epoch is None:
            ts_epoch = int(round(time.time() * 1000))

        self.ts_epoch = ts_epoch
        self.tags = tags
        self.system_tags = system_tags
        self.last_heartbeat_ts = last_heartbeat_ts

    def serialize(self, expanded: bool = False) -> Dict[str, Any]:
        if expanded:
            return {
                "flow_id": self.flow_id,
                "run_number": self.run_number,
                "run_id": self.run_id,
                "step_name": self.step_name,
                "task_id": self.task_id,
                "task_name": self.task_name,
                "user_name": self.user_name,
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
                "last_heartbeat_ts": self.last_heartbeat_ts
            }
        else:
            return {
                "flow_id": self.flow_id,
                "run_number": get_exposed_run_id(self.run_number, self.run_id),
                "step_name": self.step_name,
                "task_id": get_exposed_task_id(self.task_id, self.task_name),
                "user_name": self.user_name,
                "ts_epoch": self.ts_epoch,
                "tags": self.tags,
                "system_tags": self.system_tags,
                "last_heartbeat_ts": self.last_heartbeat_ts
            }


class MetadataRow(object):
    flow_id: str
    run_number: int
    run_id: Optional[str]
    step_name: str
    task_id: int
    task_name: Optional[str]
    id: int
    field_name: str
    value: str
    type: str
    user_name: str
    ts_epoch: int
    tags: Optional[List[str]]
    system_tags: Optional[List[str]]

    def __init__(
        self,
        flow_id: str,
        run_number: int,
        run_id: Optional[str],
        step_name: str,
        task_id: int,
        task_name: Optional[str],
        id: int,
        field_name: str,
        value: str,
        type: str,
        user_name: str,
        ts_epoch: Optional[int] = None,
        tags: Optional[List[str]] = None,
        system_tags: Optional[List[str]] = None,
    ) -> None:
        self.flow_id = flow_id
        self.run_number = run_number
        self.run_id = run_id
        self.step_name = step_name
        self.task_id = task_id
        self.task_name = task_name
        self.field_name = field_name
        self.value = value
        self.type = type
        self.user_name = user_name
        if ts_epoch is None:
            ts_epoch = int(round(time.time() * 1000))

        self.ts_epoch = ts_epoch
        self.id = id
        self.tags = tags
        self.system_tags = system_tags

    def serialize(self, expanded: bool = False) -> Dict[str, Any]:
        return {
            "id": self.id,
            "flow_id": self.flow_id,
            "run_number": get_exposed_run_id(self.run_number, self.run_id),
            "step_name": self.step_name,
            "task_id": get_exposed_task_id(self.task_id, self.task_name),
            "field_name": self.field_name,
            "value": self.value,
            "type": self.type,
            "user_name": self.user_name,
            "ts_epoch": self.ts_epoch,
            "tags": self.tags,
            "system_tags": self.system_tags,
        }


class ArtifactRow(object):
    flow_id: str
    run_number: int
    run_id: Optional[str]
    step_name: str
    task_id: int
    task_name: Optional[str]
    name: str
    location: str
    ds_type: str
    sha: Optional[str]
    type: Optional[str]
    content_type: Optional[str]
    user_name: str
    attempt_id: int
    ts_epoch: int
    tags: Optional[List[str]]
    system_tags: Optional[List[str]]

    def __init__(
        self,
        flow_id: str,
        run_number: int,
        run_id: Optional[str],
        step_name: str,
        task_id: int,
        task_name: Optional[str],
        name: str,
        location: str,
        ds_type: str,
        sha: Optional[str],
        type: Optional[str],
        content_type: Optional[str],
        user_name: str,
        attempt_id: int,
        ts_epoch: Optional[int] = None,
        tags: Optional[List[str]] = None,
        system_tags: Optional[List[str]] = None,
    ) -> None:
        self.flow_id = flow_id
        self.run_number = run_number
        self.run_id = run_id
        self.step_name = step_name
        self.task_id = task_id
        self.task_name = task_name
        self.name = name
        self.location = location
        self.ds_type = ds_type
        self.sha = sha
        self.type = type
        self.content_type = content_type
        self.user_name = user_name
        self.attempt_id = attempt_id
        if ts_epoch is None:
            ts_epoch = int(round(time.time() * 1000))

        self.ts_epoch = ts_epoch
        self.tags = tags
        self.system_tags = system_tags

    def serialize(self, expanded: bool = False) -> Dict[str, Any]:
        return {
            "flow_id": self.flow_id,
            "run_number": get_exposed_run_id(self.run_number, self.run_id),
            "step_name": self.step_name,
            "task_id": get_exposed_task_id(self.task_id, self.task_name),
            "name": self.name,
            "location": self.location,
            "ds_type": self.ds_type,
            "sha": self.sha,
            "type": self.type,
            "content_type": self.content_type,
            "user_name": self.user_name,
            "attempt_id": self.attempt_id,
            "ts_epoch": self.ts_epoch,
            "tags": self.tags,
            "system_tags": self.system_tags,
        }
