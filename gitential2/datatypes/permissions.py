from enum import Enum


class Entity(str, Enum):
    user = "user"
    workspace = "workspace"
    project = "project"
    team = "team"
    membership = "membership"
    credential = "credential"
    repository = "repository"


class Action(str, Enum):
    create = "create"
    update = "update"
    delete = "delete"
    read = "read"