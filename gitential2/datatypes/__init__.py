from abc import ABC
from datetime import datetime
from enum import Enum
from typing import Dict, Optional
from pathlib import Path

from pydantic.dataclasses import dataclass
from pydantic import BaseModel, Field
from .common import CoreModel
from .userinfos import UserInfoCreate, UserInfoUpdate, UserInfoPublic, UserInfoInDB
from .users import UserCreate, UserUpdate, UserPublic, UserInDB, UserHeader
from .workspaces import WorkspaceCreate, WorkspaceUpdate, WorkspacePublic, WorkspaceInDB
from .workspacemember import (
    WorkspaceMemberCreate,
    WorkspaceMemberUpdate,
    WorkspaceMemberPublic,
    WorkspaceMemberInDB,
    # WorkspaceWithPermission,
    WorkspaceRole,
)
from .credentials import CredentialCreate, CredentialUpdate, CredentialPublic, CredentialInDB, CredentialType
from .projects import (
    ProjectCreate,
    ProjectUpdate,
    ProjectInDB,
    ProjectPublic,
    ProjectCreateWithRepositories,
    ProjectUpdateWithRepositories,
    ProjectPublicWithRepositories,
)
from .repositories import (
    RepositoryCreate,
    RepositoryUpdate,
    RepositoryPublic,
    RepositoryInDB,
    GitRepository,
    GitRepositoryState,
    GitRepositoryStateChange,
    GitProtocol,
)

from .project_repositories import ProjectRepositoryCreate, ProjectRepositoryUpdate, ProjectRepositoryInDB
