from .common import CoreModel, IDModelMixin


class ProjectRepositoryBase(CoreModel):
    project_id: int
    repo_id: int


class ProjectRepositoryCreate(ProjectRepositoryBase):
    pass


class ProjectRepositoryUpdate(ProjectRepositoryBase):
    pass


class ProjectRepositoryInDB(IDModelMixin, ProjectRepositoryBase):
    pass
