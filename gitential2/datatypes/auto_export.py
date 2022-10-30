from typing import Optional, List
from .common import CoreModel, IDModelMixin, DateTimeModelMixin
from pydantic import validator


class AutoExportBase(CoreModel):
    """
    Base data class for auto export model
    """
    workspace_id: int
    cron_schedule_time: int
    tempo_access_token: Optional[str] = None
    emails: List[str] = []

class AutoExportCreate(AutoExportBase):
    pass

class AutoExportUpdate(AutoExportBase):
    pass

class AutoExportInDB(IDModelMixin,DateTimeModelMixin,AutoExportBase):
    pass