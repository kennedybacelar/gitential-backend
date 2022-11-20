from typing import Optional, List, Tuple
from gitential2.datatypes.export import ExportableModel
from .common import CoreModel, IDModelMixin, DateTimeModelMixin


class AutoExportBase(CoreModel):
    """
    Base data class for auto export model
    """

    workspace_id: int
    cron_schedule_time: int
    tempo_access_token: Optional[str] = None
    emails: List[str] = []
    is_exported: bool = False


class AutoExportCreate(AutoExportBase):
    pass


class AutoExportUpdate(AutoExportBase):
    pass


class AutoExportInDB(IDModelMixin, DateTimeModelMixin, AutoExportBase, ExportableModel):
    def export_fields(self) -> List[str]:
        return [
            "id",
            "workspace_id",
            "cron_schedule_time",
            "emails",
            "tempo_access_token",
            "created_at",
            "updated_at",
            "is_exported",
        ]

    def export_names(self) -> Tuple[str, str]:
        return "auto_export", "auto_exports"
