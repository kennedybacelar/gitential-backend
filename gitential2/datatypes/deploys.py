from datetime import datetime
from typing import Optional

from .common import CoreModel, ExtraFieldMixin, StringIdModelMixin


class Deploy(StringIdModelMixin, ExtraFieldMixin, CoreModel):
    pull_requests: Optional[dict]
    commits: Optional[dict]
    issues: Optional[dict]
    environment: str
    deployed_at: datetime
