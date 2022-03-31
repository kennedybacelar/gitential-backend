from typing import Optional
from datetime import datetime
from .common import CoreModel, StringIdModelMixin, DateTimeModelMixin


class ResellerCodeBase(StringIdModelMixin, DateTimeModelMixin, CoreModel):
    reseller_id: str
    expire_at: Optional[datetime] = None
    user_id: Optional[int] = None

    @property
    def code(self):
        return self.id_


class ResellerCode(ResellerCodeBase):
    pass
