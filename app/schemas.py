from pydantic import BaseModel


class LoginRequest(BaseModel):
    fio: str
    last4: str


class LoginResponse(BaseModel):
    status: str
    name: str


class CalendarResponse(BaseModel):
    point_id: str
    month: str
    status: str
    point_total: int
    overall_total: int
