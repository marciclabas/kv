from typing_extensions import Protocol, Literal, Mapping

class Response(Protocol):
  status_code: int
  content: bytes

class Request(Protocol):
  async def __call__(
    self, method: Literal['GET', 'POST', 'DELETE'], url: str, /, *,
    data: bytes | str | None = None, params: Mapping[str, str] = {}
  ) -> Response: ...

async def request(
  method: Literal['GET', 'POST', 'DELETE'], url: str, /, *,
  data: bytes | str | None = None, params: Mapping[str, str] = {}, **kwargs
) -> Response:
  import httpx
  async with httpx.AsyncClient() as client:
    return await client.request(method, url, data=data, params=params, **kwargs) # type: ignore
  
def bound_request(*, headers: Mapping[str, str], **kwargs) -> Request:
  async def _bound(
    method: Literal['GET', 'POST', 'DELETE'], url: str, /, *,
    data: bytes | str | None = None, params: Mapping[str, str] = {}
  ):
    return await request(method, url, data=data, params=params, headers=headers, **kwargs)
  
  return _bound