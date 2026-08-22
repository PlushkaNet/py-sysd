from abc import ABC, abstractmethod
from typing import Any, Literal

_HypervisorExceptionAction = Literal["restart", "exit"]

class ServiceManagerBase(ABC):
    """Base abstract interface for system service managers"""
    @abstractmethod
    async def signal(self, service_name: str, method_name: str, method_body: dict, /) -> dict | None: ...

    @abstractmethod
    async def get_config(self) -> dict[str, dict[str, Any]]: ...

    @abstractmethod
    async def set_config(self, data: dict[str, dict[str, Any]], /): ...


class ServiceBase(ABC):
    """Base abstract interface for all service classes"""

    _sysd: ServiceManagerBase
    on_exception_action: _HypervisorExceptionAction

    @abstractmethod
    def post_init(self, config: dict[str, Any], /): ...

    @abstractmethod
    def set_config(self, new_config: dict[str, Any], /): ...

    @abstractmethod
    def validate_config(self, config: dict[str, Any], /): ...

    @abstractmethod
    def get_config(self) -> dict[str, Any]: ...

    @abstractmethod
    async def service(self): ...

    @abstractmethod
    def shutdown(self): ...


class CommunicationServiceBase(ServiceBase):
    @abstractmethod
    async def call(self, method_name: str, method_body: dict, /): ...
