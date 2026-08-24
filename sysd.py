__all__ = [
    "SysD", "ServiceManagerBase", "ServiceBase",
    "CommunicationServiceBase", "SysDException",
    "ValidationError", "NoServiceError", "validate",
    "important_missing", "NoConfigPathError", "ServiceNameAlreadyDefinedError"
]

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

class SysDException(Exception):
    """Base exception for all SysD exceptions"""

class ValidationError(SysDException):
    """Raised if some expected field in config invalid or missing"""
    def __init__(self, what: str):
        self._what = what
    
    def what(self):
        return self._what

class NoServiceError(SysDException):
    """Raised if service send signal() to unknown service"""

class NoConfigPathError(SysDException):
    """Raised if tryed to save config to file with no file specified"""

class ServiceNameAlreadyDefinedError(SysDException):
    """Raised if tryed to add service with name that already defined"""

def validate(value, _type):
    """
    Validates `value` with expected `_type`
    Does nothing on success
    Raises ValidationError() on failure
    """
    if not isinstance(value, _type):
        raise ValidationError(f"{value!r} is not {_type.__name__!r} type")

def important_missing(data: dict[str, Any], *args) -> bool:
    """
    Checks if the required key is missing from the dictionary
    Returns True if the key is absent
    Returns False if all keys are present
    """
    for i in args:
        if data.get(i, None) is None:
            return True

    return False

import logging
import json
import signal
import asyncio
from contextlib import suppress
import aiofiles

class SysD(ServiceManagerBase):
    """Asynchronous SysD"""
    __slots__ = [
        "_services", "_conf_path",
        "_file", "_conf", "_logger", "_run", "_cancelled",
        "_started", "_work", "_tasks"
    ]
    def __init__(self, conf: str | dict[str, Any], /, services: dict[str, ServiceBase]):
        """
        `conf` is path to the config or dictionary
        `services` is dictionary with {NAME:SERVICE}
        """
        self._services: dict[str, ServiceBase] = {}
        for name, service in services.items():
            self.add_service(name, service)

        if isinstance(conf, str):
            self._conf_path = conf
            self._file = True
            self._conf: dict[str, dict[str, Any]] = {}
        else:
            self._conf = conf
            self._file = False
        self._logger = logging.getLogger(__name__)
        self._run = False # shut down by default
        self._cancelled = False
        self._started = False
        self._work = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    async def signal(self, service_name: str, method_name: str, method_body: dict, /) -> dict | None:
        self._logger.debug(f"received signal to service: {service_name!r} and method {method_name!r} with body {method_body!r}")
        service = self._services.get(service_name)
        if isinstance(service, CommunicationServiceBase):
            return await service.call(method_name, method_body)
        self._logger.error(f"no service found with name {service_name!r}")
        raise NoServiceError(f"no such service {service_name!r}")
    
    async def _hypervisor(self, func, on_exc: _HypervisorExceptionAction = "exit"):
        while self._run:
            try:
                await func()
                # as service exited normally
                # we'll mark it as normal behaviour
                # and exit
                return
            except asyncio.CancelledError:
                return
            except BaseException as e:
                self._logger.exception("hypervisor got fatal exception")
                if on_exc == "exit":
                    self._logger.fatal(f"as hypervisor on exception behaviour was set to {on_exc!r}, application is shutting down now")
                    self._shutdown("HYPERVISOR EXCEPTION")

    async def _run_services(self):
        """Runs all services"""
        if self._file:
            self._logger.info("load configuration from file")
            await self.read_config_file()
        
        # just to ensure that config has no another fields
        # (that doesn't associated with any service)
        for service_name in self._conf.keys():
            if service_name not in self._services.keys():
                raise ValidationError(f"no such service: {service_name!r}")

        self._logger.info("apply configuration to all services")
        for service_name, service in self._services.items():
            if service_name in self._conf.keys():
                service.post_init(self._conf[service_name])
            else:
                # call validate method to ensure that service has NO validation exceptions
                service.post_init({})

        if not self._cancelled:
            self._run = True
        else:
            self._logger.info("canceled")
            return
        
        self._logger.info("start services")

        for service in self._services.values():
            self._tasks.append(
                asyncio.create_task(self._hypervisor(
                    service.service,
                    getattr(service, "on_exception_action", "exit")
                ))
            )
        self._started = True
        self._logger.info("services started")
    
    async def wait(self):
        await self._work.wait()
        self._logger.info("waiting for services to stop")
        await asyncio.wait(self._tasks, timeout=30)
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def run(self, daemon: bool = False):
        """
        Runs SysD
        Start services and returns immediately when `daemon` is set to `True`
        """
        loop = asyncio.get_running_loop()
        with suppress(NotImplementedError):
            # Signals handling is not supported on Windows
            loop.add_signal_handler(
                signal.SIGTERM,
                self._shutdown,
                signal.SIGTERM,
            )
            loop.add_signal_handler(
                signal.SIGINT,
                self._shutdown,
                signal.SIGINT,
            )
        await self._run_services()
        if not daemon:
            await self.wait()

    def _shutdown(self, sig, /):
        """Shuttes down application"""
        self._logger.info(f"received {sig}, shut down services")
        try:
            if not self._started or not self._run:
                return # to avoid service.shutdown for unstarted services
        finally:
            self._run = False
            self._cancelled = True
            self._work.set()
        for service in self._services.values():
            try:
                service.shutdown()
            except BaseException as e:
                self._logger.exception("oops. shutdown raised an exception, which is abnormal for this method")

    def add_service(self, name: str, service: ServiceBase, /):
        """Add service to services order"""
        setattr(service, "_sysd", self)
        if name in self._services.keys():
            raise ServiceNameAlreadyDefinedError("service name already defined")
        self._services[name] = service
    
    def _choose_path(self, path: str | None = None):
        if not path:
            if not self._file:
                raise NoConfigPathError()
            return self._conf_path
        return path

    async def read_config_file(self, path: str | None = None):
        """Reads config from file to memory"""
        path = self._choose_path(path)
        async with aiofiles.open(path, "r", encoding="utf-8") as file:
            self._conf = json.loads(await file.read())

    async def write_config_file(self, path: str | None = None):
        """Saves in-memory SysD config to file"""
        path = self._choose_path(path)
        async with aiofiles.open(path, "w", encoding="utf-8") as file:
            await file.write(json.dumps(self._conf))

    async def get_config(self) -> dict[str, dict[str, Any]]:
        return self._conf

    async def merge_config(self, new_config: dict[str, Any], /):
        global_config: dict[str, Any] = {}
        service_configs: dict[str, dict[str, Any]] = {}

        for key, value in new_config.items():
            if key in self._services.keys() and isinstance(value, dict):
                service_configs[key] = value
            else:
                global_config[key] = value

        for_validation: dict[str, dict[str, Any]] = {} # services configs for validation

        for service_name, service in self._services.items():
            service_current_config = service.get_config()
            service_new_config = {}

            for k, v in global_config.items():
                if k in service_current_config.keys():
                    service_new_config[k] = v

            for k, v in service_current_config.items():
                if k not in service_new_config.keys():
                    service_new_config[k] = v

            if service_name in service_configs.keys():
                for k, v in service_configs[service_name].items():
                    service_new_config[k] = v

            for_validation[service_name] = service_new_config

        # validate configs
        for service_name, config in for_validation.items():
            self._services[service_name].validate_config(config)

        # install configs
        for service_name, config in for_validation.items():
            self._services[service_name].set_config(config)

        self._conf = for_validation
        if self._file:
            await self.write_config_file()

    async def set_config(self, new_config: dict[str, dict[str, Any]], /):
        # just to ensure that config has no new unused fields
        # (that don't associated with any service)
        for service_name in new_config.keys():
            if service_name not in self._services.keys():
                raise ValidationError(f"no such service: {service_name!r}")

        # validate configuration
        for service_name, service in self._services.items():
            if service_name in new_config.keys():
                service.validate_config(new_config[service_name])
            else:
                # call validate method to ensure that service has NO validation exceptions
                service.validate_config({})

        # install configuration
        for service_name, service_config in new_config.items():
            self._services[service_name].set_config(service_config)

        self._conf = new_config
        if self._file:
            await self.write_config_file()
