from typing import Any
import logging
import json
import signal
import asyncio
from contextlib import suppress
import aiofiles

from .abc import ServiceBase, CommunicationServiceBase, ServiceManagerBase, _HypervisorExceptionAction
from .utils import ValidationError, NoServiceError

class SysD(ServiceManagerBase):
    """Asynchronous SysD"""
    def __init__(self, conf_path: str, /, services: dict[str, ServiceBase]):
        """services is dictionary with {NAME:SERVICE}"""
        self._services: dict[str, ServiceBase] = {}
        self._signal_service_names: list[str] = []
        for name, service in services.items():
            self.add_service(name, service)

        self._conf_path = conf_path
        self._conf: dict[str, dict[str, Any]] = {}
        self._logger = logging.getLogger(__name__)
        self._run = False # shut down by default
        self._cancelled = False
        self._started = False
        self._work = asyncio.Event()

    async def signal(self, service_name: str, method_name: str, method_body: dict, /) -> dict | None:
        self._logger.debug(f"received signal to service: {service_name!r} and method {method_name!r} with body {method_body!r}")
        if service_name in self._signal_service_names:
            return await self._services[service_name].call(method_name, method_body)
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
        """Runs all services and waits them to be completed"""
        self._logger.info("load configuration from file")
        await self.read_config_file()

        self._logger.info("apply configuration to all services")
        for service_name, service in self._services.items():
            if service_name in self._conf.keys():
                service.post_init(self._conf[service_name])
            else:
                # call validate method to ensure that service has NO validation exceptions
                service.post_init({})

        # just to ensure that config has no another fields
        # (that doesn't associated with any service)
        for service_name in self._conf.keys():
            if service_name not in self._services.keys():
                raise ValidationError(f"no such service: {service_name!r}")

        if not self._cancelled:
            self._run = True
        else:
            self._logger.info("canceled")
            return
        
        self._logger.info("start services")

        tasks: list[asyncio.Task] = []
        for service in self._services.values():
            tasks.append(
                asyncio.create_task(self._hypervisor(
                    service.service,
                    getattr(service, "on_exception_action", "exit")
                ))
            )
        self._started = True
        self._logger.info("services started")
        await self._work.wait()
        self._logger.info("waiting for services to stop")
        await asyncio.wait(tasks, timeout=30)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def run(self):
        """Runs SysD"""
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
        self._services[name] = service
        if isinstance(service, CommunicationServiceBase):
            self._signal_service_names.append(name)

    async def read_config_file(self):
        """Reads config from file to memory"""
        async with aiofiles.open(self._conf_path, "r", encoding="utf-8") as file:
            self._conf = json.loads(await file.read())

    async def write_config_file(self):
        """Saves in-memory SysD config to file"""
        async with aiofiles.open(self._conf_path, "w", encoding="utf-8") as file:
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

        for_validatation: dict[str, dict[str, Any]] = {} # services configs for validation

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

            for_validatation[service_name] = service_new_config

        # validate configs
        for service_name, config in for_validatation.items():
            self._services[service_name].validate_config(config)

        # install configs
        for service_name, config in for_validatation.items():
            self._services[service_name].set_config(config)

        self._conf = for_validatation
        await self.write_config_file()

    async def set_config(self, new_config: dict[str, dict[str, Any]], /):
        # validate configuration
        for service_name, service in self._services.items():
            if service_name in new_config.keys():
                service.validate_config(new_config[service_name])
            else:
                # call validate method to ensure that service has NO validation exceptions
                service.validate_config({})

        # just to ensure that config has no another fields
        # (that don't associated with any service)
        for service_name in new_config.keys():
            if service_name not in self._services.keys():
                raise ValidationError(f"no such service: {service_name!r}")

        # install configuration
        for service_name, service_config in new_config.items():
            self._services[service_name].set_config(service_config)

        self._conf = new_config
        await self.write_config_file()
