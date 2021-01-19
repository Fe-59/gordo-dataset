import logging
import threading

from azure.core.exceptions import ResourceNotFoundError
from azure.core.pipeline.transport import RequestsTransport
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    FileSystemClient,
    PathProperties,
)
from azure.identity import ClientSecretCredential, InteractiveBrowserCredential
from typing import Optional, Any, IO, Iterable, cast, Tuple
from io import BytesIO, TextIOWrapper

from gordo_dataset.exceptions import ConfigException

from .base import FileSystem, FileInfo, FileType
from .azure import ADLSecret

logger = logging.getLogger(__name__)


class ThreadLocalRequestTransport(RequestsTransport):
    def __init__(self, **kwargs):
        self.local = threading.local()
        super().__init__(**kwargs)

    @property
    def session(self):
        return self.local.__dict__.get("session", None)

    @session.setter
    def session(self, session):
        self.local.session = session


class ADLGen2FileSystem(FileSystem):
    @classmethod
    def create_from_env(
        cls,
        account_name: str,
        file_system_name: str,
        interactive: bool = False,
        adl_secret: Optional[ADLSecret] = None,
        **kwargs,
    ) -> "ADLGen2FileSystem":
        """
        Creates ADL Gen2 file system client.

        Parameters
        ----------
        account_name: str
            Azure account name
        file_system_name: str
            Container name
        interactive: bool
            If true then use interactive authentication
        adl_secret: ADLSecret
            Azure authentication information

        Returns
        -------
        ADLGen2FileSystem
        """
        if interactive:
            logger.info("Attempting to use interactive azure authentication")
            credential = InteractiveBrowserCredential()
        else:
            if type(adl_secret) is not ADLSecret:
                raise ConfigException(
                    "Unsupported type for adl_secret '%s'" % type(adl_secret)
                )
            adl_secret = cast(ADLSecret, adl_secret)
            logger.info("Attempting to use datalake service authentication")
            credential = ClientSecretCredential(
                tenant_id=adl_secret.tenant_id,
                client_id=adl_secret.client_id,
                client_secret=adl_secret.client_secret,
            )
        return cls.create_from_credential(
            account_name, file_system_name, credential, **kwargs
        )

    @classmethod
    def create_from_credential(
        cls,
        account_name: str,
        file_system_name: str,
        credential: Any,
        use_thread_local_transport: bool = True,
        **kwargs,
    ) -> "ADLGen2FileSystem":
        """
        Creates ADL Gen2 file system client.

        Parameters
        ----------
        account_name: str
            Azure account name
        file_system_name: str
            Container name
        credential: object
            azure.identity credential
        use_thread_local_transport: bool
            Use ``ThreadLocalRequestTransport`` as HTTP transport

        Returns
        -------
        ADLGen2FileSystem
        """
        client_kwargs = {}
        if use_thread_local_transport:
            client_kwargs["transport"] = ThreadLocalRequestTransport()
        service_client = DataLakeServiceClient(
            account_url="https://%s.dfs.core.windows.net" % account_name,
            credential=credential,
            **client_kwargs,
        )
        file_system_client = service_client.get_file_system_client(
            file_system=file_system_name
        )
        return cls(file_system_client, account_name, file_system_name, **kwargs)

    def __init__(
        self,
        file_system_client: FileSystemClient,
        account_name: str,
        file_system_name: str,
        max_concurrency: int = 1,
    ):
        self.file_system_client = file_system_client
        self.account_name = account_name
        self.file_system_name = file_system_name
        self.max_concurrency = max_concurrency

    @property
    def name(self):
        return self.file_system_name + "@" + self.account_name

    def open(self, path: str, mode: str = "r") -> IO:
        for m in mode:
            if m not in "rb":
                raise ValueError("Unsupported file open mode '%s'" % m)
        wrap_as_text = "b" not in mode
        file_client = self.file_system_client.get_file_client(path)
        downloader = file_client.download_file(max_concurrency=self.max_concurrency)
        stream = BytesIO(downloader.readall())
        fd = cast(IO, TextIOWrapper(stream) if wrap_as_text else stream)
        return fd

    def exists(self, path: str) -> bool:
        try:
            self.info(path)
            return True
        except FileNotFoundError:
            return False

    def isfile(self, path: str) -> bool:
        try:
            info = self.info(path)
            return info.file_type == FileType.FILE
        except FileNotFoundError:
            return False

    def isdir(self, path: str) -> bool:
        try:
            info = self.info(path)
            return info.file_type == FileType.DIRECTORY
        except FileNotFoundError:
            return False

    def info(self, path: str) -> FileInfo:
        file_client = self.file_system_client.get_file_client(path)
        try:
            properties = file_client.get_file_properties()
        except ResourceNotFoundError:
            raise FileNotFoundError(path)
        except Exception as e:
            logger.debug("Exception %s(%s)", e.__class__.__name__, path)
            raise
        content_settings = properties["content_settings"]
        if content_settings.get("content_type", None):
            file_type = FileType.FILE
        else:
            file_type = FileType.DIRECTORY
        return FileInfo(
            file_type,
            properties.get("size", 0),
            modify_time=properties["last_modified"],
            create_time=properties["creation_time"],
        )

    @staticmethod
    def path_properties_to_info(path_properties: PathProperties) -> FileInfo:
        if path_properties.is_directory:
            file_type = FileType.DIRECTORY
        else:
            file_type = FileType.FILE
        size = path_properties.content_length if path_properties.content_length else 0
        # TODO taking into account attribute path_properties.last_modified. Example: 'Mon, 07 Sep 2020 12:01:14 GMT'
        return FileInfo(file_type, size)

    @staticmethod
    def _handle_attribute_error_bug(e: AttributeError):
        if e.__cause__ is not None and hasattr(e.__cause__, "response"):
            response = getattr(e.__cause__, "response")
            logger.error(
                "Handling azure.storage.filedatalake AttributeError bug. Response: %s",
                response,
            )
        raise e

    def ls(
        self, path: str, with_info: bool = True
    ) -> Iterable[Tuple[str, Optional[FileInfo]]]:
        dir_iterator = self.file_system_client.get_paths(path, recursive=False)

        try:
            for properties in dir_iterator:
                file_info = (
                    self.path_properties_to_info(properties) if with_info else None
                )
                yield properties.name, file_info
        except AttributeError as e:
            self._handle_attribute_error_bug(e)

    def walk(
        self, base_path: str, with_info: bool = True
    ) -> Iterable[Tuple[str, Optional[FileInfo]]]:
        dir_iterator = self.file_system_client.get_paths(base_path)

        try:
            for properties in dir_iterator:
                file_info = (
                    self.path_properties_to_info(properties) if with_info else None
                )
                yield properties.name, file_info
        except AttributeError as e:
            self._handle_attribute_error_bug(e)
