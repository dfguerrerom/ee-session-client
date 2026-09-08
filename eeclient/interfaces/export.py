from typing import Any, Dict, Optional, Protocol, Union
from eeclient.export.image import (
    AffineTransform,
    ImageFileFormat,
    PixelGrid,
    PyramidingPolicy,
)
from eeclient.export.table import TableFileFormat


class ExportProtocol(Protocol):
    async def image_to_asset_async(
        self,
        image,
        asset_id: str,
        description: str = ...,
        max_pixels: Optional[int] = ...,
        grid: Optional[PixelGrid] = ...,
        request_id: Optional[str] = ...,
        workload_tag: Optional[str] = ...,
        priority: Optional[int] = ...,
        region: Union[Any, str] = ...,
        scale: Optional[float] = ...,
        crs: Optional[str] = ...,
        crs_transform: Optional[AffineTransform] = ...,
        pyramiding_policy: Optional[PyramidingPolicy] = ...,
        pyramiding_policy_overrides: Optional[Dict[str, PyramidingPolicy]] = ...,
    ) -> dict: ...

    async def image_to_drive_async(
        self,
        image,
        filename_prefix: str = ...,
        folder: Optional[str] = ...,
        file_format: ImageFileFormat = ...,
        description: str = ...,
        max_pixels: Optional[int] = ...,
        grid: Optional[PixelGrid] = ...,
        request_id: Optional[str] = ...,
        workload_tag: Optional[str] = ...,
        priority: Optional[int] = ...,
        region: Union[Any, str] = ...,
        scale: Optional[float] = ...,
        crs: Optional[str] = ...,
        crs_transform: Optional[AffineTransform] = ...,
    ) -> dict: ...

    async def table_to_drive_async(
        self,
        collection,
        filename_prefix: str,
        file_format: TableFileFormat,
        folder: Optional[str] = ...,
        description: str = ...,
        selectors: Optional[list] = ...,
        max_vertices: Optional[int] = ...,
        priority: Optional[int] = ...,
    ) -> dict: ...

    async def table_to_asset(
        self,
        collection,
        asset_id: str,
        description: str = ...,
        selectors: Optional[list] = ...,
        max_vertices: Optional[int] = ...,
        priority: Optional[int] = ...,
    ) -> dict: ...
