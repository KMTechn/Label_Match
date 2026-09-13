"""Label_Match compatibility facade for the pinned shared raster core.

Product label writes remain inside the admitted PHS label exchange workflow.
Image-producing operations retain this facade class through the core factories.
"""
from kmtech_shared import raster as _core
from kmtech_shared.raster import (
    Anchor, BITMAPINFO, BITMAPINFOHEADER, Color, FontSpec, GdiRenderError,
    MAX_PNG_INPUT_BYTES, MAX_RASTER_PIXELS, RGBQUAD, RasterError, Resample,
    SIZE, bitmap_info,
)


class RasterImage(_core.RasterImage):
    """Label_Match image identity with the shared decode, resize and PNG implementation."""


class RasterCanvas(_core.RasterCanvas):
    """Return Label_Match images from the shared GDI canvas."""

    image_class = RasterImage
