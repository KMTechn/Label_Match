"""Pillow-free raster, PNG, GDI drawing, QR, and Tk image primitives.

The module intentionally implements only the primitive categories found in the five-application
the seq258 AST inventory: RGB/RGBA PNGs, nearest/bilinear placement, text,
rectangles, lines, ellipses, QR matrices, and textual product-barcode rows.
It does not implement an unused one-dimensional barcode symbology.
"""

from __future__ import annotations

import base64
import ctypes
from ctypes import wintypes
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import struct
from typing import Iterable, Literal, Sequence
import uuid
import zlib


Color = tuple[int, int, int]
Anchor = Literal["lt", "mt", "mm", "lm"]
Resample = Literal["nearest", "bilinear"]
MAX_PNG_INPUT_BYTES = 80 * 1024 * 1024
MAX_RASTER_PIXELS = 16_000_000


class RasterError(ValueError):
    """Raised when a raster or PNG violates this deliberately small contract."""


class GdiRenderError(OSError):
    """Raised when a Win32 GDI operation fails."""


class BITMAPINFOHEADER(ctypes.Structure):
    _fields_ = [
        ("biSize", wintypes.DWORD),
        ("biWidth", wintypes.LONG),
        ("biHeight", wintypes.LONG),
        ("biPlanes", wintypes.WORD),
        ("biBitCount", wintypes.WORD),
        ("biCompression", wintypes.DWORD),
        ("biSizeImage", wintypes.DWORD),
        ("biXPelsPerMeter", wintypes.LONG),
        ("biYPelsPerMeter", wintypes.LONG),
        ("biClrUsed", wintypes.DWORD),
        ("biClrImportant", wintypes.DWORD),
    ]


class RGBQUAD(ctypes.Structure):
    _fields_ = [
        ("rgbBlue", wintypes.BYTE),
        ("rgbGreen", wintypes.BYTE),
        ("rgbRed", wintypes.BYTE),
        ("rgbReserved", wintypes.BYTE),
    ]


class BITMAPINFO(ctypes.Structure):
    _fields_ = [("bmiHeader", BITMAPINFOHEADER), ("bmiColors", RGBQUAD * 1)]


class SIZE(ctypes.Structure):
    _fields_ = [("cx", wintypes.LONG), ("cy", wintypes.LONG)]


def bitmap_info(width: int, height: int) -> BITMAPINFO:
    _validate_raster_size(width, height)
    info = BITMAPINFO()
    info.bmiHeader.biSize = ctypes.sizeof(BITMAPINFOHEADER)
    info.bmiHeader.biWidth = int(width)
    info.bmiHeader.biHeight = -int(height)
    info.bmiHeader.biPlanes = 1
    info.bmiHeader.biBitCount = 32
    info.bmiHeader.biCompression = 0
    return info


def _validate_dimension(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 20_000:
        raise RasterError(f"{field} must be an integer from 1 through 20000")
    return value


def _validate_raster_size(width: int, height: int) -> tuple[int, int]:
    width = _validate_dimension(width, "width")
    height = _validate_dimension(height, "height")
    if width * height > MAX_RASTER_PIXELS:
        raise RasterError(f"raster exceeds the {MAX_RASTER_PIXELS}-pixel safety bound")
    return width, height


def _validate_color(value: Sequence[int]) -> Color:
    if len(value) != 3 or any(isinstance(item, bool) or not isinstance(item, int) or not 0 <= item <= 255 for item in value):
        raise RasterError("color must contain three integer RGB channels")
    return int(value[0]), int(value[1]), int(value[2])


def _colorref(color: Color) -> int:
    red, green, blue = _validate_color(color)
    return red | (green << 8) | (blue << 16)


def _png_chunk(kind: bytes, payload: bytes) -> bytes:
    return (
        struct.pack(">I", len(payload))
        + kind
        + payload
        + struct.pack(">I", zlib.crc32(kind + payload) & 0xFFFFFFFF)
    )


def _paeth(left: int, above: int, upper_left: int) -> int:
    estimate = left + above - upper_left
    left_distance = abs(estimate - left)
    above_distance = abs(estimate - above)
    upper_left_distance = abs(estimate - upper_left)
    if left_distance <= above_distance and left_distance <= upper_left_distance:
        return left
    if above_distance <= upper_left_distance:
        return above
    return upper_left


@dataclass(frozen=True)
class FontSpec:
    pixel_height: int
    family: str = "Malgun Gothic"
    bold: bool = False

    def __post_init__(self) -> None:
        if isinstance(self.pixel_height, bool) or not 1 <= int(self.pixel_height) <= 512:
            raise RasterError("font pixel_height must be from 1 through 512")
        if not str(self.family or "").strip() or len(str(self.family)) > 63:
            raise RasterError("font family is invalid")


@dataclass(frozen=True)
class RasterImage:
    """Top-down BGRA pixels with straight alpha."""

    width: int
    height: int
    bgra: bytes

    def __post_init__(self) -> None:
        _validate_raster_size(self.width, self.height)
        if len(self.bgra) != self.width * self.height * 4:
            raise RasterError("BGRA payload length does not match raster dimensions")

    @classmethod
    def solid(cls, width: int, height: int, color: Color = (255, 255, 255)) -> "RasterImage":
        width, height = _validate_raster_size(width, height)
        red, green, blue = _validate_color(color)
        return cls(width, height, bytes((blue, green, red, 255)) * (width * height))

    @classmethod
    def from_png(cls, path: str | os.PathLike[str]) -> "RasterImage":
        source = Path(path)
        if source.stat().st_size > MAX_PNG_INPUT_BYTES:
            raise RasterError(f"PNG exceeds the {MAX_PNG_INPUT_BYTES}-byte input bound")
        return cls.from_png_bytes(source.read_bytes())

    @classmethod
    def from_png_bytes(cls, raw: bytes) -> "RasterImage":
        if not isinstance(raw, bytes) or len(raw) > MAX_PNG_INPUT_BYTES:
            raise RasterError(f"PNG input must be bytes no larger than {MAX_PNG_INPUT_BYTES}")
        if not raw.startswith(b"\x89PNG\r\n\x1a\n"):
            raise RasterError("input is not a PNG")
        cursor = 8
        width = height = bit_depth = color_type = interlace = None
        compressed = bytearray()
        saw_end = False
        saw_idat = False
        idat_closed = False
        chunk_index = 0
        while cursor + 12 <= len(raw):
            length = struct.unpack(">I", raw[cursor : cursor + 4])[0]
            kind = raw[cursor + 4 : cursor + 8]
            start = cursor + 8
            end = start + length
            if end + 4 > len(raw):
                raise RasterError("PNG chunk exceeds input")
            payload = raw[start:end]
            expected_crc = struct.unpack(">I", raw[end : end + 4])[0]
            if (zlib.crc32(kind + payload) & 0xFFFFFFFF) != expected_crc:
                raise RasterError("PNG chunk CRC is invalid")
            cursor = end + 4
            if chunk_index == 0 and kind != b"IHDR":
                raise RasterError("PNG IHDR must be the first chunk")
            if kind == b"IHDR":
                if len(payload) != 13 or width is not None:
                    raise RasterError("PNG IHDR is invalid")
                width, height, bit_depth, color_type, compression, filtering, interlace = struct.unpack(">IIBBBBB", payload)
                _validate_raster_size(int(width), int(height))
                if bit_depth != 8 or color_type not in (2, 6) or compression or filtering or interlace:
                    raise RasterError("only non-interlaced 8-bit RGB/RGBA PNG is supported")
            elif kind == b"PLTE":
                if width is None or saw_idat:
                    raise RasterError("PNG PLTE chunk is out of order")
            elif kind == b"IDAT":
                if width is None or idat_closed:
                    raise RasterError("PNG IDAT chunks are out of order")
                compressed.extend(payload)
                saw_idat = True
            elif kind == b"IEND":
                if payload or not saw_idat:
                    raise RasterError("PNG IEND is invalid")
                saw_end = True
                break
            else:
                if kind[0] & 0x20 == 0:
                    raise RasterError(f"unsupported critical PNG chunk: {kind!r}")
                if saw_idat:
                    idat_closed = True
            chunk_index += 1
        if width is None or height is None or not compressed or not saw_end or cursor != len(raw):
            raise RasterError("PNG is missing required chunks")
        channels = 3 if color_type == 2 else 4
        row_bytes = int(width) * channels
        expected_filtered = int(height) * (row_bytes + 1)
        try:
            decompressor = zlib.decompressobj()
            filtered = decompressor.decompress(bytes(compressed), expected_filtered + 1)
        except zlib.error as exc:
            raise RasterError("PNG IDAT cannot be decompressed") from exc
        if (
            len(filtered) != expected_filtered
            or not decompressor.eof
            or decompressor.unconsumed_tail
            or decompressor.unused_data
        ):
            raise RasterError("PNG scanline length is invalid")
        previous = bytearray(row_bytes)
        offset = 0
        bgra = bytearray(int(width) * int(height) * 4)
        output_offset = 0
        for _row_index in range(int(height)):
            filter_kind = filtered[offset]
            offset += 1
            encoded = filtered[offset : offset + row_bytes]
            offset += row_bytes
            current = bytearray(row_bytes)
            for index, encoded_byte in enumerate(encoded):
                left = current[index - channels] if index >= channels else 0
                above = previous[index]
                upper_left = previous[index - channels] if index >= channels else 0
                if filter_kind == 0:
                    value = encoded_byte
                elif filter_kind == 1:
                    value = encoded_byte + left
                elif filter_kind == 2:
                    value = encoded_byte + above
                elif filter_kind == 3:
                    value = encoded_byte + ((left + above) // 2)
                elif filter_kind == 4:
                    value = encoded_byte + _paeth(left, above, upper_left)
                else:
                    raise RasterError("PNG scanline filter is unsupported")
                current[index] = value & 0xFF
            for pixel in range(int(width)):
                source = pixel * channels
                red, green, blue = current[source : source + 3]
                alpha = current[source + 3] if channels == 4 else 255
                bgra[output_offset : output_offset + 4] = bytes((blue, green, red, alpha))
                output_offset += 4
            previous = current
        return cls(int(width), int(height), bytes(bgra))

    def to_png_bytes(self, *, dpi: tuple[int, int] | None = None) -> bytes:
        preserve_alpha = any(alpha != 255 for alpha in self.bgra[3::4])
        scanlines = bytearray()
        stride = self.width * 4
        for row_index in range(self.height):
            scanlines.append(0)
            row = self.bgra[row_index * stride : (row_index + 1) * stride]
            for offset in range(0, len(row), 4):
                blue, green, red, alpha = row[offset : offset + 4]
                scanlines.extend((red, green, blue, alpha) if preserve_alpha else (red, green, blue))
        ihdr = struct.pack(">IIBBBBB", self.width, self.height, 8, 6 if preserve_alpha else 2, 0, 0, 0)
        chunks = [_png_chunk(b"IHDR", ihdr)]
        if dpi is not None:
            dpi_x, dpi_y = dpi
            if not 1 <= int(dpi_x) <= 9600 or not 1 <= int(dpi_y) <= 9600:
                raise RasterError("PNG DPI must be from 1 through 9600")
            chunks.append(
                _png_chunk(
                    b"pHYs",
                    struct.pack(">IIB", round(int(dpi_x) / 0.0254), round(int(dpi_y) / 0.0254), 1),
                )
            )
        chunks.extend(
            [
                _png_chunk(b"IDAT", zlib.compress(bytes(scanlines), level=9)),
                _png_chunk(b"IEND", b""),
            ]
        )
        return b"\x89PNG\r\n\x1a\n" + b"".join(chunks)

    def save_png(
        self,
        path: str | os.PathLike[str],
        *,
        dpi: tuple[int, int] | None = None,
        atomic: bool = True,
    ) -> dict[str, object]:
        target = Path(path).expanduser().absolute()
        if target.suffix.lower() != ".png":
            raise RasterError("PNG target must use a .png suffix")
        if target.is_symlink():
            raise RasterError("PNG target must not be a symbolic link")
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = self.to_png_bytes(dpi=dpi)
        if atomic:
            temporary = target.with_name(f".{target.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")
            try:
                with temporary.open("xb") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, target)
            finally:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    pass
        else:
            target.write_bytes(payload)
        return {
            "path": str(target),
            "width": self.width,
            "height": self.height,
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "dpi": list(dpi) if dpi else None,
        }

    def resized(self, width: int, height: int, *, resample: Resample = "bilinear") -> "RasterImage":
        width = _validate_dimension(width, "width")
        height = _validate_dimension(height, "height")
        if resample not in ("nearest", "bilinear"):
            raise RasterError("resample must be nearest or bilinear")
        if width == self.width and height == self.height:
            return self
        output = bytearray(width * height * 4)
        for target_y in range(height):
            if resample == "nearest":
                source_y = min(self.height - 1, (target_y * self.height) // height)
                for target_x in range(width):
                    source_x = min(self.width - 1, (target_x * self.width) // width)
                    source = (source_y * self.width + source_x) * 4
                    target = (target_y * width + target_x) * 4
                    output[target : target + 4] = self.bgra[source : source + 4]
                continue
            source_y_float = ((target_y + 0.5) * self.height / height) - 0.5
            y0 = max(0, min(self.height - 1, int(source_y_float)))
            y1 = min(self.height - 1, y0 + 1)
            fy = max(0.0, min(1.0, source_y_float - y0))
            for target_x in range(width):
                source_x_float = ((target_x + 0.5) * self.width / width) - 0.5
                x0 = max(0, min(self.width - 1, int(source_x_float)))
                x1 = min(self.width - 1, x0 + 1)
                fx = max(0.0, min(1.0, source_x_float - x0))
                target = (target_y * width + target_x) * 4
                for channel in range(4):
                    p00 = self.bgra[(y0 * self.width + x0) * 4 + channel]
                    p10 = self.bgra[(y0 * self.width + x1) * 4 + channel]
                    p01 = self.bgra[(y1 * self.width + x0) * 4 + channel]
                    p11 = self.bgra[(y1 * self.width + x1) * 4 + channel]
                    top = p00 + (p10 - p00) * fx
                    bottom = p01 + (p11 - p01) * fx
                    output[target + channel] = round(top + (bottom - top) * fy)
        return RasterImage(width, height, bytes(output))

    def contain(self, maximum: tuple[int, int], *, resample: Resample = "bilinear") -> "RasterImage":
        max_width = _validate_dimension(int(maximum[0]), "maximum width")
        max_height = _validate_dimension(int(maximum[1]), "maximum height")
        scale = min(max_width / self.width, max_height / self.height, 1.0)
        return self.resized(max(1, round(self.width * scale)), max(1, round(self.height * scale)), resample=resample)

    def to_tk_photo_image(self, *, master=None):
        """Create a native Tk 8.6 PNG PhotoImage without importing Pillow."""

        import tkinter as tk

        encoded = base64.b64encode(self.to_png_bytes()).decode("ascii")
        return tk.PhotoImage(master=master, data=encoded, format="png")


if os.name == "nt":
    _gdi32 = ctypes.WinDLL("gdi32.dll", use_last_error=True)
    _gdi32.CreateCompatibleDC.argtypes = (wintypes.HDC,)
    _gdi32.CreateCompatibleDC.restype = wintypes.HDC
    _gdi32.DeleteDC.argtypes = (wintypes.HDC,)
    _gdi32.DeleteDC.restype = wintypes.BOOL
    _gdi32.CreateDIBSection.argtypes = (
        wintypes.HDC,
        ctypes.POINTER(BITMAPINFO),
        wintypes.UINT,
        ctypes.POINTER(ctypes.c_void_p),
        wintypes.HANDLE,
        wintypes.DWORD,
    )
    _gdi32.CreateDIBSection.restype = wintypes.HBITMAP
    _gdi32.SelectObject.argtypes = (wintypes.HDC, wintypes.HGDIOBJ)
    _gdi32.SelectObject.restype = wintypes.HGDIOBJ
    _gdi32.DeleteObject.argtypes = (wintypes.HGDIOBJ,)
    _gdi32.DeleteObject.restype = wintypes.BOOL
    _gdi32.CreatePen.argtypes = (ctypes.c_int, ctypes.c_int, wintypes.COLORREF)
    _gdi32.CreatePen.restype = wintypes.HPEN
    _gdi32.CreateSolidBrush.argtypes = (wintypes.COLORREF,)
    _gdi32.CreateSolidBrush.restype = wintypes.HBRUSH
    _gdi32.GetStockObject.argtypes = (ctypes.c_int,)
    _gdi32.GetStockObject.restype = wintypes.HGDIOBJ
    _gdi32.Rectangle.argtypes = (wintypes.HDC, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int)
    _gdi32.Rectangle.restype = wintypes.BOOL
    _gdi32.Ellipse.argtypes = (wintypes.HDC, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int)
    _gdi32.Ellipse.restype = wintypes.BOOL
    _gdi32.MoveToEx.argtypes = (wintypes.HDC, ctypes.c_int, ctypes.c_int, ctypes.c_void_p)
    _gdi32.MoveToEx.restype = wintypes.BOOL
    _gdi32.LineTo.argtypes = (wintypes.HDC, ctypes.c_int, ctypes.c_int)
    _gdi32.LineTo.restype = wintypes.BOOL
    _gdi32.CreateFontW.argtypes = (
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPCWSTR,
    )
    _gdi32.CreateFontW.restype = wintypes.HFONT
    _gdi32.SetBkMode.argtypes = (wintypes.HDC, ctypes.c_int)
    _gdi32.SetBkMode.restype = ctypes.c_int
    _gdi32.SetTextColor.argtypes = (wintypes.HDC, wintypes.COLORREF)
    _gdi32.SetTextColor.restype = wintypes.COLORREF
    _gdi32.TextOutW.argtypes = (wintypes.HDC, ctypes.c_int, ctypes.c_int, wintypes.LPCWSTR, ctypes.c_int)
    _gdi32.TextOutW.restype = wintypes.BOOL
    _gdi32.GetTextExtentPoint32W.argtypes = (
        wintypes.HDC,
        wintypes.LPCWSTR,
        ctypes.c_int,
        ctypes.POINTER(SIZE),
    )
    _gdi32.GetTextExtentPoint32W.restype = wintypes.BOOL
    _gdi32.GdiFlush.argtypes = ()
    _gdi32.GdiFlush.restype = wintypes.BOOL


def _require(value, operation: str):
    numeric = int(value) if value is not None else 0
    if not numeric or numeric == -1 or numeric == 0xFFFFFFFFFFFFFFFF:
        raise GdiRenderError(ctypes.get_last_error(), operation)
    return value


class RasterCanvas:
    """A closeable top-down 32-bit GDI DIB canvas."""

    DIB_RGB_COLORS = 0
    NULL_BRUSH = 5
    NULL_PEN = 8
    TRANSPARENT = 1

    def __init__(self, width: int, height: int, *, background: Color = (255, 255, 255)) -> None:
        if os.name != "nt":
            raise OSError("RasterCanvas requires Windows GDI")
        self.width, self.height = _validate_raster_size(width, height)
        background = _validate_color(background)
        self._info = bitmap_info(self.width, self.height)
        self._bits = ctypes.c_void_p()
        bitmap = _require(
            _gdi32.CreateDIBSection(None, ctypes.byref(self._info), self.DIB_RGB_COLORS, ctypes.byref(self._bits), None, 0),
            "CreateDIBSection",
        )
        try:
            dc = _require(_gdi32.CreateCompatibleDC(None), "CreateCompatibleDC")
        except Exception:
            _gdi32.DeleteObject(bitmap)
            raise
        try:
            old_bitmap = _require(_gdi32.SelectObject(dc, bitmap), "SelectObject(bitmap)")
        except Exception:
            _gdi32.DeleteDC(dc)
            _gdi32.DeleteObject(bitmap)
            raise
        self._bitmap = bitmap
        self._dc = dc
        self._old_bitmap = old_bitmap
        self._closed = False
        try:
            self.fill_rect((0, 0, self.width, self.height), background)
        except Exception:
            self.close()
            raise

    @property
    def hdc(self):
        self._ensure_open()
        return self._dc

    @property
    def bitmap_info(self) -> BITMAPINFO:
        self._ensure_open()
        return self._info

    @property
    def bits_pointer(self) -> ctypes.c_void_p:
        self._ensure_open()
        return self._bits

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("raster canvas is closed")

    def __enter__(self) -> "RasterCanvas":
        self._ensure_open()
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        _gdi32.SelectObject(self._dc, self._old_bitmap)
        _gdi32.DeleteObject(self._bitmap)
        _gdi32.DeleteDC(self._dc)
        self._bits = ctypes.c_void_p()
        self._bitmap = None
        self._dc = None
        self._old_bitmap = None
        self._closed = True

    def _with_pen_brush(self, *, color: Color, width: int, fill: Color | None, operation) -> None:
        self._ensure_open()
        pen = _require(_gdi32.CreatePen(0, max(1, int(width)), _colorref(color)), "CreatePen")
        try:
            brush = _require(
                _gdi32.CreateSolidBrush(_colorref(fill)) if fill is not None else _gdi32.GetStockObject(self.NULL_BRUSH),
                "CreateSolidBrush/GetStockObject",
            )
            try:
                old_pen = _require(_gdi32.SelectObject(self._dc, pen), "SelectObject(pen)")
                try:
                    old_brush = _require(_gdi32.SelectObject(self._dc, brush), "SelectObject(brush)")
                    try:
                        _require(operation(), "GDI shape")
                    finally:
                        _gdi32.SelectObject(self._dc, old_brush)
                finally:
                    _gdi32.SelectObject(self._dc, old_pen)
            finally:
                if fill is not None:
                    _gdi32.DeleteObject(brush)
        finally:
            _gdi32.DeleteObject(pen)

    def fill_rect(self, box: tuple[int, int, int, int], color: Color) -> None:
        left, top, right, bottom = (int(value) for value in box)
        if right <= left or bottom <= top:
            raise RasterError("rectangle must have positive width and height")
        self._with_pen_brush(
            color=color,
            width=1,
            fill=color,
            operation=lambda: _gdi32.Rectangle(self._dc, left, top, right, bottom),
        )

    def rectangle(
        self,
        box: tuple[int, int, int, int],
        *,
        outline: Color = (0, 0, 0),
        width: int = 1,
        fill: Color | None = None,
    ) -> None:
        left, top, right, bottom = (int(value) for value in box)
        if right <= left or bottom <= top:
            raise RasterError("rectangle must have positive width and height")
        self._with_pen_brush(
            color=outline,
            width=width,
            fill=fill,
            operation=lambda: _gdi32.Rectangle(self._dc, left, top, right, bottom),
        )

    def ellipse(
        self,
        box: tuple[int, int, int, int],
        *,
        outline: Color = (0, 0, 0),
        width: int = 1,
        fill: Color | None = None,
    ) -> None:
        left, top, right, bottom = (int(value) for value in box)
        if right <= left or bottom <= top:
            raise RasterError("ellipse must have positive width and height")
        self._with_pen_brush(
            color=outline,
            width=width,
            fill=fill,
            operation=lambda: _gdi32.Ellipse(self._dc, left, top, right, bottom),
        )

    def line(
        self,
        start: tuple[int, int],
        end: tuple[int, int],
        *,
        color: Color = (0, 0, 0),
        width: int = 1,
    ) -> None:
        self._ensure_open()
        pen = _require(_gdi32.CreatePen(0, max(1, int(width)), _colorref(color)), "CreatePen")
        try:
            old_pen = _require(_gdi32.SelectObject(self._dc, pen), "SelectObject(pen)")
            try:
                _require(_gdi32.MoveToEx(self._dc, int(start[0]), int(start[1]), None), "MoveToEx")
                _require(_gdi32.LineTo(self._dc, int(end[0]), int(end[1])), "LineTo")
            finally:
                _gdi32.SelectObject(self._dc, old_pen)
        finally:
            _gdi32.DeleteObject(pen)

    @staticmethod
    def _create_font(spec: FontSpec):
        return _require(
            _gdi32.CreateFontW(
                -int(spec.pixel_height),
                0,
                0,
                0,
                700 if spec.bold else 400,
                0,
                0,
                0,
                1,
                0,
                0,
                5,
                0,
                str(spec.family),
            ),
            "CreateFontW",
        )

    def text_size(self, text: str, font: FontSpec) -> tuple[int, int]:
        self._ensure_open()
        value = str(text)
        handle = self._create_font(font)
        try:
            previous = _require(_gdi32.SelectObject(self._dc, handle), "SelectObject(font)")
            extent = SIZE()
            try:
                _require(_gdi32.GetTextExtentPoint32W(self._dc, value, len(value), ctypes.byref(extent)), "GetTextExtentPoint32W")
                return int(extent.cx), int(extent.cy)
            finally:
                _gdi32.SelectObject(self._dc, previous)
        finally:
            _gdi32.DeleteObject(handle)

    def fit_font(
        self,
        text: str,
        maximum_width: int,
        start_height: int,
        *,
        minimum_height: int = 10,
        family: str = "Malgun Gothic",
        bold: bool = False,
    ) -> FontSpec:
        for height in range(int(start_height), int(minimum_height) - 1, -2):
            candidate = FontSpec(height, family=family, bold=bold)
            if self.text_size(text, candidate)[0] <= maximum_width:
                return candidate
        return FontSpec(int(minimum_height), family=family, bold=bold)

    def text(
        self,
        position: tuple[int, int],
        value: object,
        *,
        font: FontSpec,
        fill: Color = (0, 0, 0),
        anchor: Anchor = "lt",
    ) -> None:
        self._ensure_open()
        text = str(value)
        if anchor not in ("lt", "mt", "mm", "lm"):
            raise RasterError("text anchor is unsupported")
        handle = self._create_font(font)
        try:
            previous = _require(_gdi32.SelectObject(self._dc, handle), "SelectObject(font)")
            try:
                _gdi32.SetBkMode(self._dc, self.TRANSPARENT)
                _gdi32.SetTextColor(self._dc, _colorref(fill))
                extent = SIZE()
                _require(_gdi32.GetTextExtentPoint32W(self._dc, text, len(text), ctypes.byref(extent)), "GetTextExtentPoint32W")
                width, height = int(extent.cx), int(extent.cy)
                x, y = int(position[0]), int(position[1])
                if anchor in ("mt", "mm"):
                    x -= width // 2
                if anchor in ("lm", "mm"):
                    y -= height // 2
                _require(_gdi32.TextOutW(self._dc, x, y, text, len(text)), "TextOutW")
            finally:
                _gdi32.SelectObject(self._dc, previous)
        finally:
            _gdi32.DeleteObject(handle)

    def blit(
        self,
        image: RasterImage,
        box: tuple[int, int, int, int],
        *,
        resample: Resample = "bilinear",
    ) -> None:
        self._ensure_open()
        left, top, right, bottom = (int(value) for value in box)
        if right <= left or bottom <= top:
            raise RasterError("image placement must have positive width and height")
        scaled = image.resized(right - left, bottom - top, resample=resample)
        target = (ctypes.c_ubyte * (self.width * self.height * 4)).from_address(self._bits.value)
        for source_y in range(scaled.height):
            target_y = top + source_y
            if not 0 <= target_y < self.height:
                continue
            for source_x in range(scaled.width):
                target_x = left + source_x
                if not 0 <= target_x < self.width:
                    continue
                source_offset = (source_y * scaled.width + source_x) * 4
                target_offset = (target_y * self.width + target_x) * 4
                blue, green, red, alpha = scaled.bgra[source_offset : source_offset + 4]
                if alpha == 255:
                    target[target_offset : target_offset + 4] = bytes((blue, green, red, 0))
                    continue
                inverse = 255 - alpha
                target[target_offset] = (blue * alpha + target[target_offset] * inverse + 127) // 255
                target[target_offset + 1] = (green * alpha + target[target_offset + 1] * inverse + 127) // 255
                target[target_offset + 2] = (red * alpha + target[target_offset + 2] * inverse + 127) // 255
                target[target_offset + 3] = 0

    def qr(
        self,
        payload: str,
        box: tuple[int, int, int, int],
        *,
        error_correction: Literal["L", "M", "Q", "H"] = "M",
        border: int = 4,
        mask_pattern: int | None = None,
    ) -> dict[str, object]:
        import qrcode
        from qrcode.exceptions import DataOverflowError

        self._ensure_open()
        levels = {
            "L": qrcode.constants.ERROR_CORRECT_L,
            "M": qrcode.constants.ERROR_CORRECT_M,
            "Q": qrcode.constants.ERROR_CORRECT_Q,
            "H": qrcode.constants.ERROR_CORRECT_H,
        }
        if error_correction not in levels:
            raise RasterError("QR error correction level is invalid")
        if isinstance(border, bool) or not isinstance(border, int) or not 0 <= border <= 16:
            raise RasterError("QR border must be from 0 through 16 modules")
        if mask_pattern is not None and (
            isinstance(mask_pattern, bool) or not isinstance(mask_pattern, int) or not 0 <= mask_pattern <= 7
        ):
            raise RasterError("QR mask_pattern must be from 0 through 7")
        payload_text = str(payload)
        try:
            payload_bytes = payload_text.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise RasterError("QR payload is not valid Unicode") from exc
        if len(payload_bytes) > 4096:
            raise RasterError("QR payload exceeds the 4096-byte safety bound")
        qr = qrcode.QRCode(
            version=None,
            error_correction=levels[error_correction],
            box_size=1,
            border=int(border),
            mask_pattern=mask_pattern,
        )
        try:
            qr.add_data(payload_text)
            qr.make(fit=True)
        except DataOverflowError as exc:
            raise RasterError("QR payload does not fit the supported symbol") from exc
        matrix = qr.get_matrix()
        left, top, right, bottom = (int(value) for value in box)
        count = len(matrix)
        if (
            right <= left
            or bottom <= top
            or right - left != bottom - top
            or not (0 <= left < right <= self.width and 0 <= top < bottom <= self.height)
            or not matrix
            or count != len(matrix[0])
            or right - left < count
        ):
            raise RasterError("QR placement box is invalid")
        self.fill_rect((left, top, right, bottom), (255, 255, 255))
        for row_index, row in enumerate(matrix):
            y0 = top + (row_index * (bottom - top)) // count
            y1 = top + ((row_index + 1) * (bottom - top)) // count
            for column_index, active in enumerate(row):
                if not active:
                    continue
                x0 = left + (column_index * (right - left)) // count
                x1 = left + ((column_index + 1) * (right - left)) // count
                self.fill_rect((x0, y0, max(x0 + 1, x1), max(y0 + 1, y1)), (0, 0, 0))
        return {
            "payload": payload_text,
            "matrix_width": count,
            "matrix_height": len(matrix),
            "box": [left, top, right, bottom],
            "error_correction": error_correction,
            "border": int(border),
            "mask_pattern": mask_pattern,
        }

    def barcode_rows(
        self,
        barcodes: Iterable[object],
        box: tuple[int, int, int, int],
        *,
        font: FontSpec,
        columns: int = 3,
        row_height: int = 34,
        fill: Color = (0, 0, 0),
    ) -> int:
        """Draw the Defect A4 accepted-product barcode list as text rows."""

        if columns < 1 or row_height < 1:
            raise RasterError("barcode row geometry is invalid")
        left, top, right, bottom = (int(value) for value in box)
        column_width = max(1, (right - left) // columns)
        count = 0
        for index, barcode in enumerate(barcodes):
            column = index % columns
            row = index // columns
            y = top + row * row_height
            if y + row_height > bottom:
                break
            self.text(
                (left + column * column_width, y),
                f"{index + 1:02d}. {barcode}",
                font=font,
                fill=fill,
            )
            count += 1
        return count

    def snapshot(self) -> RasterImage:
        self._ensure_open()
        _require(_gdi32.GdiFlush(), "GdiFlush")
        raw = bytearray(ctypes.string_at(self._bits, self.width * self.height * 4))
        raw[3::4] = b"\xff" * (self.width * self.height)
        return RasterImage(self.width, self.height, bytes(raw))


__all__ = [
    "Anchor",
    "BITMAPINFO",
    "FontSpec",
    "GdiRenderError",
    "RasterCanvas",
    "RasterError",
    "RasterImage",
    "bitmap_info",
]
