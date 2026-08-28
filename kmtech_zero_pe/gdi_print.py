"""Deterministic per-job Win32 GDI printing without Pillow or pywin32."""

from __future__ import annotations

import ctypes
from ctypes import wintypes
from dataclasses import asdict, dataclass
import hashlib
import os
from pathlib import Path
import time

from .raster import BITMAPINFO, RasterImage, bitmap_info


MAX_PRINT_OUTPUT_BYTES = 512 * 1024 * 1024


class PrinterError(OSError):
    """A printer API or driver contract failed closed."""


@dataclass(frozen=True)
class PaperSpec:
    """Per-job driver settings; no value is written to the printer default."""

    orientation: str = "portrait"
    paper_size_code: int | None = 9
    width_mm: float | None = None
    height_mm: float | None = None
    form_name: str | None = None
    driver_scale_percent: int = 100

    def __post_init__(self) -> None:
        if self.orientation not in {"portrait", "landscape"}:
            raise ValueError("orientation must be portrait or landscape")
        custom = self.width_mm is not None or self.height_mm is not None
        if custom and (self.width_mm is None or self.height_mm is None):
            raise ValueError("custom paper requires width_mm and height_mm")
        selectors = int(self.paper_size_code is not None) + int(custom) + int(self.form_name is not None)
        if selectors != 1:
            raise ValueError("select exactly one paper_size_code, custom dimension pair, or form_name")
        if custom and not (1 <= float(self.width_mm) <= 3276.7 and 1 <= float(self.height_mm) <= 3276.7):
            raise ValueError("custom paper dimensions exceed DEVMODE bounds")
        if self.form_name is not None and (not self.form_name.strip() or len(self.form_name) > 31):
            raise ValueError("form_name must contain from 1 through 31 characters")
        if not 1 <= int(self.driver_scale_percent) <= 100:
            raise ValueError("driver_scale_percent must be from 1 through 100")


@dataclass(frozen=True)
class MarginsMM:
    left: float = 0.0
    top: float = 0.0
    right: float = 0.0
    bottom: float = 0.0

    def __post_init__(self) -> None:
        if any(float(value) < 0 or float(value) > 1000 for value in (self.left, self.top, self.right, self.bottom)):
            raise ValueError("margins must be non-negative and bounded")


@dataclass(frozen=True)
class PrintSpec:
    document_name: str
    paper: PaperSpec = PaperSpec()
    margins_mm: MarginsMM = MarginsMM()
    content_scale_percent: int = 100
    output_path: str | os.PathLike[str] | None = None
    overwrite_output: bool = False

    def __post_init__(self) -> None:
        if not str(self.document_name or "").strip():
            raise ValueError("document_name is required")
        if not 1 <= int(self.content_scale_percent) <= 100:
            raise ValueError("content_scale_percent must be from 1 through 100")


@dataclass(frozen=True)
class PrintReceipt:
    printer_name: str
    job_id: int
    copied_scanlines: int
    document_name: str
    output_path: str | None
    output_bytes: int | None
    output_sha256: str | None
    default_printer_before: str
    default_printer_after: str
    default_printer_unchanged: bool
    default_devmode_sha256_before: str
    default_devmode_sha256_after: str
    default_devmode_unchanged: bool
    requested_paper: dict[str, object]
    validated_devmode: dict[str, object]
    device_caps: dict[str, int]
    geometry: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


class POINTL(ctypes.Structure):
    _fields_ = [("x", wintypes.LONG), ("y", wintypes.LONG)]


class PRINTER_DEVMODE_FIELDS(ctypes.Structure):
    _fields_ = [
        ("dmOrientation", ctypes.c_short),
        ("dmPaperSize", ctypes.c_short),
        ("dmPaperLength", ctypes.c_short),
        ("dmPaperWidth", ctypes.c_short),
        ("dmScale", ctypes.c_short),
        ("dmCopies", ctypes.c_short),
        ("dmDefaultSource", ctypes.c_short),
        ("dmPrintQuality", ctypes.c_short),
    ]


class DISPLAY_DEVMODE_FIELDS(ctypes.Structure):
    _fields_ = [
        ("dmPosition", POINTL),
        ("dmDisplayOrientation", wintypes.DWORD),
        ("dmDisplayFixedOutput", wintypes.DWORD),
    ]


class DEVMODE_UNION(ctypes.Union):
    _anonymous_ = ("printer",)
    _fields_ = [("printer", PRINTER_DEVMODE_FIELDS), ("display", DISPLAY_DEVMODE_FIELDS)]


class DEVMODEW(ctypes.Structure):
    _anonymous_ = ("u",)
    _fields_ = [
        ("dmDeviceName", ctypes.c_wchar * 32),
        ("dmSpecVersion", wintypes.WORD),
        ("dmDriverVersion", wintypes.WORD),
        ("dmSize", wintypes.WORD),
        ("dmDriverExtra", wintypes.WORD),
        ("dmFields", wintypes.DWORD),
        ("u", DEVMODE_UNION),
        ("dmColor", ctypes.c_short),
        ("dmDuplex", ctypes.c_short),
        ("dmYResolution", ctypes.c_short),
        ("dmTTOption", ctypes.c_short),
        ("dmCollate", ctypes.c_short),
        ("dmFormName", ctypes.c_wchar * 32),
        ("dmLogPixels", wintypes.WORD),
        ("dmBitsPerPel", wintypes.DWORD),
        ("dmPelsWidth", wintypes.DWORD),
        ("dmPelsHeight", wintypes.DWORD),
        ("dmDisplayFlags", wintypes.DWORD),
        ("dmDisplayFrequency", wintypes.DWORD),
        ("dmICMMethod", wintypes.DWORD),
        ("dmICMIntent", wintypes.DWORD),
        ("dmMediaType", wintypes.DWORD),
        ("dmDitherType", wintypes.DWORD),
        ("dmReserved1", wintypes.DWORD),
        ("dmReserved2", wintypes.DWORD),
        ("dmPanningWidth", wintypes.DWORD),
        ("dmPanningHeight", wintypes.DWORD),
    ]


class DOCINFOW(ctypes.Structure):
    _fields_ = [
        ("cbSize", ctypes.c_int),
        ("lpszDocName", wintypes.LPCWSTR),
        ("lpszOutput", wintypes.LPCWSTR),
        ("lpszDatatype", wintypes.LPCWSTR),
        ("fwType", wintypes.DWORD),
    ]


DM_ORIENTATION = 0x00000001
DM_PAPERSIZE = 0x00000002
DM_PAPERLENGTH = 0x00000004
DM_PAPERWIDTH = 0x00000008
DM_SCALE = 0x00000010
DM_FORMNAME = 0x00010000
DMPAPER_USER = 256
DMORIENT_PORTRAIT = 1
DMORIENT_LANDSCAPE = 2
DM_OUT_BUFFER = 0x00000002
DM_IN_BUFFER = 0x00000008
IDOK = 1
HORZRES = 8
VERTRES = 10
LOGPIXELSX = 88
LOGPIXELSY = 90
PHYSICALWIDTH = 110
PHYSICALHEIGHT = 111
PHYSICALOFFSETX = 112
PHYSICALOFFSETY = 113
DIB_RGB_COLORS = 0
SRCCOPY = 0x00CC0020


if os.name == "nt":
    _gdi32 = ctypes.WinDLL("gdi32.dll", use_last_error=True)
    _winspool = ctypes.WinDLL("winspool.drv", use_last_error=True)
    _gdi32.CreateDCW.argtypes = (wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.LPCWSTR, ctypes.c_void_p)
    _gdi32.CreateDCW.restype = wintypes.HDC
    _gdi32.DeleteDC.argtypes = (wintypes.HDC,)
    _gdi32.DeleteDC.restype = wintypes.BOOL
    _gdi32.GetDeviceCaps.argtypes = (wintypes.HDC, ctypes.c_int)
    _gdi32.GetDeviceCaps.restype = ctypes.c_int
    _gdi32.StartDocW.argtypes = (wintypes.HDC, ctypes.POINTER(DOCINFOW))
    _gdi32.StartDocW.restype = ctypes.c_int
    _gdi32.StartPage.argtypes = (wintypes.HDC,)
    _gdi32.StartPage.restype = ctypes.c_int
    _gdi32.EndPage.argtypes = (wintypes.HDC,)
    _gdi32.EndPage.restype = ctypes.c_int
    _gdi32.EndDoc.argtypes = (wintypes.HDC,)
    _gdi32.EndDoc.restype = ctypes.c_int
    _gdi32.AbortDoc.argtypes = (wintypes.HDC,)
    _gdi32.AbortDoc.restype = ctypes.c_int
    _gdi32.StretchDIBits.argtypes = (
        wintypes.HDC,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.POINTER(BITMAPINFO),
        wintypes.UINT,
        wintypes.DWORD,
    )
    _gdi32.StretchDIBits.restype = ctypes.c_int
    _winspool.OpenPrinterW.argtypes = (wintypes.LPWSTR, ctypes.POINTER(wintypes.HANDLE), ctypes.c_void_p)
    _winspool.OpenPrinterW.restype = wintypes.BOOL
    _winspool.ClosePrinter.argtypes = (wintypes.HANDLE,)
    _winspool.ClosePrinter.restype = wintypes.BOOL
    _winspool.DocumentPropertiesW.argtypes = (
        wintypes.HWND,
        wintypes.HANDLE,
        wintypes.LPWSTR,
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.DWORD,
    )
    _winspool.DocumentPropertiesW.restype = wintypes.LONG
    _winspool.GetDefaultPrinterW.argtypes = (wintypes.LPWSTR, ctypes.POINTER(wintypes.DWORD))
    _winspool.GetDefaultPrinterW.restype = wintypes.BOOL


def _require(value, operation: str):
    if not value:
        raise PrinterError(ctypes.get_last_error(), operation)
    return value


def _default_printer_name() -> str:
    size = wintypes.DWORD(0)
    _winspool.GetDefaultPrinterW(None, ctypes.byref(size))
    if size.value < 2:
        return ""
    buffer = ctypes.create_unicode_buffer(size.value)
    _require(_winspool.GetDefaultPrinterW(buffer, ctypes.byref(size)), "GetDefaultPrinterW")
    return str(buffer.value)


def _devmode_values(buffer: ctypes.Array) -> dict[str, object]:
    devmode = ctypes.cast(buffer, ctypes.POINTER(DEVMODEW)).contents
    return {
        "ctypes_structure_size": ctypes.sizeof(DEVMODEW),
        "dm_size": int(devmode.dmSize),
        "dm_driver_extra": int(devmode.dmDriverExtra),
        "dm_fields": int(devmode.dmFields),
        "dm_orientation": int(devmode.dmOrientation),
        "dm_paper_size": int(devmode.dmPaperSize),
        "dm_paper_length_tenth_mm": int(devmode.dmPaperLength),
        "dm_paper_width_tenth_mm": int(devmode.dmPaperWidth),
        "dm_scale_percent": int(devmode.dmScale),
        "dm_form_name": str(devmode.dmFormName),
    }


def _opaque_bgra(image: RasterImage) -> bytes:
    if all(alpha == 255 for alpha in image.bgra[3::4]):
        mutable = bytearray(image.bgra)
        mutable[3::4] = b"\x00" * (image.width * image.height)
        return bytes(mutable)
    output = bytearray(len(image.bgra))
    for offset in range(0, len(image.bgra), 4):
        blue, green, red, alpha = image.bgra[offset : offset + 4]
        output[offset : offset + 4] = bytes(
            (
                (blue * alpha + 255 * (255 - alpha) + 127) // 255,
                (green * alpha + 255 * (255 - alpha) + 127) // 255,
                (red * alpha + 255 * (255 - alpha) + 127) // 255,
                0,
            )
        )
    return bytes(output)


class GdiPrinter:
    def __init__(self, printer_name: str | None = None) -> None:
        if os.name != "nt":
            raise OSError("GdiPrinter requires Windows")
        self.printer_name = str(printer_name or _default_printer_name()).strip()
        if not self.printer_name:
            raise PrinterError("no printer is selected")

    def _open(self):
        handle = wintypes.HANDLE()
        _require(_winspool.OpenPrinterW(self.printer_name, ctypes.byref(handle), None), "OpenPrinterW")
        return handle

    def _default_devmode(self, handle) -> ctypes.Array:
        size = int(_winspool.DocumentPropertiesW(None, handle, self.printer_name, None, None, 0))
        if size < ctypes.sizeof(DEVMODEW):
            raise PrinterError(size, "DocumentPropertiesW(size)")
        buffer = ctypes.create_string_buffer(size)
        status = int(_winspool.DocumentPropertiesW(None, handle, self.printer_name, buffer, None, DM_OUT_BUFFER))
        if status != IDOK:
            raise PrinterError(status, "DocumentPropertiesW(default)")
        if int(ctypes.cast(buffer, ctypes.POINTER(DEVMODEW)).contents.dmSize) != ctypes.sizeof(DEVMODEW):
            raise PrinterError("driver DEVMODE layout does not match the public DEVMODEW layout")
        return buffer

    def _job_devmode(self, handle, source: ctypes.Array, paper: PaperSpec) -> tuple[ctypes.Array, dict[str, object]]:
        buffer = ctypes.create_string_buffer(source.raw, len(source.raw))
        devmode = ctypes.cast(buffer, ctypes.POINTER(DEVMODEW)).contents
        devmode.dmFields |= DM_ORIENTATION | DM_SCALE
        devmode.dmOrientation = DMORIENT_LANDSCAPE if paper.orientation == "landscape" else DMORIENT_PORTRAIT
        devmode.dmScale = int(paper.driver_scale_percent)
        devmode.dmFields &= ~(DM_PAPERSIZE | DM_PAPERWIDTH | DM_PAPERLENGTH | DM_FORMNAME)
        if paper.form_name:
            devmode.dmFields |= DM_FORMNAME
            devmode.dmFormName = paper.form_name
        if paper.width_mm is not None and paper.height_mm is not None:
            devmode.dmFields &= ~DM_PAPERSIZE
            devmode.dmFields |= DM_PAPERWIDTH | DM_PAPERLENGTH
            devmode.dmPaperSize = 0
            devmode.dmPaperWidth = round(float(paper.width_mm) * 10)
            devmode.dmPaperLength = round(float(paper.height_mm) * 10)
        elif paper.paper_size_code is not None:
            devmode.dmFields |= DM_PAPERSIZE
            devmode.dmPaperSize = int(paper.paper_size_code)
        status = int(
            _winspool.DocumentPropertiesW(
                None,
                handle,
                self.printer_name,
                buffer,
                buffer,
                DM_IN_BUFFER | DM_OUT_BUFFER,
            )
        )
        if status != IDOK:
            raise PrinterError(status, "DocumentPropertiesW(validate per-job DEVMODE)")
        values = _devmode_values(buffer)
        normalized_fields = int(values["dm_fields"])
        if not normalized_fields & DM_ORIENTATION:
            raise PrinterError(f"driver cleared the required orientation field: {values}")
        expected_orientation = DMORIENT_LANDSCAPE if paper.orientation == "landscape" else DMORIENT_PORTRAIT
        if values["dm_orientation"] != expected_orientation:
            raise PrinterError(f"driver rejected orientation: {values}")
        if normalized_fields & DM_SCALE:
            if values["dm_scale_percent"] != int(paper.driver_scale_percent):
                raise PrinterError(f"driver rejected scale: {values}")
        elif int(paper.driver_scale_percent) != 100:
            raise PrinterError(f"driver does not support the requested non-default scale: {values}")
        if paper.paper_size_code is not None:
            if not normalized_fields & DM_PAPERSIZE or normalized_fields & (DM_PAPERWIDTH | DM_PAPERLENGTH):
                raise PrinterError(f"driver returned conflicting paper_size_code fields: {values}")
            if values["dm_paper_size"] != int(paper.paper_size_code):
                raise PrinterError(f"driver rejected paper_size_code: {values}")
        if paper.width_mm is not None and (
            normalized_fields & (DM_PAPERWIDTH | DM_PAPERLENGTH) != (DM_PAPERWIDTH | DM_PAPERLENGTH)
            or (normalized_fields & DM_PAPERSIZE and values["dm_paper_size"] not in (0, DMPAPER_USER))
            or values["dm_paper_width_tenth_mm"] != round(float(paper.width_mm) * 10)
            or values["dm_paper_length_tenth_mm"] != round(float(paper.height_mm) * 10)
        ):
            raise PrinterError(f"driver rejected or conflicted custom paper dimensions: {values}")
        if paper.form_name:
            if not normalized_fields & DM_FORMNAME or normalized_fields & (DM_PAPERWIDTH | DM_PAPERLENGTH):
                raise PrinterError(f"driver returned conflicting form_name fields: {values}")
            if values["dm_form_name"] != paper.form_name:
                raise PrinterError(f"driver rejected form_name: {values}")
        return buffer, values

    def validate_paper(self, paper: PaperSpec) -> dict[str, object]:
        """Return a driver's normalized job-local DEVMODE without printing or updating defaults."""

        printer = self._open()
        try:
            source = self._default_devmode(printer)
            _buffer, values = self._job_devmode(printer, source, paper)
            return values
        finally:
            _winspool.ClosePrinter(printer)

    @staticmethod
    def _caps(hdc) -> dict[str, int]:
        caps = {
            "horzres": int(_gdi32.GetDeviceCaps(hdc, HORZRES)),
            "vertres": int(_gdi32.GetDeviceCaps(hdc, VERTRES)),
            "logpixelsx": int(_gdi32.GetDeviceCaps(hdc, LOGPIXELSX)),
            "logpixelsy": int(_gdi32.GetDeviceCaps(hdc, LOGPIXELSY)),
            "physicalwidth": int(_gdi32.GetDeviceCaps(hdc, PHYSICALWIDTH)),
            "physicalheight": int(_gdi32.GetDeviceCaps(hdc, PHYSICALHEIGHT)),
            "physicaloffsetx": int(_gdi32.GetDeviceCaps(hdc, PHYSICALOFFSETX)),
            "physicaloffsety": int(_gdi32.GetDeviceCaps(hdc, PHYSICALOFFSETY)),
        }
        if min(caps.values()) < 0 or not caps["horzres"] or not caps["vertres"] or not caps["logpixelsx"] or not caps["logpixelsy"]:
            raise PrinterError(f"printer returned invalid device caps: {caps}")
        return caps

    @staticmethod
    def _geometry(image: RasterImage, caps: dict[str, int], spec: PrintSpec) -> dict[str, object]:
        margins = spec.margins_mm
        left = max(0, round(float(margins.left) * caps["logpixelsx"] / 25.4) - caps["physicaloffsetx"])
        top = max(0, round(float(margins.top) * caps["logpixelsy"] / 25.4) - caps["physicaloffsety"])
        right = min(
            caps["horzres"],
            caps["physicalwidth"] - caps["physicaloffsetx"] - round(float(margins.right) * caps["logpixelsx"] / 25.4),
        )
        bottom = min(
            caps["vertres"],
            caps["physicalheight"] - caps["physicaloffsety"] - round(float(margins.bottom) * caps["logpixelsy"] / 25.4),
        )
        if right <= left or bottom <= top:
            raise PrinterError("requested physical margins leave no imageable content box")
        fit_scale = min((right - left) / image.width, (bottom - top) / image.height)
        scale = fit_scale * int(spec.content_scale_percent) / 100
        output_width = max(1, round(image.width * scale))
        output_height = max(1, round(image.height * scale))
        output_left = left + ((right - left) - output_width) // 2
        output_top = top + ((bottom - top) - output_height) // 2
        return {
            "content_box_device_pixels": [left, top, right - left, bottom - top],
            "destination_rect_device_pixels": [output_left, output_top, output_width, output_height],
            "fit_scale_device_pixels_per_source_pixel": fit_scale,
            "effective_scale_device_pixels_per_source_pixel": scale,
            "requested_margins_mm": asdict(margins),
            "physical_page_mm": [
                caps["physicalwidth"] * 25.4 / caps["logpixelsx"],
                caps["physicalheight"] * 25.4 / caps["logpixelsy"],
            ],
            "imageable_origin_mm": [
                caps["physicaloffsetx"] * 25.4 / caps["logpixelsx"],
                caps["physicaloffsety"] * 25.4 / caps["logpixelsy"],
            ],
            "destination_origin_mm": [
                (output_left + caps["physicaloffsetx"]) * 25.4 / caps["logpixelsx"],
                (output_top + caps["physicaloffsety"]) * 25.4 / caps["logpixelsy"],
            ],
            "destination_extent_mm": [
                output_width * 25.4 / caps["logpixelsx"],
                output_height * 25.4 / caps["logpixelsy"],
            ],
        }

    @staticmethod
    def _completed_output(
        path: Path,
        *,
        timeout_seconds: float = 30.0,
        stable_seconds: float = 0.5,
    ) -> tuple[int, str]:
        deadline = time.monotonic() + timeout_seconds
        last_marker: tuple[int, int] | None = None
        stable_since: float | None = None
        while time.monotonic() < deadline:
            try:
                stat = path.stat()
            except FileNotFoundError:
                last_marker = None
                stable_since = None
                time.sleep(0.1)
                continue
            marker = (int(stat.st_size), int(stat.st_mtime_ns))
            if marker[0] > MAX_PRINT_OUTPUT_BYTES:
                raise PrinterError(f"print output exceeds the {MAX_PRINT_OUTPUT_BYTES}-byte safety bound")
            if marker[0] <= 100:
                last_marker = marker
                stable_since = None
                time.sleep(0.1)
                continue
            if marker != last_marker:
                last_marker = marker
                stable_since = time.monotonic()
            elif stable_since is not None and time.monotonic() - stable_since >= stable_seconds:
                try:
                    digest = hashlib.sha256()
                    total = 0
                    with path.open("rb") as handle:
                        while chunk := handle.read(1024 * 1024):
                            total += len(chunk)
                            if total > MAX_PRINT_OUTPUT_BYTES:
                                raise PrinterError(f"print output exceeds the {MAX_PRINT_OUTPUT_BYTES}-byte safety bound")
                            digest.update(chunk)
                    after = path.stat()
                except PrinterError:
                    raise
                except (FileNotFoundError, PermissionError, OSError):
                    last_marker = None
                    stable_since = None
                else:
                    if (total, int(after.st_mtime_ns)) == marker:
                        return total, digest.hexdigest()
                    last_marker = (int(after.st_size), int(after.st_mtime_ns))
                    stable_since = time.monotonic()
            time.sleep(0.1)
        raise PrinterError("print job completed but output file did not become stable")

    def print_png(self, path: str | os.PathLike[str], spec: PrintSpec) -> PrintReceipt:
        return self.print_image(RasterImage.from_png(path), spec)

    def print_image(self, image: RasterImage, spec: PrintSpec) -> PrintReceipt:
        output = Path(spec.output_path).expanduser().absolute() if spec.output_path is not None else None
        if output is not None:
            output.parent.mkdir(parents=True, exist_ok=True)
            if output.exists() and not spec.overwrite_output:
                raise FileExistsError(f"refusing to overwrite print output: {output}")
            if output.exists():
                output.unlink()
        default_before = _default_printer_name()
        printer = self._open()
        hdc = None
        job_started = False
        job_id = 0
        try:
            default_devmode_before = self._default_devmode(printer)
            default_hash_before = hashlib.sha256(default_devmode_before.raw).hexdigest()
            job_devmode, validated = self._job_devmode(printer, default_devmode_before, spec.paper)
            hdc = _require(_gdi32.CreateDCW("WINSPOOL", self.printer_name, None, job_devmode), "CreateDCW")
            caps = self._caps(hdc)
            if spec.paper.width_mm is not None:
                requested_page = (float(spec.paper.width_mm), float(spec.paper.height_mm))
                if spec.paper.orientation == "landscape":
                    requested_page = (requested_page[1], requested_page[0])
                actual_page = (
                    caps["physicalwidth"] * 25.4 / caps["logpixelsx"],
                    caps["physicalheight"] * 25.4 / caps["logpixelsy"],
                )
                if any(abs(actual - expected) > 0.5 for actual, expected in zip(actual_page, requested_page)):
                    raise PrinterError(
                        f"CreateDC did not honor custom paper dimensions: requested={requested_page}, actual={actual_page}"
                    )
            geometry = self._geometry(image, caps, spec)
            left, top, width, height = geometry["destination_rect_device_pixels"]
            pixels = _opaque_bgra(image)
            pixel_buffer = ctypes.create_string_buffer(pixels)
            info = bitmap_info(image.width, image.height)
            doc_info = DOCINFOW(
                ctypes.sizeof(DOCINFOW),
                str(spec.document_name)[:240],
                str(output) if output is not None else None,
                None,
                0,
            )
            job_id = int(_gdi32.StartDocW(hdc, ctypes.byref(doc_info)))
            if job_id <= 0:
                raise PrinterError(ctypes.get_last_error(), "StartDocW")
            job_started = True
            if _gdi32.StartPage(hdc) <= 0:
                raise PrinterError(ctypes.get_last_error(), "StartPage")
            copied = int(
                _gdi32.StretchDIBits(
                    hdc,
                    int(left),
                    int(top),
                    int(width),
                    int(height),
                    0,
                    0,
                    image.width,
                    image.height,
                    pixel_buffer,
                    ctypes.byref(info),
                    DIB_RGB_COLORS,
                    SRCCOPY,
                )
            )
            if copied != image.height:
                raise PrinterError(
                    ctypes.get_last_error(),
                    f"StretchDIBits copied {copied} of {image.height} scanlines",
                )
            if _gdi32.EndPage(hdc) <= 0:
                raise PrinterError(ctypes.get_last_error(), "EndPage")
            if _gdi32.EndDoc(hdc) <= 0:
                raise PrinterError(ctypes.get_last_error(), "EndDoc")
            job_started = False
            if output is not None:
                output_bytes, output_hash = self._completed_output(output)
            else:
                output_bytes = None
                output_hash = None
            default_devmode_after = self._default_devmode(printer)
            default_hash_after = hashlib.sha256(default_devmode_after.raw).hexdigest()
            default_after = _default_printer_name()
            return PrintReceipt(
                printer_name=self.printer_name,
                job_id=job_id,
                copied_scanlines=copied,
                document_name=str(spec.document_name)[:240],
                output_path=str(output) if output is not None else None,
                output_bytes=output_bytes,
                output_sha256=output_hash,
                default_printer_before=default_before,
                default_printer_after=default_after,
                default_printer_unchanged=default_before == default_after,
                default_devmode_sha256_before=default_hash_before,
                default_devmode_sha256_after=default_hash_after,
                default_devmode_unchanged=default_hash_before == default_hash_after,
                requested_paper=asdict(spec.paper),
                validated_devmode=validated,
                device_caps=caps,
                geometry=geometry,
            )
        finally:
            if job_started and hdc:
                _gdi32.AbortDoc(hdc)
            if hdc:
                _gdi32.DeleteDC(hdc)
            if printer:
                _winspool.ClosePrinter(printer)


__all__ = [
    "GdiPrinter",
    "MarginsMM",
    "PaperSpec",
    "PrinterError",
    "PrintReceipt",
    "PrintSpec",
]
