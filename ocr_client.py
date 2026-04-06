from __future__ import annotations

import hashlib
import json
import os
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

import aiohttp
from PIL import Image


OCR_SPACE_ENDPOINT = "https://api.ocr.space/parse/image"


class OcrError(RuntimeError):
    pass


class OcrSpaceClient:
    def __init__(
        self,
        api_key: str,
        *,
        cache_path: Optional[str] = None,
        request_timeout_seconds: int = 30,
        use_system_proxy: bool = False,
    ) -> None:
        self.api_key = (api_key or "").strip()
        self.request_timeout_seconds = max(int(request_timeout_seconds), 5)
        self.use_system_proxy = bool(use_system_proxy)
        self._memory_cache: dict[str, str] = {}
        self._cache_path = self._resolve_cache_path(cache_path)
        self._cache_dirty = False
        self._cache_loaded = False

    @staticmethod
    def _resolve_cache_path(cache_path: Optional[str]) -> Optional[Path]:
        raw = (cache_path or os.getenv("OCR_CACHE_PATH", "")).strip()
        if not raw:
            return None
        path = Path(raw)
        if not path.is_absolute():
            path = Path(__file__).resolve().parent / path
        return path

    def _load_cache(self) -> None:
        if self._cache_loaded:
            return
        self._cache_loaded = True
        if self._cache_path is None or not self._cache_path.exists():
            return
        try:
            payload = json.loads(self._cache_path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        for key, value in payload.items():
            if not isinstance(key, str) or not isinstance(value, str):
                continue
            self._memory_cache[key] = value

    def _save_cache(self) -> None:
        if not self._cache_dirty or self._cache_path is None:
            return
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(sorted(self._memory_cache.items(), key=lambda item: item[0]))
        self._cache_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self._cache_dirty = False

    def _cache_get(self, key: str) -> Optional[str]:
        self._load_cache()
        return self._memory_cache.get(key)

    def _cache_set(self, key: str, value: str) -> None:
        self._load_cache()
        self._memory_cache[key] = value
        self._cache_dirty = True
        self._save_cache()

    @staticmethod
    def _hash_bytes(image_bytes: bytes) -> str:
        return hashlib.sha256(image_bytes).hexdigest()

    @staticmethod
    def _ocr_filetype_from_content_type(content_type: str, image_bytes: bytes) -> Optional[str]:
        normalized = (content_type or "").lower().split(";")[0].strip()
        mapping = {
            "image/png": "PNG",
            "image/jpeg": "JPG",
            "image/jpg": "JPG",
            "image/gif": "GIF",
            "image/tiff": "TIF",
            "image/bmp": "BMP",
            "image/webp": "PNG",  # WebP в параметре filetype не задокументирован; шлем как PNG fallback-path.
        }
        if normalized in mapping:
            return mapping[normalized]

        # Magic bytes fallback
        if image_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
            return "PNG"
        if image_bytes.startswith(b"\xff\xd8\xff"):
            return "JPG"
        if image_bytes.startswith((b"GIF87a", b"GIF89a")):
            return "GIF"
        if image_bytes.startswith(b"BM"):
            return "BMP"
        if image_bytes.startswith((b"II*\x00", b"MM\x00*")):
            return "TIF"
        return None

    @staticmethod
    def _should_try_reencode_fallback(content_type: str, image_bytes: bytes) -> bool:
        normalized_type = (content_type or "").lower().split(";")[0].strip()
        if normalized_type in {"image/png", "image/webp", "image/gif", "image/bmp", "image/tiff"}:
            return True
        if image_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
            return True
        if image_bytes.startswith(b"RIFF") and b"WEBP" in image_bytes[:16]:
            return True
        return False

    @staticmethod
    def _reencode_to_png_bytes(image_bytes: bytes) -> bytes:
        try:
            with Image.open(BytesIO(image_bytes)) as image:
                # Если есть альфа-канал, прижимаем к белому фону,
                # чтобы не терять читаемость текста при OCR.
                if image.mode in {"RGBA", "LA"} or (
                    image.mode == "P" and "transparency" in image.info
                ):
                    rgba = image.convert("RGBA")
                    background = Image.new("RGBA", rgba.size, (255, 255, 255, 255))
                    merged = Image.alpha_composite(background, rgba).convert("RGB")
                else:
                    merged = image.convert("RGB")
                buffer = BytesIO()
                merged.save(buffer, format="PNG", optimize=True)
                return buffer.getvalue()
        except Exception as exc:  # noqa: BLE001
            raise OcrError(f"Не удалось перекодировать изображение в PNG: {exc}") from exc

    @staticmethod
    def _reencode_to_jpeg_bytes(image_bytes: bytes) -> bytes:
        try:
            with Image.open(BytesIO(image_bytes)) as image:
                rgb = image.convert("RGB")
                buffer = BytesIO()
                rgb.save(buffer, format="JPEG", quality=95)
                return buffer.getvalue()
        except Exception as exc:  # noqa: BLE001
            raise OcrError(f"Не удалось перекодировать изображение в JPEG: {exc}") from exc

    @staticmethod
    def _extract_text(response_payload: Any) -> str:
        if not isinstance(response_payload, dict):
            raise OcrError("OCR.space вернул неожиданный формат ответа")

        if response_payload.get("IsErroredOnProcessing"):
            error_chunks: list[str] = []
            error_message = response_payload.get("ErrorMessage")
            if isinstance(error_message, list):
                error_chunks.extend(str(chunk) for chunk in error_message if str(chunk).strip())
            elif error_message is not None:
                error_chunks.append(str(error_message))
            error_details = response_payload.get("ErrorDetails")
            if error_details:
                error_chunks.append(str(error_details))
            joined = " | ".join(chunk.strip() for chunk in error_chunks if chunk and str(chunk).strip())
            raise OcrError(joined or "OCR.space вернул ошибку распознавания")

        parsed_results = response_payload.get("ParsedResults")
        if not isinstance(parsed_results, list) or not parsed_results:
            raise OcrError("OCR.space не вернул ParsedResults")

        parts: list[str] = []
        for item in parsed_results:
            if not isinstance(item, dict):
                continue
            parsed_text = item.get("ParsedText")
            if isinstance(parsed_text, str) and parsed_text.strip():
                parts.append(parsed_text.strip())

        text = "\n".join(parts).strip()
        if not text:
            raise OcrError("OCR.space не распознал текст на изображении")
        return text

    async def _post_form(self, form: aiohttp.FormData) -> str:
        if not self.api_key:
            raise OcrError("Не задан OCR_SPACE_API_KEY")

        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=self.use_system_proxy) as session:
            try:
                async with session.post(OCR_SPACE_ENDPOINT, data=form) as response:
                    status_code = response.status
                    payload = await response.json(content_type=None)
            except Exception as exc:  # noqa: BLE001
                raise OcrError(f"Ошибка запроса к OCR.space: {exc}") from exc

        if status_code >= 400:
            raise OcrError(f"OCR.space вернул HTTP {status_code}")
        return self._extract_text(payload)

    async def _download_image_url_bytes(self, image_url: str) -> tuple[bytes, str]:
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=self.use_system_proxy) as session:
            try:
                async with session.get(image_url, allow_redirects=True) as response:
                    if response.status >= 400:
                        raise OcrError(f"Не удалось скачать изображение (HTTP {response.status})")
                    image_bytes = await response.read()
                    if not image_bytes:
                        raise OcrError("Не удалось скачать изображение: пустой ответ")
                    content_type = (response.headers.get("Content-Type", "image/jpeg") or "").strip()
                    normalized_content_type = content_type.split(";")[0].strip().lower() or "image/jpeg"
                    return image_bytes, normalized_content_type
            except OcrError:
                raise
            except Exception as exc:  # noqa: BLE001
                raise OcrError(f"Не удалось скачать изображение по URL: {exc}") from exc

    async def recognize_text_from_image_bytes(
        self,
        image_bytes: bytes,
        content_type: str = "image/jpeg",
        *,
        cache_key: Optional[str] = None,
    ) -> str:
        if not image_bytes:
            raise OcrError("Пустые данные изображения")

        cache_id = cache_key or f"sha256:{self._hash_bytes(image_bytes)}"
        cached = self._cache_get(cache_id)
        if cached is not None:
            return cached

        form = aiohttp.FormData()
        form.add_field("apikey", self.api_key)
        form.add_field("language", "rus")
        form.add_field("scale", "true")
        form.add_field("OCREngine", "2")
        form.add_field("isOverlayRequired", "false")
        filetype = self._ocr_filetype_from_content_type(content_type, image_bytes)
        if filetype:
            form.add_field("filetype", filetype)
        form.add_field(
            "file",
            image_bytes,
            filename="bet-image.jpg",
            content_type=content_type or "image/jpeg",
        )

        try:
            text = await self._post_form(form)
        except OcrError as initial_exc:
            if not self._should_try_reencode_fallback(content_type, image_bytes):
                raise

            # Fallback 1: lossless PNG (предпочтительно для OCR текста).
            try:
                png_bytes = self._reencode_to_png_bytes(image_bytes)
                fallback_form_png = aiohttp.FormData()
                fallback_form_png.add_field("apikey", self.api_key)
                fallback_form_png.add_field("language", "rus")
                fallback_form_png.add_field("scale", "true")
                fallback_form_png.add_field("OCREngine", "2")
                fallback_form_png.add_field("isOverlayRequired", "false")
                fallback_form_png.add_field("filetype", "PNG")
                fallback_form_png.add_field(
                    "file",
                    png_bytes,
                    filename="bet-image-fallback.png",
                    content_type="image/png",
                )
                text = await self._post_form(fallback_form_png)
            except OcrError:
                # Fallback 2: JPEG как последняя попытка.
                jpeg_bytes = self._reencode_to_jpeg_bytes(image_bytes)
                fallback_form_jpeg = aiohttp.FormData()
                fallback_form_jpeg.add_field("apikey", self.api_key)
                fallback_form_jpeg.add_field("language", "rus")
                fallback_form_jpeg.add_field("scale", "true")
                fallback_form_jpeg.add_field("OCREngine", "2")
                fallback_form_jpeg.add_field("isOverlayRequired", "false")
                fallback_form_jpeg.add_field("filetype", "JPG")
                fallback_form_jpeg.add_field(
                    "file",
                    jpeg_bytes,
                    filename="bet-image-fallback.jpg",
                    content_type="image/jpeg",
                )
                try:
                    text = await self._post_form(fallback_form_jpeg)
                except OcrError as jpeg_exc:
                    raise OcrError(
                        f"OCR не распознал изображение после fallback (original/png/jpeg): {jpeg_exc}"
                    ) from initial_exc

        self._cache_set(cache_id, text)
        return text

    async def recognize_text_from_image_url(self, image_url: str) -> str:
        normalized_url = (image_url or "").strip()
        if not normalized_url:
            raise OcrError("Пустой URL изображения")

        cache_id = f"url:{normalized_url}"
        cached = self._cache_get(cache_id)
        if cached is not None:
            return cached

        # Для ссылок с антибот-защитой (например, CDN/hotlink) OCR.space не всегда может
        # скачать картинку сам по URL. Скачиваем картинку локально и отправляем как file.
        try:
            image_bytes, content_type = await self._download_image_url_bytes(normalized_url)
            return await self.recognize_text_from_image_bytes(
                image_bytes,
                content_type=content_type,
                cache_key=cache_id,
            )
        except OcrError:
            # Fallback на native URL режим OCR.space.
            form = aiohttp.FormData()
            form.add_field("apikey", self.api_key)
            form.add_field("language", "rus")
            form.add_field("scale", "true")
            form.add_field("OCREngine", "2")
            form.add_field("isOverlayRequired", "false")
            form.add_field("url", normalized_url)

            text = await self._post_form(form)
            self._cache_set(cache_id, text)
            return text


_DEFAULT_CLIENT: Optional[OcrSpaceClient] = None


def get_default_client() -> OcrSpaceClient:
    global _DEFAULT_CLIENT
    if _DEFAULT_CLIENT is None:
        _DEFAULT_CLIENT = OcrSpaceClient(
            api_key=os.getenv("OCR_SPACE_API_KEY", ""),
            cache_path=os.getenv("OCR_CACHE_PATH", "./ocr_cache.json"),
            request_timeout_seconds=int(os.getenv("OCR_SPACE_TIMEOUT_SECONDS", "30") or "30"),
            use_system_proxy=os.getenv("OCR_USE_SYSTEM_PROXY", "0").strip().lower() in {"1", "true", "yes", "on"},
        )
    return _DEFAULT_CLIENT


async def recognize_text_from_image_bytes(image_bytes: bytes, content_type: str = "image/jpeg") -> str:
    return await get_default_client().recognize_text_from_image_bytes(image_bytes, content_type)


async def recognize_text_from_image_url(image_url: str) -> str:
    return await get_default_client().recognize_text_from_image_url(image_url)
