import asyncio
import json
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import aiofiles
import aiohttp
import astrbot.api.message_components as Comp
import imageio_ffmpeg
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register

NETEASE_API = "https://music.163.com/api"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
)
SONG_URL_RE = re.compile(r"(?:music\.163\.com/.*?song\?|song\?)[^\s#]*\bid=(\d+)")


class NeteaseError(Exception):
    pass


@dataclass(slots=True)
class Song:
    song_id: int
    name: str
    artists: str
    album: str
    duration_ms: int


@register(
    "netease_preview",
    "24097",
    "搜索网易云歌曲并发送数秒 WAV 语音预览",
    "1.1.0",
)
class NeteasePreview(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.session: aiohttp.ClientSession | None = None
        self.preview_slots = asyncio.Semaphore(2)
        self.account_verified = False

    async def initialize(self):
        timeout = aiohttp.ClientTimeout(
            total=self._int_config("request_timeout", 15, 5, 60)
        )
        headers = {"User-Agent": USER_AGENT, "Referer": "https://music.163.com/"}
        self.session = aiohttp.ClientSession(timeout=timeout, headers=headers)
        if self._cookie_header():
            await self._validate_account()

    async def terminate(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def _validate_account(self):
        try:
            data = await self._get_json(f"{NETEASE_API}/nuser/account/get", params={})
        except NeteaseError as exc:
            logger.warning("[netease_preview] Account verification failed: %s", exc)
            return

        profile = data.get("profile")
        if isinstance(profile, dict) and profile.get("userId"):
            self.account_verified = True
            logger.info("[netease_preview] NetEase account verified")
        else:
            logger.warning(
                "[netease_preview] MUSIC_U is invalid or expired; using guest access"
            )

    @filter.event_message_type(filter.EventMessageType.ALL, priority=1100)
    async def request_song(self, event: AstrMessageEvent):
        """搜索网易云歌曲并发送一段语音预览。"""
        query = self._request_argument(event.message_str)
        if query is None:
            return

        event.stop_event()
        if not query:
            await event.send(
                event.plain_result("用法：点歌 歌名 [歌手]\n也可以粘贴网易云歌曲链接。")
            )
            return

        output_path: Path | None = None
        try:
            async with self.preview_slots:
                song, audio_url = await self._find_playable_song(query)
                duration = self._int_config("preview_seconds", 8, 3, 15)
                start = self._preview_start(song, duration)
                output_path = await self._make_preview(audio_url, start, duration)

                minutes, seconds = divmod(song.duration_ms // 1000, 60)
                info = (
                    f"网易云：{song.name} - {song.artists}\n"
                    f"专辑：{song.album or '未知'}｜时长：{minutes}:{seconds:02d}\n"
                    f"试听片段：{start}-{start + duration} 秒"
                )
                await event.send(event.plain_result(info))
                await event.send(
                    event.chain_result([Comp.Record.fromFileSystem(str(output_path))])
                )
        except NeteaseError as exc:
            await event.send(event.plain_result(f"点歌失败：{exc}"))
        except Exception:  # noqa: BLE001 - keep command failures inside the plugin boundary
            logger.exception(
                "[netease_preview] Unexpected error while creating preview"
            )
            await event.send(
                event.plain_result("点歌失败：处理音频时发生异常，请稍后重试。")
            )
        finally:
            if output_path:
                try:
                    output_path.unlink(missing_ok=True)
                except OSError:
                    logger.warning("[netease_preview] Failed to remove %s", output_path)

    async def _find_playable_song(self, query: str) -> tuple[Song, str]:
        song_id = self._extract_song_id(query)
        if song_id is not None:
            songs = [await self._song_detail(song_id)]
        else:
            songs = await self._search(query)

        for song in songs:
            audio_url = await self._audio_url(song.song_id)
            if audio_url:
                return song, audio_url
        raise NeteaseError("没有找到当前可试听的歌曲，可能受版权或地区限制。")

    async def _search(self, keywords: str) -> list[Song]:
        limit = self._int_config("search_limit", 5, 1, 10)
        data = await self._get_json(
            f"{NETEASE_API}/search/get/web",
            params={"s": keywords, "type": 1, "offset": 0, "limit": limit},
        )
        raw_songs = self._search_items(data)
        if raw_songs is None:
            logger.warning(
                "[netease_preview] Search returned malformed result; retrying fallback"
            )
            data = await self._get_json(
                f"{NETEASE_API}/cloudsearch/pc",
                params={"s": keywords, "type": 1, "offset": 0, "limit": limit},
            )
            raw_songs = self._search_items(data)
        if raw_songs is None:
            raise NeteaseError("网易云搜索接口返回异常数据，请稍后重试。")

        songs = []
        for item in raw_songs:
            if not isinstance(item, dict):
                continue
            try:
                songs.append(self._parse_song(item))
            except (KeyError, TypeError, ValueError):
                logger.warning(
                    "[netease_preview] Ignored malformed song in search result"
                )
        if not songs:
            raise NeteaseError("没有搜索到相关歌曲。")
        return songs

    async def _song_detail(self, song_id: int) -> Song:
        data = await self._get_json(
            f"{NETEASE_API}/song/detail/",
            params={"id": song_id, "ids": json.dumps([song_id])},
        )
        songs = data.get("songs", [])
        if not isinstance(songs, list) or not songs or not isinstance(songs[0], dict):
            raise NeteaseError("歌曲链接无效或歌曲已下架。")
        try:
            return self._parse_song(songs[0])
        except (KeyError, TypeError, ValueError) as exc:
            raise NeteaseError("网易云歌曲详情数据异常，请稍后重试。") from exc

    async def _audio_url(self, song_id: int) -> str | None:
        data = await self._get_json(
            f"{NETEASE_API}/song/enhance/player/url",
            params={"id": song_id, "ids": json.dumps([song_id]), "br": 128000},
        )
        entries = data.get("data", [])
        if (
            not isinstance(entries, list)
            or not entries
            or not isinstance(entries[0], dict)
        ):
            return None
        url = entries[0].get("url")
        if not isinstance(url, str) or not url:
            return None

        parsed = urlparse(url)
        hostname = (parsed.hostname or "").lower()
        if hostname != "music.126.net" and not hostname.endswith(".music.126.net"):
            logger.warning(
                "[netease_preview] Rejected unexpected audio host: %s", hostname
            )
            return None
        return parsed._replace(scheme="https").geturl()

    async def _get_json(self, url: str, params: dict) -> dict:
        if not self.session or self.session.closed:
            await self.initialize()
        headers = {}
        if cookie := self._cookie_header():
            headers["Cookie"] = cookie
        try:
            async with self.session.get(
                url, params=params, headers=headers
            ) as response:
                if response.status != 200:
                    raise NeteaseError(f"网易云接口返回 HTTP {response.status}。")
                data = await response.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise NeteaseError("连接网易云超时或网络不可用。") from exc
        except (json.JSONDecodeError, ValueError) as exc:
            raise NeteaseError("网易云接口返回了无法解析的数据。") from exc
        if not isinstance(data, dict):
            raise NeteaseError("网易云接口返回了异常数据。")
        if data.get("code") != 200:
            raise NeteaseError(f"网易云接口返回错误码 {data.get('code', '未知')}。")
        return data

    @staticmethod
    def _search_items(data: dict) -> list | None:
        result = data.get("result")
        if isinstance(result, str):
            try:
                result = json.loads(result)
            except (json.JSONDecodeError, TypeError):
                return None
        if not isinstance(result, dict):
            return None
        songs = result.get("songs", [])
        return songs if isinstance(songs, list) else None

    async def _make_preview(self, url: str, start: int, duration: int) -> Path:
        try:
            ffmpeg = (
                str(self.config.get("ffmpeg_path", "")).strip()
                or imageio_ffmpeg.get_ffmpeg_exe()
            )
        except Exception as exc:
            raise NeteaseError(
                "找不到 ffmpeg，请检查插件依赖或配置 ffmpeg_path。"
            ) from exc

        source = await self._download_audio(url)
        descriptor, path = tempfile.mkstemp(prefix="astrbot_netease_", suffix=".wav")
        os.close(descriptor)
        output = Path(path)
        command = [
            ffmpeg,
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-ss",
            str(start),
            "-i",
            str(source),
            "-t",
            str(duration),
            "-vn",
            "-ac",
            "1",
            "-ar",
            "24000",
            "-c:a",
            "pcm_s16le",
            str(output),
        ]
        kwargs = {}
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW

        process = None
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
                **kwargs,
            )
            try:
                _, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=self._int_config("convert_timeout", 30, 10, 120),
                )
            except asyncio.TimeoutError as exc:
                raise NeteaseError("生成试听片段超时。") from exc
        except OSError as exc:
            source.unlink(missing_ok=True)
            output.unlink(missing_ok=True)
            raise NeteaseError("无法启动 ffmpeg，请检查可执行文件路径。") from exc
        except BaseException:
            if process and process.returncode is None:
                process.kill()
                await process.wait()
            source.unlink(missing_ok=True)
            output.unlink(missing_ok=True)
            raise

        source_size = source.stat().st_size if source.exists() else 0
        source.unlink(missing_ok=True)
        if (
            process.returncode != 0
            or not output.exists()
            or output.stat().st_size < 1024
        ):
            detail = stderr.decode(errors="replace").strip()[-300:] or "<empty stderr>"
            logger.warning(
                "[netease_preview] ffmpeg failed: code=%s source_bytes=%s "
                "output_bytes=%s detail=%s",
                process.returncode,
                source_size,
                output.stat().st_size if output.exists() else 0,
                detail,
            )
            output.unlink(missing_ok=True)
            raise NeteaseError("音源暂时无法读取或转换。")
        return output

    async def _download_audio(self, url: str) -> Path:
        if not self.session or self.session.closed:
            await self.initialize()

        descriptor, path = tempfile.mkstemp(prefix="astrbot_netease_", suffix=".mp3")
        os.close(descriptor)
        output = Path(path)
        max_bytes = self._int_config("max_audio_mb", 20, 5, 100) * 1024 * 1024
        downloaded = 0
        try:
            timeout = aiohttp.ClientTimeout(
                total=self._int_config("download_timeout", 60, 15, 300)
            )
            async with self.session.get(
                url, allow_redirects=True, timeout=timeout
            ) as response:
                if response.status != 200:
                    raise NeteaseError(f"网易云音源返回 HTTP {response.status}。")
                content_length = response.content_length
                if content_length and content_length > max_bytes:
                    raise NeteaseError("歌曲音源超过临时下载大小限制。")

                async with aiofiles.open(output, "wb") as file:
                    async for chunk in response.content.iter_chunked(64 * 1024):
                        downloaded += len(chunk)
                        if downloaded > max_bytes:
                            raise NeteaseError("歌曲音源超过临时下载大小限制。")
                        await file.write(chunk)
        except NeteaseError:
            output.unlink(missing_ok=True)
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
            output.unlink(missing_ok=True)
            raise NeteaseError("下载网易云音源失败。") from exc

        if downloaded < 1024:
            output.unlink(missing_ok=True)
            raise NeteaseError("网易云返回了空音源。")
        return output

    def _preview_start(self, song: Song, duration: int) -> int:
        configured = self._int_config("preview_start", 30, 0, 300)
        total_seconds = max(song.duration_ms // 1000, duration)
        return min(configured, max(total_seconds - duration, 0))

    @staticmethod
    def _parse_song(data: dict) -> Song:
        artists = data.get("artists") or data.get("ar") or []
        album = data.get("album") or data.get("al") or {}
        return Song(
            song_id=int(data["id"]),
            name=str(data.get("name") or "未知歌曲"),
            artists=" / ".join(str(item.get("name", "")) for item in artists)
            or "未知歌手",
            album=str(album.get("name") or ""),
            duration_ms=int(data.get("duration") or data.get("dt") or 0),
        )

    @staticmethod
    def _extract_song_id(value: str) -> int | None:
        if value.strip().isdigit():
            return int(value.strip())
        match = SONG_URL_RE.search(value)
        if match:
            return int(match.group(1))
        parsed = urlparse(value.strip())
        if parsed.hostname and parsed.hostname.endswith("music.163.com"):
            ids = parse_qs(parsed.query).get("id", [])
            if ids and ids[0].isdigit():
                return int(ids[0])
        return None

    @staticmethod
    def _request_argument(message: str) -> str | None:
        stripped = message.strip()
        for command in ("网易云点歌", "music", "点歌"):
            if stripped == command:
                return ""
            if stripped.startswith(command) and stripped[len(command)].isspace():
                return stripped[len(command) :].strip()
        return None

    def _int_config(self, key: str, default: int, minimum: int, maximum: int) -> int:
        try:
            value = int(self.config.get(key, default))
        except (TypeError, ValueError):
            value = default
        return max(minimum, min(value, maximum))

    def _cookie_header(self) -> str:
        token = str(self.config.get("music_u", "")).strip()
        if token.startswith("MUSIC_U="):
            token = token.removeprefix("MUSIC_U=").split(";", 1)[0].strip()
        if not token:
            return ""
        if "\r" in token or "\n" in token or ";" in token:
            logger.warning("[netease_preview] Ignored invalid MUSIC_U value")
            return ""
        return f"MUSIC_U={token}; os=pc; appver=9.2.10"
