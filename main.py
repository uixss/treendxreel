#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import threading
import subprocess
import hashlib
import random
import sqlite3
import re
import warnings
import asyncio
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from PIL import Image, ImageTk, ImageDraw, ImageFilter
from collections import deque
import imageio
import requests
import customtkinter as ctk
from tkinter import filedialog, messagebox, TclError
from instagrapi import Client
from TikTokApi import TikTokApi

warnings.filterwarnings("ignore", category=DeprecationWarning, module="sqlite3")

CONFIG = {
    "INSTAGRAM_USERNAME": "tu_usuario",
    "INSTAGRAM_PASSWORD": "tu_contraseña",
    "TIKTOK_COOKIES": {"s_v_web_id": "", "ttwid": ""},
    "MODE": "tiktok",
    "TIKTOK_LANGUAGE": "es",
    "TIKTOK_TRENDING_COUNT": 10,
    "LOCAL_VIDEO_PATH": "video.mp4",
    "DOWNLOAD_FOLDER": "downloads",
    "OUTPUT_FOLDER": "processed",
    "DATA_FOLDER": "data",
    "POST_CAPTION_TEMPLATE": "| {desc}",
    "POST_HASHTAGS": "#reels #viral #trending",
    "DISABLE_LIKE_COUNTS": True,
    "DISABLE_COMMENTS": True,
    "ENABLE_ALT_TEXT": True,
    "ALT_TEXT": "Contenido automático",
    "LOOP_ENABLED": False,
    "LOOP_DELAY_SECONDS": 1800,
    "MAX_RETRIES": 3,
    "RETRY_BACKOFF_BASE": 2,
    "RETRY_BACKOFF_MULTIPLIER": 1,
    "RANDOM_JITTER_PERCENT": 10,
    "CLEANUP_AFTER_UPLOAD": True,
    "TARGET_WIDTH": 720,
    "TARGET_HEIGHT": 1280,
    "VIDEO_BITRATE": "2500k",
    "ENHANCE_QUALITY": True,
    "WATERMARK_ENABLED": True,
    "WATERMARK_PATH": "",
    "WATERMARK_X": 30,
    "WATERMARK_Y": 30,
    "WATERMARK_OPACITY": 0.7,
    "DUPLICATE_WINDOW_HOURS": 72,
    "MAX_HISTORY_ITEMS": 5000,
}

TARGET_W = CONFIG["TARGET_WIDTH"]
TARGET_H = CONFIG["TARGET_HEIGHT"]

PREVIEW_SCALE = 0.45
GUI_W = int(TARGET_W * PREVIEW_SCALE)
GUI_H = int(TARGET_H * PREVIEW_SCALE)


class DataManager:
    def __init__(self, data_folder):
        self.data_folder = Path(data_folder)
        self.data_folder.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_folder / "history.db"
        self._init_db()
        self.processed_hashes = deque(maxlen=CONFIG["MAX_HISTORY_ITEMS"])
        self._load_recent_hashes()

    def _init_db(self):
        conn = sqlite3.connect(str(self.db_path))
        conn.text_factory = str
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS processed (
            id TEXT PRIMARY KEY, video_hash TEXT, source TEXT, caption TEXT,
            posted_at TEXT, status TEXT, error_msg TEXT)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_hash ON processed(video_hash)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_posted ON processed(posted_at)")
        conn.commit()
        conn.close()

    def _load_recent_hashes(self):
        conn = sqlite3.connect(str(self.db_path))
        conn.text_factory = str
        c = conn.cursor()
        cutoff = (datetime.now() - timedelta(
            hours=CONFIG["DUPLICATE_WINDOW_HOURS"])).isoformat()
        c.execute(
            'SELECT video_hash FROM processed '
            'WHERE posted_at >= ? AND status = "success"', (cutoff,))
        for row in c.fetchall():
            self.processed_hashes.append(row[0])
        conn.close()

    def _calculate_hash(self, filepath):
        h = hashlib.sha256()
        try:
            with open(filepath, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    h.update(chunk)
            return h.hexdigest()
        except Exception:
            return None

    def is_duplicate(self, video_id=None, filepath=None):
        if filepath:
            file_hash = self._calculate_hash(filepath)
            if file_hash and file_hash in self.processed_hashes:
                return True
        if video_id:
            conn = sqlite3.connect(str(self.db_path))
            conn.text_factory = str
            c = conn.cursor()
            cutoff = (datetime.now() - timedelta(
                hours=CONFIG["DUPLICATE_WINDOW_HOURS"])).isoformat()
            c.execute("SELECT id FROM processed WHERE id = ? AND posted_at >= ?",
                      (video_id, cutoff))
            result = c.fetchone()
            conn.close()
            if result:
                return True
        return False

    def register_success(self, video_id, filepath, source, caption):
        video_hash = self._calculate_hash(filepath) if filepath else None
        conn = sqlite3.connect(str(self.db_path))
        conn.text_factory = str
        c = conn.cursor()
        c.execute("""INSERT OR REPLACE INTO processed
            (id, video_hash, source, caption, posted_at, status, error_msg)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (video_id, video_hash, source, caption,
             datetime.now().isoformat(), "success", None))
        conn.commit()
        conn.close()
        if video_hash:
            self.processed_hashes.append(video_hash)

    def register_error(self, video_id, source, error_msg):
        conn = sqlite3.connect(str(self.db_path))
        conn.text_factory = str
        c = conn.cursor()
        c.execute("""INSERT OR REPLACE INTO processed
            (id, video_hash, source, caption, posted_at, status, error_msg)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (video_id, None, source, None,
             datetime.now().isoformat(), "error", str(error_msg)[:500]))
        conn.commit()
        conn.close()


def _ensure_directory(path):
    Path(path).mkdir(parents=True, exist_ok=True)


def _retry_operation(func, max_retries=None, *args, **kwargs):
    max_retries = max_retries or CONFIG["MAX_RETRIES"]
    last_exception = None
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            wait_time = ((CONFIG["RETRY_BACKOFF_BASE"] ** attempt)
                         * CONFIG["RETRY_BACKOFF_MULTIPLIER"])
            time.sleep(wait_time)
    raise last_exception


def _build_caption(tiktok_desc=None):
    template = CONFIG["POST_CAPTION_TEMPLATE"]
    base = (template.format(desc=tiktok_desc)
            if tiktok_desc and "{desc}" in template else template)
    return f"{base} {CONFIG['POST_HASHTAGS']}".strip()


def _get_upload_extra_data():
    data = {
        "like_and_view_counts_disabled": 1 if CONFIG["DISABLE_LIKE_COUNTS"] else 0,
        "disable_comments": 1 if CONFIG["DISABLE_COMMENTS"] else 0,
    }
    if CONFIG["ENABLE_ALT_TEXT"]:
        data["custom_accessibility_caption"] = CONFIG["ALT_TEXT"]
    return data


def _ensure_ffmpeg():
    try:
        imageio.plugins.ffmpeg.get_exe()
    except imageio.core.NeedDownloadError:
        imageio.plugins.ffmpeg.download()


def _instagram_login():
    def _login():
        cl = Client()
        session_path = Path(CONFIG["DATA_FOLDER"]) / "ig_session.json"
        if session_path.exists():
            try:
                cl.load_settings(str(session_path))
                cl.login(CONFIG["INSTAGRAM_USERNAME"], CONFIG["INSTAGRAM_PASSWORD"])
            except Exception:
                cl.login(CONFIG["INSTAGRAM_USERNAME"], CONFIG["INSTAGRAM_PASSWORD"])
        else:
            cl.login(CONFIG["INSTAGRAM_USERNAME"], CONFIG["INSTAGRAM_PASSWORD"])
        cl.dump_settings(str(session_path))
        return cl
    return _retry_operation(_login)


def _upload_reel(client, filepath, caption):
    def _upload():
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Not found: {filepath}")
        media = client.clip_upload(filepath, caption,
                                   extra_data=_get_upload_extra_data())
        return media.dict() if hasattr(media, "dict") else {}
    return _retry_operation(_upload)


# ─── NUEVAS FUNCIONES USANDO TikTokApi ───────────────────────────────
def _fetch_trending_tiktok():
    async def _fetch():
        async with TikTokApi() as api:
            await api.create_sessions(ms_tokens=[], num_sessions=1, sleep_after=3)
            videos = []
            async for video in api.trending.videos(count=CONFIG["TIKTOK_TRENDING_COUNT"]):
                videos.append(video)
            return videos
    try:
        return asyncio.run(_fetch())
    except Exception as e:
        raise RuntimeError(f"Error fetching trending videos: {e}") from e


def _download_tiktok_video(video, output_dir):
    video_id = video.id
    create_time = getattr(video, 'create_time', int(time.time()))
    filename = f"{create_time}_{video_id}.mp4"
    filepath = os.path.join(output_dir, filename)

    async def _get_url():
        return await video.video.url()

    try:
        video_url = asyncio.run(_get_url())
        _ensure_directory(output_dir)
        urllib.request.urlretrieve(video_url, filepath)
        if not os.path.exists(filepath):
            raise RuntimeError("Download failed - file not created")
        return filepath
    except Exception as e:
        raise RuntimeError(f"Error downloading video: {e}") from e


def _process_video_ffmpeg(input_path, output_path, watermark_path=None,
                          wx=0, wy=0, opacity=0.7, enhance=True, bitrate="2500k"):
    filters = [
        f"[0:v]scale={TARGET_W}:{TARGET_H}:"
        f"force_original_aspect_ratio=decrease,"
        f"pad={TARGET_W}:{TARGET_H}:(ow-iw)/2:(oh-ih)/2:black"
    ]
    if enhance:
        filters[0] += ",unsharp=5:5:1.0:5:5:0.5"

    has_wm = watermark_path and os.path.exists(watermark_path)
    if has_wm:
        filters.append(
            f"[1:v]format=rgba,"
            f"geq=r='r(X,Y)*{opacity}':g='g(X,Y)*{opacity}':"
            f"b='b(X,Y)*{opacity}':a='a(X,Y)*{opacity}'[wm]")
        filters.append(f"[0:v][wm]overlay={wx}:{wy}")

    cmd = ["ffmpeg", "-y", "-v", "error", "-i", str(input_path)]
    if has_wm:
        cmd.extend(["-i", watermark_path])
    cmd.extend([
        "-filter_complex", ";".join(filters) if has_wm else filters[0],
        "-c:v", "libx264", "-preset", "medium", "-b:v", bitrate,
        "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart",
        str(output_path),
    ])

    si = subprocess.STARTUPINFO()
    si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    result = subprocess.run(cmd, startupinfo=si, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"FFmpeg error: {result.stderr}")
    return output_path


def _process_tiktok_mode(ig_client, data_mgr, watermark_path=None,
                         wx=0, wy=0, opacity=0.7, enhance=True, bitrate="2500k"):
    trending_videos = _fetch_trending_tiktok()
    if not trending_videos:
        return False

    # Filtrar duplicados por ID
    available = [v for v in trending_videos if not data_mgr.is_duplicate(video_id=v.id)]
    if not available:
        return False

    selected = random.choice(available)
    video_id = selected.id
    caption = _build_caption(selected.desc)

    raw_path = _download_tiktok_video(selected, CONFIG["DOWNLOAD_FOLDER"])

    if data_mgr.is_duplicate(filepath=raw_path):
        if CONFIG["CLEANUP_AFTER_UPLOAD"] and os.path.exists(raw_path):
            os.remove(raw_path)
        return False

    _ensure_directory(CONFIG["OUTPUT_FOLDER"])
    output_path = Path(CONFIG["OUTPUT_FOLDER"]) / f"processed_{Path(raw_path).name}"
    _process_video_ffmpeg(raw_path, output_path, watermark_path,
                          wx, wy, opacity, enhance, bitrate)
    _upload_reel(ig_client, str(output_path), caption)
    data_mgr.register_success(video_id, str(output_path), "tiktok", caption)

    if CONFIG["CLEANUP_AFTER_UPLOAD"]:
        for p in [raw_path, output_path]:
            if os.path.exists(str(p)):
                os.remove(str(p))
    return True


def _process_local_mode(ig_client, data_mgr, watermark_path=None,
                        wx=0, wy=0, opacity=0.7, enhance=True, bitrate="2500k"):
    input_path = CONFIG["LOCAL_VIDEO_PATH"]
    if data_mgr.is_duplicate(filepath=input_path):
        return False

    caption = _build_caption()
    _ensure_directory(CONFIG["OUTPUT_FOLDER"])
    output_path = Path(CONFIG["OUTPUT_FOLDER"]) / f"processed_{Path(input_path).name}"
    _process_video_ffmpeg(input_path, output_path, watermark_path,
                          wx, wy, opacity, enhance, bitrate)
    _upload_reel(ig_client, str(output_path), caption)

    vid = hashlib.md5(
        f"{input_path}{os.path.getmtime(input_path)}".encode()).hexdigest()
    data_mgr.register_success(vid, str(output_path), "local", caption)

    if CONFIG["CLEANUP_AFTER_UPLOAD"] and output_path.exists():
        os.remove(output_path)
    return True


def _calc_watermark_position(code, logo_w, logo_h,
                             target_w=TARGET_W, target_h=TARGET_H, pad=30):
    x = pad if "L" in code else (target_w - logo_w - pad if "R" in code
                                  else (target_w - logo_w) // 2)
    y = pad if "T" in code else (target_h - logo_h - pad if "B" in code
                                  else (target_h - logo_h) // 2)
    return x, y


def _check_resources():
    try:
        import psutil
        if psutil.cpu_percent(interval=0.5) > 95:
            return False
        if psutil.virtual_memory().percent > 90:
            return False
        if psutil.disk_usage(CONFIG.get("OUTPUT_FOLDER", ".")).percent > 95:
            return False
    except ImportError:
        pass
    return True


class ModernCard(ctk.CTkFrame):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        self.configure(corner_radius=12, border_width=1, border_color="#2D3748")


class BotGUI(ctk.CTk):

    HDR_H       = 52
    ROW_H       = 34
    BTN_H       = 38
    FONT_TITLE  = 15
    FONT_LABEL  = 11
    FONT_VALUE  = 10
    FONT_SMALL  = 9
    PAD_SECTION = 6
    PAD_INNER   = 12

    def __init__(self):
        super().__init__()
        self.title("🎬 Instagram Reels Bot Pro")
        self.geometry("1500x920")
        self.minsize(1280, 780)

        self.running = False
        self.worker_thread = None
        self.logo_path = CONFIG["WATERMARK_PATH"]
        self.logo_dims = (150, 150)
        self.preview_img = None

        self.pos_x   = ctk.IntVar(value=CONFIG["WATERMARK_X"])
        self.pos_y   = ctk.IntVar(value=CONFIG["WATERMARK_Y"])
        self.opacity  = ctk.DoubleVar(value=CONFIG["WATERMARK_OPACITY"])
        self.mode_var = ctk.StringVar(value=CONFIG["MODE"])

        self.iteration_count = 0
        self.success_count   = 0
        self.error_count     = 0

        self.pos_x.trace_add("write",   lambda *_: self._update_preview())
        self.pos_y.trace_add("write",   lambda *_: self._update_preview())
        self.opacity.trace_add("write", lambda *_: self._update_preview())

        self.data_mgr = DataManager(CONFIG["DATA_FOLDER"])

        self._setup_theme()
        self._create_layout()
        self._update_preview()

    def _setup_theme(self):
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")
        self.c = {
            "bg1": "#0B0E14",  "bg2": "#14181F",  "bg3": "#1E242C",
            "card": "#1A1F27", "card_h": "#252B35",
            "blue": "#3B82F6", "green": "#10B981", "yellow": "#F59E0B",
            "red": "#EF4444",  "purple": "#8B5CF6",
            "t1": "#FFFFFF",   "t2": "#94A3B8",    "t3": "#64748B",
            "border": "#2D3748", "border_l": "#3A4458",
        }
        self.configure(fg_color=self.c["bg1"])

    def _create_layout(self):
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=58)
        self.grid_columnconfigure(1, weight=42)

        self._create_config_panel().grid(
            row=0, column=0, sticky="nsew", padx=(14, 7), pady=14)
        self._create_preview_panel().grid(
            row=0, column=1, sticky="nsew", padx=(7, 14), pady=14)

    def _create_config_panel(self):
        panel = ModernCard(self, fg_color=self.c["bg2"])

        hdr = ctk.CTkFrame(panel, fg_color=self.c["bg3"], height=self.HDR_H,
                           corner_radius=0)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)

        hi = ctk.CTkFrame(hdr, fg_color="transparent")
        hi.pack(expand=True, fill="both", padx=18, pady=10)

        ctk.CTkLabel(hi, text="PRO",
                     font=ctk.CTkFont(size=11, weight="bold"),
                     text_color=self.c["purple"], fg_color=self.c["card"],
                     corner_radius=8, padx=10, pady=3).pack(side="left", padx=12)

        self.status_indicator = ctk.CTkLabel(
            hi, text="● Listo",
            font=ctk.CTkFont(size=11, weight="bold"),
            text_color=self.c["green"])
        self.status_indicator.pack(side="right", padx=12)

        scroll = ctk.CTkScrollableFrame(
            panel, fg_color="transparent",
            scrollbar_button_color=self.c["blue"],
            scrollbar_button_hover_color=self.c["blue"])
        scroll.pack(fill="both", expand=True, padx=12, pady=(10, 6))
        scroll.grid_columnconfigure(0, weight=1, uniform="cfg")
        scroll.grid_columnconfigure(1, weight=1, uniform="cfg")

        sections = [
            ("",    "🎯", self._fill_mode,    0, 0, 1),
            ("",  "📱", self._fill_content, 0, 1, 1),
            ("",     "🎬", self._fill_proc,    1, 0, 1),
            ("",       "📤", self._fill_post,    1, 1, 1),
        ]
        for title, icon, fn, r, c, cs in sections:
            s = self._section(scroll, title, icon, fn)
            s.grid(row=r, column=c, columnspan=cs, sticky="nsew",
                   padx=self.PAD_SECTION, pady=self.PAD_SECTION)

        bar = ctk.CTkFrame(panel, fg_color=self.c["bg3"], height=56,
                           corner_radius=0)
        bar.pack(fill="x", side="bottom")
        bar.pack_propagate(False)
        self._create_buttons(bar)

        return panel

    def _section(self, parent, title, icon, fill_fn):
        sec = ModernCard(parent, fg_color=self.c["card"])

        h = ctk.CTkFrame(sec, fg_color="transparent", height=32)
        h.pack(fill="x", padx=self.PAD_INNER, pady=(8, 4))
        h.pack_propagate(False)

        ctk.CTkLabel(h, text=icon,
                     font=ctk.CTkFont(size=14),
                     text_color=self.c["blue"]).pack(side="left", padx=(0, 8))
        ctk.CTkLabel(h, text=title,
                     font=ctk.CTkFont(size=self.FONT_TITLE - 2, weight="bold"),
                     text_color=self.c["t1"]).pack(side="left")

        ctk.CTkFrame(sec, fg_color=self.c["border"], height=1).pack(
            fill="x", padx=self.PAD_INNER, pady=(0, 6))

        content = ctk.CTkFrame(sec, fg_color="transparent")
        content.pack(fill="x", padx=self.PAD_INNER, pady=(0, 10))
        fill_fn(content)
        return sec

    def _row(self, parent, label, icon=None):
        r = ctk.CTkFrame(parent, fg_color="transparent", height=self.ROW_H)
        r.pack(fill="x", pady=2)
        r.pack_propagate(False)

        if icon:
            ctk.CTkLabel(r, text=icon,
                         font=ctk.CTkFont(size=self.FONT_LABEL),
                         text_color=self.c["blue"], width=22).pack(side="left")
        ctk.CTkLabel(r, text=label,
                     font=ctk.CTkFont(size=self.FONT_LABEL),
                     text_color=self.c["t2"]).pack(side="left", padx=(4, 12))

        right = ctk.CTkFrame(r, fg_color="transparent")
        right.pack(side="right", fill="x", expand=True)
        return right

    def _fill_mode(self, p):
        r = self._row(p, "", "⚡")
        ctk.CTkSegmentedButton(
            r, values=["tiktok", "local", "both"], variable=self.mode_var,
            command=lambda v: CONFIG.update({"MODE": v}),
            font=ctk.CTkFont(size=self.FONT_VALUE), height=28
        ).pack(fill="x")

        for lbl, key, ico in [
            ("Bucle",   "LOOP_ENABLED",         "🔄"),
            ("Limpiar", "CLEANUP_AFTER_UPLOAD",  "🧹"),
            ("Mejorar", "ENHANCE_QUALITY",       "✨"),
        ]:
            r = self._row(p, lbl, ico)
            v = ctk.BooleanVar(value=CONFIG.get(key, False))
            ctk.CTkSwitch(r, text="", variable=v, width=42,
                          command=lambda k=key, vv=v: CONFIG.update({k: vv.get()}),
                          progress_color=self.c["green"]).pack(side="right")

    def _fill_content(self, p):
        r = self._row(p, "Videos trending:", "📊")
        cv = ctk.StringVar(value=str(CONFIG["TIKTOK_TRENDING_COUNT"]))
        e = ctk.CTkEntry(r, width=60, height=28,
                         font=ctk.CTkFont(size=self.FONT_VALUE),
                         textvariable=cv, fg_color=self.c["bg3"])
        e.pack(side="right")
        e.bind("<FocusOut>", lambda _: CONFIG.update(
            {"TIKTOK_TRENDING_COUNT": int(cv.get() or "10")}))

        r = self._row(p, "Idioma:", "🌐")
        lv = ctk.StringVar(value=CONFIG["TIKTOK_LANGUAGE"])
        ctk.CTkOptionMenu(
            r, values=["es", "en", "pt", "fr"], variable=lv,
            command=lambda v: CONFIG.update({"TIKTOK_LANGUAGE": v}),
            fg_color=self.c["bg3"], button_color=self.c["blue"],
            font=ctk.CTkFont(size=self.FONT_VALUE), height=28
        ).pack(side="right")

    def _fill_proc(self, p):
        r = self._row(p, "Resolución:", "📐")
        ctk.CTkLabel(r, text=f"{TARGET_W} × {TARGET_H}",
                     font=ctk.CTkFont(size=self.FONT_VALUE, weight="bold"),
                     text_color=self.c["blue"]).pack(side="right")

        r = self._row(p, "Bitrate:", "⚡")
        bv = ctk.StringVar(value=CONFIG["VIDEO_BITRATE"])
        ctk.CTkOptionMenu(
            r, values=["1500k", "2000k", "2500k", "3000k"], variable=bv,
            command=lambda v: CONFIG.update({"VIDEO_BITRATE": v}),
            fg_color=self.c["bg3"],
            font=ctk.CTkFont(size=self.FONT_VALUE), height=28
        ).pack(side="right")

        for lbl, key, ico in [
            ("Ocultar likes",    "DISABLE_LIKE_COUNTS", "❤️"),
            ("Deshab. comments", "DISABLE_COMMENTS",    "💬"),
            ("Texto alt.",       "ENABLE_ALT_TEXT",      "♿"),
        ]:
            r = self._row(p, lbl, ico)
            v = ctk.BooleanVar(value=CONFIG.get(key, False))
            ctk.CTkSwitch(r, text="", variable=v, width=42,
                          command=lambda k=key, vv=v: CONFIG.update({k: vv.get()}),
                          progress_color=self.c["green"]).pack(side="right")

    def _fill_post(self, p):
        r = self._row(p, "Descripción", "📝")
        cv = ctk.StringVar(value=CONFIG["POST_CAPTION_TEMPLATE"])
        e = ctk.CTkEntry(r, height=28,
                         font=ctk.CTkFont(size=self.FONT_VALUE),
                         textvariable=cv, fg_color=self.c["bg3"])
        e.pack(fill="x")
        e.bind("<FocusOut>", lambda _: CONFIG.update(
            {"POST_CAPTION_TEMPLATE": cv.get()}))

        r = self._row(p, "", "#")
        hv = ctk.StringVar(value=CONFIG["POST_HASHTAGS"])
        e2 = ctk.CTkEntry(r, height=28,
                          font=ctk.CTkFont(size=self.FONT_VALUE),
                          textvariable=hv, fg_color=self.c["bg3"])
        e2.pack(fill="x")
        e2.bind("<FocusOut>", lambda _: CONFIG.update(
            {"POST_HASHTAGS": hv.get()}))

    def _create_buttons(self, parent):
        bf = ctk.CTkFrame(parent, fg_color="transparent")
        bf.pack(expand=True, fill="both", padx=16, pady=10)

        self.btn_start = ctk.CTkButton(
            bf, text="▶  INICIAR", height=self.BTN_H,
            font=ctk.CTkFont(size=14, weight="bold"),
            command=self._toggle_run, fg_color=self.c["green"],
            hover_color="#059669", corner_radius=8)
        self.btn_start.pack(side="left", fill="x", expand=True, padx=(0, 6))

        self.btn_stop = ctk.CTkButton(
            bf, text="⏹  DETENER", height=self.BTN_H,
            font=ctk.CTkFont(size=14, weight="bold"),
            command=self._stop_bot, fg_color=self.c["red"],
            hover_color="#DC2626", corner_radius=8, state="disabled")
        self.btn_stop.pack(side="right", fill="x", expand=True, padx=(6, 0))

    def _create_preview_panel(self):
        panel = ModernCard(self, fg_color=self.c["bg2"])

        hdr = ctk.CTkFrame(panel, fg_color=self.c["bg3"], height=self.HDR_H,
                           corner_radius=0)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)

        hi = ctk.CTkFrame(hdr, fg_color="transparent")
        hi.pack(expand=True, fill="both", padx=16, pady=10)

        ctk.CTkLabel(hi, text="📺",
                     font=ctk.CTkFont(size=16, weight="bold"),
                     text_color=self.c["t1"]).pack(side="left")
        self.wm_badge = ctk.CTkLabel(
            hi, text="● WM: ON",
            font=ctk.CTkFont(size=9, weight="bold"),
            text_color=self.c["green"], fg_color=self.c["card"],
            corner_radius=8, padx=8, pady=3)
        self.wm_badge.pack(side="right")

        body = ctk.CTkFrame(panel, fg_color="transparent")
        body.pack(fill="both", expand=True, padx=10, pady=10)
        body.grid_columnconfigure(0, weight=3)
        body.grid_columnconfigure(1, weight=2)
        body.grid_rowconfigure(0, weight=1)

        canvas_frame = ctk.CTkFrame(
            body, fg_color=self.c["bg1"], corner_radius=10,
            border_width=2, border_color=self.c["border"])
        canvas_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 6))

        canvas_center = ctk.CTkFrame(canvas_frame, fg_color="transparent")
        canvas_center.place(relx=0.5, rely=0.5, anchor="center")

        self.canvas = ctk.CTkCanvas(
            canvas_center, width=GUI_W, height=GUI_H,
            bg="#000000", highlightthickness=0, cursor="crosshair")
        self.canvas.pack()
        self._draw_guides()

        ctk.CTkLabel(
            canvas_frame, text=f"📐 {TARGET_W}×{TARGET_H}  •  {PREVIEW_SCALE:.0%}",
            font=ctk.CTkFont(size=self.FONT_SMALL),
            text_color=self.c["t3"]
        ).place(relx=0.5, rely=0.97, anchor="s")

        ctrl_frame = ctk.CTkFrame(body, fg_color="transparent")
        ctrl_frame.grid(row=0, column=1, sticky="nsew", padx=(6, 0))
        ctrl_frame.grid_rowconfigure(0, weight=1)
        ctrl_frame.grid_columnconfigure(0, weight=1)

        inner_ctrl = ctk.CTkFrame(ctrl_frame, fg_color="transparent")
        inner_ctrl.pack(fill="both", expand=True)

        self._wm_positions(inner_ctrl)
        self._wm_sliders(inner_ctrl)
        self._wm_toggle(inner_ctrl)
        self._create_paths_card(inner_ctrl)

        return panel

    def _wm_positions(self, parent):
        card = ctk.CTkFrame(parent, fg_color=self.c["bg3"], corner_radius=10)
        card.pack(fill="x", pady=(0, 8))

        grid = ctk.CTkFrame(card, fg_color="transparent")
        grid.pack(padx=12, pady=(0, 12))
        grid.grid_columnconfigure((0, 1, 2), weight=1, uniform="pos")

        positions = [
            ("↖", "TL"), ("⬆", "TC"), ("↗", "TR"),
            ("⬅", "CL"), ("⏺", "CC"), ("➡", "CR"),
            ("↙", "BL"), ("⬇", "BC"), ("↘", "BR"),
        ]
        for i, (icon, code) in enumerate(positions):
            r, c = divmod(i, 3)
            ctk.CTkButton(
                grid, text=icon, width=38, height=32,
                font=ctk.CTkFont(size=14),
                fg_color=self.c["card"], hover_color=self.c["blue"],
                command=lambda cd=code: self._set_preset(cd),
            ).grid(row=r, column=c, padx=3, pady=3, sticky="ew")

    def _wm_sliders(self, parent):
        card = ctk.CTkFrame(parent, fg_color=self.c["bg3"], corner_radius=10)
        card.pack(fill="x", pady=(0, 8))

        for label, var, lo, hi_val, fmt in [
            ("X",  self.pos_x,   0, TARGET_W, "d"),
            ("Y",  self.pos_y,   0, TARGET_H, "d"),
            ("Op", self.opacity, 0, 1,        ".2f"),
        ]:
            sf = ctk.CTkFrame(card, fg_color="transparent")
            sf.pack(fill="x", padx=12, pady=3)

            ctk.CTkLabel(sf, text=label,
                         font=ctk.CTkFont(size=self.FONT_VALUE, weight="bold"),
                         text_color=self.c["t2"], width=28).pack(side="left")
            ctk.CTkSlider(
                sf, from_=lo, to=hi_val, variable=var,
                command=lambda _: self._update_preview(),
                button_color=self.c["blue"],
                button_hover_color="#60A5FA", height=16,
            ).pack(side="left", fill="x", expand=True, padx=6)

            val_lbl = ctk.CTkLabel(
                sf, text="",
                font=ctk.CTkFont(family="Consolas", size=self.FONT_SMALL),
                text_color=self.c["t3"], width=42)
            val_lbl.pack(side="right")
            def _updater(lbl=val_lbl, v=var, f=fmt):
                try:
                    lbl.configure(text=format(v.get(), f))
                except Exception:
                    pass
            var.trace_add("write", lambda *_, fn=_updater: fn())
            _updater()

        ctk.CTkFrame(card, fg_color="transparent", height=6).pack()

    def _wm_toggle(self, parent):
        card = ctk.CTkFrame(parent, fg_color=self.c["bg3"], corner_radius=10)
        card.pack(fill="x", pady=(0, 8))

        inner = ctk.CTkFrame(card, fg_color="transparent")
        inner.pack(fill="x", padx=12, pady=10)

        ctk.CTkLabel(inner, text="Watermark",
                     font=ctk.CTkFont(size=self.FONT_LABEL, weight="bold"),
                     text_color=self.c["t2"]).pack(side="left")

        self.wm_switch_var = ctk.BooleanVar(value=CONFIG["WATERMARK_ENABLED"])
        ctk.CTkSwitch(
            inner, text="", variable=self.wm_switch_var, width=42,
            command=self._toggle_watermark,
            progress_color=self.c["green"]).pack(side="right")

        self.logo_info = ctk.CTkLabel(
            card, text="Sin logo cargado",
            font=ctk.CTkFont(size=self.FONT_SMALL),
            text_color=self.c["t3"])
        self.logo_info.pack(padx=12, pady=(0, 10))

    def _create_paths_card(self, parent):
        card = ModernCard(parent, fg_color=self.c["bg3"])
        card.pack(fill="x", pady=(0, 8))

        title_frame = ctk.CTkFrame(card, fg_color="transparent", height=30)
        title_frame.pack(fill="x", padx=12, pady=(8, 4))
        ctk.CTkLabel(title_frame, text="📁",
                     font=ctk.CTkFont(size=self.FONT_TITLE-2, weight="bold"),
                     text_color=self.c["t1"]).pack(side="left")

        grid = ctk.CTkFrame(card, fg_color="transparent")
        grid.pack(fill="x", padx=12, pady=(0, 12))
        grid.grid_columnconfigure(0, weight=1, uniform="path")
        grid.grid_columnconfigure(1, weight=1, uniform="path")

        all_paths = [
            ("Descargas",  "DOWNLOAD_FOLDER",  "📥", self._sel_dl,    0, 0),
            ("Procesados", "OUTPUT_FOLDER",     "📤", self._sel_out,   0, 1),
            ("Vídeo local","LOCAL_VIDEO_PATH",  "🎬", self._sel_video, 1, 0),
            ("Logo / WM",  "WATERMARK_PATH",    "🖼️", self._sel_logo,  1, 1),
        ]
        for label, key, icon, cb, row, col in all_paths:
            cell = ctk.CTkFrame(grid, fg_color="transparent", height=self.ROW_H)
            cell.grid(row=row, column=col, sticky="ew", padx=4, pady=3)
            cell.grid_propagate(False)

            ctk.CTkLabel(cell, text=icon,
                         font=ctk.CTkFont(size=self.FONT_LABEL),
                         text_color=self.c["blue"], width=22).pack(side="left")
            ctk.CTkLabel(cell, text=label,
                         font=ctk.CTkFont(size=self.FONT_VALUE),
                         text_color=self.c["t2"], width=75).pack(side="left")

            val = Path(CONFIG[key]).name if CONFIG[key] else "N/A"
            lbl = ctk.CTkLabel(cell, text=val,
                               font=ctk.CTkFont(size=self.FONT_SMALL),
                               text_color=self.c["t3"])
            lbl.pack(side="left", fill="x", expand=True, padx=4)

            ctk.CTkButton(cell, text="📂", width=30, height=24, command=cb,
                          fg_color=self.c["card"],
                          font=ctk.CTkFont(size=self.FONT_VALUE)
                          ).pack(side="right")

            if key == "DOWNLOAD_FOLDER":
                self.plbl_download_folder = lbl
            elif key == "OUTPUT_FOLDER":
                self.plbl_output_folder = lbl
            elif key == "LOCAL_VIDEO_PATH":
                self.plbl_local_video_path = lbl
            elif key == "WATERMARK_PATH":
                self.plbl_watermark_path = lbl

    def _toggle_watermark(self):
        CONFIG["WATERMARK_ENABLED"] = self.wm_switch_var.get()
        self._update_preview()

    def _draw_guides(self):
        self.canvas.delete("guide")
        for i in (1, 2):
            x = GUI_W * i / 3
            y = GUI_H * i / 3
            self.canvas.create_line(x, 0, x, GUI_H,
                                    fill="#2D3748", dash=(3, 5), tags="guide")
            self.canvas.create_line(0, y, GUI_W, y,
                                    fill="#2D3748", dash=(3, 5), tags="guide")
        self.canvas.create_rectangle(
            0, int(GUI_H * 0.85), GUI_W, GUI_H,
            fill="#1A1F27", outline="#2D3748", stipple="gray50", tags="guide")
        m = 12
        self.canvas.create_rectangle(
            m, m, GUI_W - m, GUI_H - m,
            outline="#2D3748", dash=(2, 6), tags="guide")

    def _set_preset(self, code):
        wx, wy = _calc_watermark_position(
            code, self.logo_dims[0], self.logo_dims[1])
        self.pos_x.set(wx)
        self.pos_y.set(wy)

    def _update_preview(self, *_):
        self.canvas.delete("preview")

        if hasattr(self, "wm_badge"):
            on = CONFIG.get("WATERMARK_ENABLED", True)
            self.wm_badge.configure(
                text=f"● WM: {'ON' if on else 'OFF'}",
                text_color=self.c["green"] if on else self.c["t3"])

        if not CONFIG.get("WATERMARK_ENABLED", True):
            return

        wx, wy = self.pos_x.get(), self.pos_y.get()
        vx, vy = wx * PREVIEW_SCALE, wy * PREVIEW_SCALE
        pw = max(12, int(self.logo_dims[0] * PREVIEW_SCALE))
        ph = max(12, int(self.logo_dims[1] * PREVIEW_SCALE))

        if self.logo_path and os.path.exists(self.logo_path):
            try:
                img = Image.open(self.logo_path).convert("RGBA")
                img = img.resize((pw, ph), Image.Resampling.LANCZOS)
                alpha = img.split()[3].point(
                    lambda p: int(p * self.opacity.get()))
                img.putalpha(alpha)
                self.preview_img = ImageTk.PhotoImage(img)
                self.canvas.create_image(
                    vx, vy, image=self.preview_img,
                    anchor="nw", tags="preview")
                self.canvas.create_rectangle(
                    vx, vy, vx + pw, vy + ph,
                    outline=self.c["green"], width=1,
                    dash=(2, 2), tags="preview")
            except Exception:
                self._placeholder(vx, vy, pw, ph, "Err")
        else:
            self._placeholder(vx, vy, max(20, pw), max(20, ph), "Logo")

    def _placeholder(self, x, y, w, h, text):
        self.canvas.create_rectangle(
            x, y, x + w, y + h,
            fill=self.c["yellow"], outline=self.c["t1"],
            width=1, tags="preview")
        self.canvas.create_text(
            x + w / 2, y + h / 2, text=text,
            fill=self.c["t1"], font=("Arial", 8, "bold"),
            anchor="center", tags="preview")

    def _on_logo_selected(self, filepath):
        if filepath and os.path.exists(filepath):
            self.logo_path = filepath
            CONFIG["WATERMARK_PATH"] = filepath
            try:
                with Image.open(filepath) as img:
                    self.logo_dims = img.size
                self._update_preview()
                self.logo_info.configure(
                    text=f"✓ {Path(filepath).name}  "
                         f"({self.logo_dims[0]}×{self.logo_dims[1]}px)")
                self.status_indicator.configure(
                    text="✓ Logo cargado", text_color=self.c["green"])
            except Exception:
                pass

    def _toggle_run(self):
        if self.running:
            self._stop_bot()
        else:
            self._start_bot()

    def _start_bot(self):
        CONFIG.update({
            "WATERMARK_X": self.pos_x.get(),
            "WATERMARK_Y": self.pos_y.get(),
            "WATERMARK_OPACITY": self.opacity.get(),
            "MODE": self.mode_var.get(),
        })
        self.running = True
        self.btn_start.configure(
            state="disabled", text="● EJECUTANDO…",
            fg_color=self.c["yellow"])
        self.btn_stop.configure(state="normal")
        self.status_indicator.configure(
            text="🔄 Ejecutando…", text_color=self.c["blue"])
        self.worker_thread = threading.Thread(
            target=self._bot_worker, daemon=True)
        self.worker_thread.start()

    def _stop_bot(self):
        self.running = False
        CONFIG["LOOP_ENABLED"] = False
        self.btn_start.configure(
            state="normal", text="▶  INICIAR",
            fg_color=self.c["green"])
        self.btn_stop.configure(state="disabled")
        self.status_indicator.configure(
            text="⏹ Detenido", text_color=self.c["yellow"])

    def _bot_worker(self):
        try:
            _ensure_ffmpeg()
            _ensure_directory(CONFIG["DOWNLOAD_FOLDER"])
            _ensure_directory(CONFIG["OUTPUT_FOLDER"])
            ig_client = _instagram_login()
            iteration = 0

            while self.running:
                iteration += 1
                try:
                    self.status_indicator.configure(
                        text=f"🔄 Iteración #{iteration}")
                except TclError:
                    pass

                if not _check_resources():
                    time.sleep(300)
                    continue

                try:
                    mode = CONFIG["MODE"]
                    wm = (CONFIG["WATERMARK_PATH"]
                          if CONFIG["WATERMARK_ENABLED"] else None)
                    kw = dict(
                        watermark_path=wm,
                        wx=CONFIG["WATERMARK_X"],
                        wy=CONFIG["WATERMARK_Y"],
                        opacity=CONFIG["WATERMARK_OPACITY"],
                        enhance=CONFIG["ENHANCE_QUALITY"],
                        bitrate=CONFIG["VIDEO_BITRATE"])

                    if mode in ("tiktok", "both"):
                        if _process_tiktok_mode(ig_client, self.data_mgr, **kw):
                            self.success_count += 1
                    if mode in ("local", "both"):
                        if _process_local_mode(ig_client, self.data_mgr, **kw):
                            self.success_count += 1

                except Exception as e:
                    self.error_count += 1
                    self.data_mgr.register_error(
                        f"iter_{iteration}", "bot", str(e))
                    time.sleep(60)
                    continue

                if not CONFIG["LOOP_ENABLED"]:
                    break

                base = CONFIG["LOOP_DELAY_SECONDS"]
                jitter = int(base * CONFIG["RANDOM_JITTER_PERCENT"] / 100)
                delay = random.randint(base - jitter, base + jitter)
                for _ in range(delay // 2):
                    if not self.running:
                        break
                    time.sleep(2)

        except Exception:
            pass
        finally:
            self.running = False
            try:
                self.btn_start.configure(
                    state="normal", text="▶  INICIAR",
                    fg_color=self.c["green"])
                self.btn_stop.configure(state="disabled")
            except TclError:
                pass

    def _sel_dl(self):
        f = filedialog.askdirectory()
        if f:
            CONFIG["DOWNLOAD_FOLDER"] = f
            if hasattr(self, "plbl_download_folder"):
                self.plbl_download_folder.configure(text=Path(f).name)

    def _sel_out(self):
        f = filedialog.askdirectory()
        if f:
            CONFIG["OUTPUT_FOLDER"] = f
            if hasattr(self, "plbl_output_folder"):
                self.plbl_output_folder.configure(text=Path(f).name)

    def _sel_video(self):
        f = filedialog.askopenfilename(
            filetypes=[("Videos", "*.mp4 *.mov *.mkv *.avi")])
        if f:
            CONFIG["LOCAL_VIDEO_PATH"] = f
            if hasattr(self, "plbl_local_video_path"):
                self.plbl_local_video_path.configure(text=Path(f).name)

    def _sel_logo(self):
        f = filedialog.askopenfilename(filetypes=[("PNG", "*.png")])
        if f:
            self._on_logo_selected(f)
            if hasattr(self, "plbl_watermark_path"):
                self.plbl_watermark_path.configure(text=Path(f).name)

    def _on_closing(self):
        self._stop_bot()
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=2)
        self.destroy()
        sys.exit(0)


def main():
    try:
        for folder in [CONFIG["DATA_FOLDER"], CONFIG["DOWNLOAD_FOLDER"],
                       CONFIG["OUTPUT_FOLDER"]]:
            Path(folder).mkdir(parents=True, exist_ok=True)
        app = BotGUI()
        app.protocol("WM_DELETE_WINDOW", app._on_closing)
        app.mainloop()
    except Exception as e:
        messagebox.showerror("Error", f"Error al iniciar:\n{str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()