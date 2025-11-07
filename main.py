import os
import logging
import asyncio
import re
import shutil
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict
import hashlib

from fastapi import FastAPI, Form, HTTPException, Request, BackgroundTasks
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import yt_dlp
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging with colors
class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m'
    }
    
    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        record.levelname = f"{log_color}{record.levelname}{self.COLORS['RESET']}"
        return super().format(record)

handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter('%(asctime)s - %(levelname)s - %(message)s'))
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Initialize FastAPI app
app = FastAPI(
    title="Social Media Downloader Pro",
    version="4.0.0",
    description="Professional media downloader with advanced features",
    docs_url=None,  # Disable docs in production
    redoc_url=None
)

# Add middlewares
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Configuration
DOWNLOAD_FOLDER = os.getenv("DOWNLOAD_FOLDER", "downloads")
HISTORY_FILE = os.path.join(DOWNLOAD_FOLDER, ".history.json")
MAX_FILE_AGE_DAYS = int(os.getenv("MAX_FILE_AGE_DAYS", "1"))
MAX_HISTORY_ITEMS = int(os.getenv("MAX_HISTORY_ITEMS", "100"))
PORT = int(os.getenv("PORT", "8000"))

# Create necessary directories
Path(DOWNLOAD_FOLDER).mkdir(exist_ok=True)
Path("templates").mkdir(exist_ok=True)

# Mount templates
templates = Jinja2Templates(directory="templates")

# Download history management
def load_history() -> List[Dict]:
    """Load download history from file"""
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error loading history: {e}")
    return []

def save_history(history: List[Dict]):
    """Save download history to file"""
    try:
        # Keep only last MAX_HISTORY_ITEMS
        history = history[-MAX_HISTORY_ITEMS:]
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Error saving history: {e}")

def add_to_history(data: Dict):
    """Add item to download history"""
    history = load_history()
    history.append({
        **data,
        'timestamp': datetime.now().isoformat(),
        'id': hashlib.md5(str(datetime.now().timestamp()).encode()).hexdigest()[:8]
    })
    save_history(history)

def sanitize_filename(filename: str, max_length: int = 100) -> str:
    """Sanitize filename to be safe across all operating systems"""
    # Remove or replace problematic characters
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    filename = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', filename)
    filename = re.sub(r'\s+', '_', filename)
    filename = re.sub(r'\.+', '.', filename)
    filename = re.sub(r'_+', '_', filename)
    filename = filename.strip('._')
    
    # Limit length
    if len(filename) > max_length:
        name, ext = os.path.splitext(filename)
        filename = name[:max_length - len(ext)] + ext
    
    return filename or "download"

def clean_old_files():
    """Remove files older than MAX_FILE_AGE_DAYS"""
    try:
        now = datetime.now()
        cutoff = now - timedelta(days=MAX_FILE_AGE_DAYS)
        deleted_count = 0
        
        for file_path in Path(DOWNLOAD_FOLDER).glob("*"):
            if file_path.is_file() and not file_path.name.startswith('.'):
                file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                if file_time < cutoff:
                    file_path.unlink()
                    deleted_count += 1
        
        if deleted_count > 0:
            logger.info(f"🗑️  Cleaned up {deleted_count} old file(s)")
    except Exception as e:
        logger.error(f"Error cleaning old files: {str(e)}")

def get_format_string(quality: str, format_type: str) -> str:
    """Get yt-dlp format string based on quality and format type"""
    if format_type == "audio":
        return "bestaudio/best"
    
    quality_map = {
        "best": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        "high": "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080][ext=mp4]/best",
        "medium": "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best",
        "low": "bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/best[height<=480][ext=mp4]/best"
    }
    
    return quality_map.get(quality, "best")

def get_common_headers():
    """Get common headers to avoid bot detection"""
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-us,en;q=0.5',
        'Accept-Encoding': 'gzip,deflate',
        'Connection': 'keep-alive',
    }

def get_platform_from_url(url: str) -> str:
    """Detect platform from URL"""
    url_lower = url.lower()
    if 'youtube.com' in url_lower or 'youtu.be' in url_lower:
        return 'YouTube'
    elif 'tiktok.com' in url_lower:
        return 'TikTok'
    elif 'instagram.com' in url_lower:
        return 'Instagram'
    elif 'twitter.com' in url_lower or 'x.com' in url_lower:
        return 'Twitter/X'
    elif 'facebook.com' in url_lower or 'fb.watch' in url_lower:
        return 'Facebook'
    elif 'vimeo.com' in url_lower:
        return 'Vimeo'
    elif 'reddit.com' in url_lower:
        return 'Reddit'
    elif 'twitch.tv' in url_lower:
        return 'Twitch'
    else:
        return 'Other'

@app.on_event("startup")
async def startup_event():
    """Run on application startup"""
    logger.info("=" * 70)
    logger.info("🚀 Social Media Downloader Pro v4.0 - Starting")
    logger.info("=" * 70)
    logger.info(f"📁 Download folder: {DOWNLOAD_FOLDER}")
    logger.info(f"🗑️  Auto-cleanup: {MAX_FILE_AGE_DAYS} day(s)")
    logger.info(f"📜 Max history: {MAX_HISTORY_ITEMS} items")
    logger.info(f"🌐 Port: {PORT}")
    clean_old_files()
    history = load_history()
    logger.info(f"📊 History loaded: {len(history)} items")
    logger.info("✅ Application ready!")
    logger.info("=" * 70)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Serve the main page"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/health")
async def health_check():
    """Enhanced health check endpoint"""
    return {
        "status": "ok",
        "version": "4.0.0",
        "timestamp": datetime.now().isoformat(),
        "uptime": "active",
        "features": ["download", "history", "stats", "dark_mode"]
    }

@app.get("/stats")
async def get_stats():
    """Get comprehensive download statistics"""
    try:
        files = list(Path(DOWNLOAD_FOLDER).glob("*"))
        file_list = [f for f in files if f.is_file() and not f.name.startswith('.')]
        total_files = len(file_list)
        total_size = sum(f.stat().st_size for f in file_list)
        
        # Get available space
        try:
            stat = os.statvfs(DOWNLOAD_FOLDER)
            available_space = stat.f_bavail * stat.f_frsize / (1024 * 1024)
        except (AttributeError, OSError):
            try:
                stat = shutil.disk_usage(DOWNLOAD_FOLDER)
                available_space = stat.free / (1024 * 1024)
            except Exception:
                available_space = 0
        
        # Get history stats
        history = load_history()
        
        return {
            "total_downloads": total_files,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "available_space_mb": round(available_space, 2),
            "history_count": len(history),
            "last_download": history[-1]['timestamp'] if history else None
        }
    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}")
        return {
            "total_downloads": 0,
            "total_size_mb": 0,
            "available_space_mb": 0,
            "history_count": 0,
            "last_download": None
        }

@app.get("/history")
async def get_history(limit: int = 50):
    """Get download history"""
    try:
        history = load_history()
        return {"history": history[-limit:][::-1]}  # Latest first
    except Exception as e:
        logger.error(f"Error getting history: {str(e)}")
        return {"history": []}

@app.delete("/history")
async def clear_history():
    """Clear download history"""
    try:
        save_history([])
        logger.info("🗑️  History cleared")
        return {"success": True, "message": "History cleared successfully"}
    except Exception as e:
        logger.error(f"Error clearing history: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/download")
async def download_media(
    background_tasks: BackgroundTasks,
    url: str = Form(...),
    quality: str = Form("best"),
    format_type: str = Form("video")
):
    """Download video/photo with advanced features"""
    
    # Validate URL
    if not url.startswith(("http://", "https://")):
        logger.warning(f"❌ Invalid URL format: {url}")
        raise HTTPException(status_code=400, detail="URL must start with http:// or https://")
    
    platform = get_platform_from_url(url)
    
    logger.info("=" * 80)
    logger.info(f"📥 New download request")
    logger.info(f"🌐 Platform: {platform}")
    logger.info(f"🔗 URL: {url[:80]}...")
    logger.info(f"🎨 Quality: {quality}")
    logger.info(f"📼 Format: {format_type}")
    
    # Configure filename
    timestamp = int(datetime.now().timestamp())
    filename_template = f"{timestamp}_%(title).100s.%(ext)s"
    
    # Get format string
    format_string = get_format_string(quality, format_type)
    
    # Configure yt-dlp options
    ydl_opts = {
        'format': format_string,
        'outtmpl': os.path.join(DOWNLOAD_FOLDER, filename_template),
        'restrictfilenames': True,
        'windowsfilenames': True,
        'no_warnings': False,
        'ignoreerrors': False,
        'quiet': False,
        'no_color': True,
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'referer': 'https://www.google.com/',
        'http_headers': get_common_headers(),
        'extractor_retries': 5,
        'fragment_retries': 5,
        'skip_unavailable_fragments': True,
        'nocheckcertificate': True,
        'age_limit': None,
        'youtube_include_dash_manifest': True,
        'youtube_include_hls_manifest': True,
        'cookiefile': 'cookies.txt',
    }
    
    # Add format-specific options
    if format_type == "audio":
        ydl_opts.update({
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
            'prefer_ffmpeg': True,
        })
    else:
        ydl_opts.update({
            'postprocessors': [{
                'key': 'FFmpegVideoConvertor',
                'preferedformat': 'mp4',
            }],
        })
    
    try:
        # Download using yt-dlp
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            logger.info("⏳ Extracting video information...")
            info = ydl.extract_info(url, download=True)
            
            # Get filename
            if format_type == "audio":
                base_filename = ydl.prepare_filename(info)
                filename = base_filename.rsplit('.', 1)[0] + '.mp3'
            else:
                filename = ydl.prepare_filename(info)
            
            basename = os.path.basename(filename)
            basename = sanitize_filename(basename)
            
            # Get metadata
            title = (info.get('title') or 'Unknown')[:150]
            uploader = (info.get('uploader') or 'Unknown')[:100]
            duration = info.get('duration') or 0
            thumbnail = info.get('thumbnail') or ''
            view_count = info.get('view_count') or 0
            
            # Get quality info
            if format_type == "video":
                height = info.get('height') or 0
                width = info.get('width') or 0
                fps = info.get('fps') or 0
                if height and width:
                    quality_info = f"{width}x{height}"
                    if fps:
                        quality_info += f" @{fps}fps"
                else:
                    quality_info = "Unknown"
            else:
                quality_info = "MP3 Audio (192kbps)"
            
            # Get file size
            filesize = info.get('filesize') or info.get('filesize_approx') or 0
            filesize_mb = round(filesize / (1024 * 1024), 2) if filesize > 0 else 0
            
            logger.info("✅ Download successful!")
            logger.info(f"📄 Filename: {basename}")
            logger.info(f"🎬 Title: {title[:50]}...")
            logger.info(f"📊 Quality: {quality_info}")
            logger.info(f"💾 Size: {filesize_mb} MB")
            logger.info("=" * 80)
            
            # Add to history
            history_entry = {
                'url': url,
                'title': title,
                'filename': basename,
                'platform': platform,
                'quality': quality_info,
                'format': format_type,
                'size_mb': filesize_mb,
                'duration': duration
            }
            add_to_history(history_entry)
            
            return JSONResponse({
                "success": True,
                "message": "Download completed successfully",
                "filename": basename,
                "download_url": f"/media/{basename}",
                "metadata": {
                    "title": title,
                    "uploader": uploader,
                    "duration": duration,
                    "thumbnail": thumbnail,
                    "quality": quality_info,
                    "format_type": format_type,
                    "filesize": filesize,
                    "platform": platform,
                    "views": view_count
                }
            })
            
    except yt_dlp.utils.DownloadError as e:
        error_msg = str(e)
        logger.error(f"❌ Download error: {error_msg}")
        logger.error("=" * 80)
        raise HTTPException(status_code=400, detail=f"Download failed: {error_msg}")
    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ Unexpected error: {error_msg}")
        logger.error("=" * 80)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {error_msg}")

@app.get("/media/{filename}")
async def serve_media(filename: str):
    """Serve downloaded media files"""
    filename = os.path.basename(filename)
    file_path = os.path.join(DOWNLOAD_FOLDER, filename)
    
    if not os.path.exists(file_path):
        logger.warning(f"⚠️  File not found: {filename}")
        raise HTTPException(status_code=404, detail="File not found")
    
    logger.info(f"📤 Serving file: {filename}")
    return FileResponse(
        file_path,
        media_type="application/octet-stream",
        filename=filename,
        headers={
            "Cache-Control": "public, max-age=3600",
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )

@app.delete("/media/{filename}")
async def delete_media(filename: str):
    """Delete a specific media file"""
    filename = os.path.basename(filename)
    file_path = os.path.join(DOWNLOAD_FOLDER, filename)
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    try:
        os.remove(file_path)
        logger.info(f"🗑️  Deleted file: {filename}")
        return {"success": True, "message": f"File deleted successfully"}
    except Exception as e:
        logger.error(f"❌ Error deleting file: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/list")
async def list_downloads():
    """List all downloaded files"""
    try:
        files = []
        for file_path in Path(DOWNLOAD_FOLDER).glob("*"):
            if file_path.is_file() and not file_path.name.startswith('.'):
                stat = file_path.stat()
                files.append({
                    "filename": file_path.name,
                    "size_mb": round(stat.st_size / (1024 * 1024), 2),
                    "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                    "download_url": f"/media/{file_path.name}"
                })
        
        return {"files": sorted(files, key=lambda x: x['created'], reverse=True)}
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        return {"files": []}

@app.post("/cleanup")
async def cleanup_old_files():
    """Manually trigger cleanup of old files"""
    try:
        clean_old_files()
        return {"success": True, "message": "Cleanup completed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=True)