import hashlib
import os
from PIL import Image, ImageOps
import requests
from io import BytesIO


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def download_telegram_file(file_url: str, dest_path: str):
    ensure_dir(os.path.dirname(dest_path))
    r = requests.get(file_url, timeout=60)
    r.raise_for_status()
    with open(dest_path, 'wb') as f:
        f.write(r.content)


def normalize_exif(path_in: str, path_out: str):
    ensure_dir(os.path.dirname(path_out))
    with Image.open(path_in) as im:
        im = ImageOps.exif_transpose(im)
        im.convert('RGB').save(path_out, format='JPEG', quality=92, subsampling=1, optimize=True)


def save_preview(image: Image.Image, path_out: str):
    ensure_dir(os.path.dirname(path_out))
    image.convert('RGB').save(path_out, format='JPEG', quality=85, subsampling=1, optimize=True)


def delete_tree(path: str):
    if os.path.isdir(path):
        for root, dirs, files in os.walk(path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(path)
