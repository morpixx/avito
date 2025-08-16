import os
import tempfile
import zipfile
import logging

logger = logging.getLogger(__name__)

import os
import tempfile
import shutil
import logging
import random
from aiogram import Bot

from text_generator.generator import generate_texts
from image_pipeline.pipeline import process_images
from uniqueness_check.phash_checker import compute_phash, is_unique
from storage.storage import save_metadata, create_zip

logger = logging.getLogger(__name__)
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

async def start_job(description: str, photo_ids: list, num_listings: int, watermark: bool) -> str:
    """
    Download photos by Telegram file_id, generate texts and processed images,
    ensure uniqueness by pHash, save metadata and return path to ZIP.
    """
    # Create job temp directory
    job_dir = tempfile.mkdtemp(prefix='job_')
    orig_dir = os.path.join(job_dir, 'orig')
    os.makedirs(orig_dir, exist_ok=True)
    bot = Bot(token=TOKEN)
    # Download original photos
    local_photos = []
    for idx, fid in enumerate(photo_ids, start=1):
        # If fid is a local file path, use directly
        if os.path.exists(str(fid)):
            path = os.path.join(orig_dir, f'{idx:02d}.jpg')
            shutil.copy(str(fid), path)
        else:
            file = await bot.get_file(fid)
            path = os.path.join(orig_dir, f'{idx:02d}.jpg')
            await bot.download_file(file.file_path, destination=path)
        local_photos.append(path)
    # Generate listing texts
    texts = await generate_texts(description, num_listings)
    # Process each listing
    listing_dirs = []
    seed = random.randint(0, 1_000_000)
    for i in range(1, num_listings + 1):
        lst_name = f'listing_{i:02d}'
        lst_dir = os.path.join(job_dir, lst_name)
        os.makedirs(lst_dir, exist_ok=True)
        # Generate images
        imgs = await process_images(local_photos, watermark, lst_dir, seed=seed + i)
        # Check uniqueness and metadata
        hashes = []
        metadata = {'description': description, 'text': texts[i-1], 'images': []}
        for img in imgs:
            ph = compute_phash(img)
            # if too similar, note but keep
            unique = is_unique(ph, hashes, int(os.getenv('PHASH_THRESHOLD', '8')))
            hashes.append(ph)
            metadata['images'].append({'file': os.path.basename(img), 'phash': str(ph), 'unique': unique})
        # Save metadata.json
        save_metadata(lst_dir, metadata)
        listing_dirs.append(lst_dir)
    # Create ZIP
    zip_path = os.path.join(job_dir, 'job_listings.zip')
    create_zip(zip_path, listing_dirs)
    # Cleanup originals
    shutil.rmtree(orig_dir)
    return zip_path
