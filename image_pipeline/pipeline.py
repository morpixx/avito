import os
import random
import logging
from PIL import Image
import piexif
from image_pipeline.transforms import resize, random_crop, rotate, add_vignette, add_noise, apply_watermark
from uniqueness_check.phash_checker import compute_phash, is_unique
logger = logging.getLogger(__name__)


async def process_images(photo_paths: list, watermark: bool, output_dir: str, seed: int=None) -> list:
    """
    Process list of input image file paths and save transformed images to output_dir.
    Returns list of output paths.
    """
    os.makedirs(output_dir, exist_ok=True)
    random.seed(seed)
    output_paths = []

    for idx, src in enumerate(photo_paths, start=1):
        img = Image.open(src)
        # remove geolocation EXIF if present
        exif_bytes = img.info.get('exif')
        if exif_bytes:
            exif_dict = piexif.load(exif_bytes)
            exif_dict.pop('GPS', None)
            exif_bytes = piexif.dump(exif_dict)
        else:
            exif_bytes = None
        # transformations
        img = resize(img)
        img = random_crop(img)
        img = rotate(img)
        img = add_vignette(img)
        img = add_noise(img)
        if watermark:
            img = apply_watermark(img, opacity=random.uniform(0.08, 0.12))
        # recompress
        quality = random.randint(75, 90)
        out_path = os.path.join(output_dir, f"img_{idx:02d}.jpg")
        # Save with or without EXIF
        if exif_bytes:
            img.save(out_path, "JPEG", quality=quality, exif=exif_bytes)
        else:
            img.save(out_path, "JPEG", quality=quality)
        output_paths.append(out_path)
    return output_paths
