from PIL import Image
import imagehash


def compute_phash(image_path: str) -> imagehash.ImageHash:
    """Compute perceptual hash for an image file."""
    img = Image.open(image_path)
    return imagehash.phash(img)


def is_unique(new_hash: imagehash.ImageHash, existing_hashes: list, threshold: int) -> bool:
    """Return True if new_hash is sufficiently different from all existing_hashes."""
    for h in existing_hashes:
        if new_hash - h <= threshold:
            return False
    return True
