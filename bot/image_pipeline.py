from PIL import Image, ImageEnhance, ImageFilter, ImageOps
import numpy as np
import hashlib
import random

PLACEMENTS = {
    'tl': 'tl', 'tr': 'tr', 'bl': 'bl', 'br': 'br', 'center': 'center'
}


def seeded_rng(job_id: str, variant_index: int, src_index: int):
    seed = int(hashlib.sha256(f"{job_id}:{variant_index}:{src_index}".encode()).hexdigest(), 16) % (2**32)
    rng = random.Random(seed)
    return rng


def soft_augment(img: Image.Image, rng: random.Random) -> Image.Image:
    im = ImageOps.exif_transpose(img).convert('RGB')
    w, h = im.size
    # crop/resize 1-6%
    perc = rng.uniform(0.01, 0.06)
    dx, dy = int(w * perc), int(h * perc)
    im = im.crop((dx, dy, w - dx, h - dy)).resize((w, h), Image.Resampling.LANCZOS)
    # micro rotate +-1.5 deg
    angle = rng.uniform(-1.5, 1.5)
    im = im.rotate(angle, resample=Image.Resampling.BICUBIC, expand=True, fillcolor=(255, 255, 255))
    im = im.resize((w, h), Image.Resampling.LANCZOS)
    # brightness/contrast/sharpness
    b = 1.0 + rng.uniform(-0.05, 0.05)
    c = 1.0 + rng.uniform(-0.05, 0.05)
    s = 1.0 + rng.uniform(-0.05, 0.05)
    im = ImageEnhance.Brightness(im).enhance(b)
    im = ImageEnhance.Contrast(im).enhance(c)
    im = ImageEnhance.Sharpness(im).enhance(s)
    # light noise
    arr = np.asarray(im).astype(np.int16)
    noise = rng.normalvariate(0, 3)
    arr = np.clip(arr + noise, 0, 255).astype(np.uint8)
    im = Image.fromarray(arr)
    # slight blur
    im = im.filter(ImageFilter.GaussianBlur(radius=0.3))
    return im


def apply_watermark(img: Image.Image, wm_img: Image.Image, placement: str = 'br', opacity: int = 70, margin: int = 24) -> Image.Image:
    placement = PLACEMENTS.get(placement, 'br')
    base = img.convert('RGBA')
    logo = wm_img.convert('RGBA')

    # scale logo ~14% of width
    bw, bh = base.size
    target_w = int(bw * 0.14)
    ratio = target_w / logo.width
    logo = logo.resize((target_w, int(logo.height * ratio)), Image.Resampling.LANCZOS)

    # apply opacity
    alpha = logo.split()[-1]
    alpha = alpha.point(lambda p: int(p * (opacity / 100.0)))
    logo.putalpha(alpha)

    x, y = 0, 0
    if placement == 'br':
      x = bw - logo.width - margin
      y = bh - logo.height - margin
    elif placement == 'bl':
      x = margin
      y = bh - logo.height - margin
    elif placement == 'tr':
      x = bw - logo.width - margin
      y = margin
    elif placement == 'tl':
      x = margin
      y = margin
    elif placement == 'center':
      x = (bw - logo.width) // 2
      y = (bh - logo.height) // 2

    base.alpha_composite(logo, (x, y))
    return base.convert('RGB')
