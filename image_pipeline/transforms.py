import random
from PIL import Image, ImageDraw, ImageFilter, ImageEnhance


def resize(image: Image.Image, size=(800, 600)) -> Image.Image:
    return image.resize(size, Image.ANTIALIAS)


def random_crop(image: Image.Image) -> Image.Image:
    w, h = image.size
    crop_w, crop_h = int(w * 0.9), int(h * 0.9)
    left = random.randint(0, w - crop_w)
    top = random.randint(0, h - crop_h)
    return image.crop((left, top, left + crop_w, top + crop_h))


def rotate(image: Image.Image) -> Image.Image:
    angle = random.uniform(-10, 10)
    return image.rotate(angle, expand=True)


def add_vignette(image: Image.Image) -> Image.Image:
    w, h = image.size
    vign = Image.new('L', (w, h), 0)
    draw = ImageDraw.Draw(vign)
    for i in range(int(max(w, h)/2)):
        draw.ellipse(
            (i, i, w-i, h-i),
            fill=int(255 * (i / (max(w, h)/2)))
        )
    image.putalpha(vign)
    return image.convert('RGB')


def add_noise(image: Image.Image) -> Image.Image:
    import numpy as np
    arr = np.array(image)
    noise = np.random.randint(0, 25, arr.shape, dtype='uint8')
    arr = np.clip(arr + noise, 0, 255)
    return Image.fromarray(arr)


def apply_watermark(image: Image.Image, text="Avito", opacity=0.1) -> Image.Image:
    watermark = Image.new('RGBA', image.size)
    draw = ImageDraw.Draw(watermark)
    font_size = int(min(image.size) / 15)
    try:
        from PIL import ImageFont
        font = ImageFont.truetype("arial.ttf", font_size)
    except Exception:
        font = None
    text_w, text_h = draw.textsize(text, font=font)
    pos = ((image.size[0] - text_w) // 2, (image.size[1] - text_h) // 2)
    draw.text(pos, text, fill=(255,255,255,int(255*opacity)), font=font)
    return Image.alpha_composite(image.convert('RGBA'), watermark).convert('RGB')
