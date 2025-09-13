from PIL import Image
import numpy as np

# Перцептуальный хэш (pHash) 64-bit

def phash(image: Image.Image) -> int:
    img = image.convert('L').resize((32, 32), Image.Resampling.LANCZOS)
    pixels = np.asarray(img, dtype=np.float32)
    # DCT по строкам, затем по столбцам
    dct1 = np.real(np.fft.fft(pixels, axis=0))
    dct2 = np.real(np.fft.fft(dct1, axis=1))
    dct = dct2[:8, :8]
    med = np.median(dct)
    bits = (dct > med).astype(np.uint8)
    val = 0
    for b in bits.flatten():
        val = (val << 1) | int(b)
    return val


def hamming(a: int, b: int) -> int:
    return (a ^ b).bit_count()
