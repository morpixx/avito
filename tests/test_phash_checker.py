import pytest
from uniqueness_check.phash_checker import compute_phash, is_unique
from PIL import Image
import tempfile


def create_sample_image(color, path):
    img = Image.new('RGB', (100,100), color=color)
    img.save(path)


def test_compute_phash_and_uniqueness(tmp_path):
    p1 = tmp_path / "a.jpg"
    p2 = tmp_path / "b.jpg"
    create_sample_image((255,0,0), str(p1))
    create_sample_image((255,0,0), str(p2))
    h1 = compute_phash(str(p1))
    h2 = compute_phash(str(p2))
    assert h1 - h2 == 0
    assert not is_unique(h2, [h1], threshold=0)
    # different color
    p3 = tmp_path / "c.jpg"
    create_sample_image((0,255,0), str(p3))
    h3 = compute_phash(str(p3))
    assert is_unique(h3, [h1], threshold=0)
