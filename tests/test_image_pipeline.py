import pytest
import asyncio
from image_pipeline.pipeline import process_images
import shutil
import os


@pytest.mark.asyncio
async def test_process_images(tmp_path):
    # Create dummy images
    photos = []
    for i in range(3):
        img_path = tmp_path / f"orig{i}.jpg"
        from PIL import Image
        Image.new('RGB', (100,100), color=(i*50, i*50, i*50)).save(img_path)
        photos.append(str(img_path))
    out_dir = tmp_path / "out"
    paths = await process_images(photos, watermark=True, output_dir=str(out_dir), seed=42)
    assert len(paths) == 3
    for p in paths:
        assert os.path.exists(p)
