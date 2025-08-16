import pytest
import asyncio
import os

from job_manager.manager import start_job

@ pytest.mark.asyncio
async def test_integration(tmp_path):
    # Prepare 10 dummy photos
    photos = []
    from PIL import Image
    for i in range(10):
        p = tmp_path / f"img{i}.jpg"
        Image.new('RGB', (100,100), color=(i*20, i*20, i*20)).save(p)
        photos.append(str(p))
    # Run job
    zip_path = await start_job(
        description="Тестовая квартира",
        photo_ids=photos,
        num_listings=5,
        watermark=True
    )
    assert os.path.exists(zip_path)
    # Check ZIP contents
    import zipfile
    with zipfile.ZipFile(zip_path) as z:
        names = z.namelist()
        # Expect at least listing_01/metadata.json
        assert any('listing_01/metadata.json' in n for n in names)
