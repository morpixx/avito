import pytest
import os
from storage.storage import save_metadata, create_zip
import json

def test_save_metadata_and_zip(tmp_path):
    lst = tmp_path / "listing_01"
    os.makedirs(lst, exist_ok=True)
    metadata = {'a':1}
    save_metadata(str(lst), metadata)
    meta_path = lst / 'metadata.json'
    assert meta_path.exists()
    data = json.loads(meta_path.read_text(encoding='utf-8'))
    assert data == metadata
    # create dummy files
    f1 = lst / 'f1.txt'
    f1.write_text('x')
    zipf = tmp_path / 'out.zip'
    zp = create_zip(str(zipf), [str(lst)])
    assert os.path.exists(zp)
    import zipfile
    with zipfile.ZipFile(zp) as z:
        assert 'listing_01/f1.txt' in z.namelist()
