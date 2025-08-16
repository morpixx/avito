import os
import json
import zipfile
from typing import List, Dict


def save_metadata(listing_folder: str, metadata: Dict):
    """
    Save metadata.json into the listing folder.
    """
    os.makedirs(listing_folder, exist_ok=True)
    path = os.path.join(listing_folder, 'metadata.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)


def create_zip(output_path: str, listing_dirs: List[str]) -> str:
    """
    Create ZIP archive containing given listing directories.
    """
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as z:
        for listing in listing_dirs:
            for root, _, files in os.walk(listing):
                for file in files:
                    full = os.path.join(root, file)
                    arcname = os.path.relpath(full, os.path.dirname(listing))
                    z.write(full, arcname)
    return output_path
