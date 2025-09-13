import os
import zipfile

def pack_job(root_folder: str, archive_path: str, root_name: str | None = None):
    os.makedirs(os.path.dirname(archive_path), exist_ok=True)
    with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        for foldername, subfolders, filenames in os.walk(root_folder):
            for filename in filenames:
                abs_path = os.path.join(foldername, filename)
                rel_path = os.path.relpath(abs_path, os.path.dirname(root_folder))
                if root_name:
                    rel_path = os.path.join(root_name, os.path.relpath(abs_path, root_folder))
                z.write(abs_path, rel_path)
    return archive_path
