import fs from 'fs';
import path from 'path';
import archiver from 'archiver';

export async function createZipFromFolders({ inputFolders, outputZipPath, rootFolderName, flatten = false, files = [] }) {
  await fs.promises.mkdir(path.dirname(outputZipPath), { recursive: true });
  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(outputZipPath);
    const archive = archiver('zip', { zlib: { level: 9 } });

    output.on('close', () => resolve({ bytes: archive.pointer(), output: outputZipPath }));
    archive.on('warning', (err) => {
      if (err.code === 'ENOENT') {
        console.warn(err);
      } else {
        reject(err);
      }
    });
    archive.on('error', (err) => reject(err));

    archive.pipe(output);

    for (const folder of inputFolders) {
      if (flatten) {
        // Кладём содержимое папки непосредственно под rootFolderName (без добавления basename)
        archive.directory(folder, rootFolderName || false);
      } else {
        const folderName = path.basename(folder);
        const destPath = rootFolderName ? path.join(rootFolderName, folderName) : folderName;
        archive.directory(folder, destPath);
      }
    }

    // Добавляем отдельные файлы (если заданы)
    for (const f of files) {
      if (!f?.path || !f?.name) continue;
      const arcName = rootFolderName ? path.join(rootFolderName, f.name) : f.name;
      archive.file(f.path, { name: arcName });
    }

    archive.finalize();
  });
}
