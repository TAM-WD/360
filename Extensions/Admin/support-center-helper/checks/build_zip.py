"""Build the unpacked extension distribution without tests, CRX or nested archives."""
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

root = Path(__file__).resolve().parent.parent
files = [
    'manifest.json', 'background.js', 'content.js', 'interceptor.js', 'navigation.js',
    'navigation.css', 'popup.js', 'popup.html', 'popup.css', 'readme.md',
    'icons/icon16.png', 'icons/icon48.png', 'icons/icon128.png',
]
with ZipFile(root / 'support-center-helper.zip', 'w', ZIP_DEFLATED) as archive:
    for name in files:
        archive.write(root / name, name)
print(f'Built support-center-helper.zip: {len(files)} files')
