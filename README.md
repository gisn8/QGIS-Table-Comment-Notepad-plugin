# Table Comment Notepad (QGIS Plugin)

A Chat-GPTv5-generated utility to read/edit **PostgreSQL/GeoPackage table comments** for loaded PostGIS and GeoPackage layers, with a comfortable text box and **Update/Revert** buttons. Perfect if you use table comments as your metadata changelog and want that to flow into QGIS “Abstract” and Shapefile exports.

## Features
- Lists loaded **PostGIS** and **GeoPackage** layers in the current project
- Loads the table’s current **COMMENT** (from source)
- Full-sized, no-wrap text editor for multi-line comments
- **Update comment → DB** writes back via provider connection
- **Revert** restores the comment to what was originally loaded if changes have not yet been pushed
- No external libraries; uses QGIS’ provider connections

## Requirements
- QGIS 3.22+
- A PostGIS or GeoPackage layer loaded in the project
- DB user with `COMMENT` privilege on the table

## Install (dev)
1. Copy this folder to your QGIS profile plugins dir, e.g.:
   - Linux: `~/.local/share/QGIS/QGIS3/profiles/default/python/plugins/table_comment_notepad`
   - Windows: `%APPDATA%\QGIS\QGIS3\profiles\default\python\plugins\table_comment_notepad`
2. Restart QGIS → **Plugins → Manage and Install** → enable **Table Comment Notepad**.
3. Use via **Database → Table Comment Notepad…**

## Packaging for QGIS Plugin Repository
1. Ensure `metadata.txt` is correct; bump `version` when updating.
2. Include a 24×24 `icon.png`.
3. Zip the folder **contents** (the folder itself at top-level), e.g.: 
   cd table_comment_notepad && zip -r ../table_comment_notepad.zip .
4. Upload the zip at https://plugins.qgis.org/ (log in, then “Add plugin”).
5. Tag releases properly and keep a short **changelog** in `metadata.txt`.

## License
GPL-3.0-or-later


