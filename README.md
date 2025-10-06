# Table Comment Notepad (QGIS Plugin)

A Chat-GPTv5-generated utility to read/edit **PostgreSQL/GeoPackage table comments** for loaded PostGIS and GeoPackage layers, with a comfortable text box and **Update/Revert** buttons. Perfect if you use table comments as your metadata changelog and want that to flow into QGIS “Abstract” and Shapefile exports.

## Features
- Lists loaded **PostGIS** and **GeoPackage** layers in the current project
- Loads the table’s current **COMMENT** (from source)
- Full-sized, no-wrap text editor for multi-line comments
- **Update comment → DB** writes back via provider connection
- **Revert** restores the comment to what was originally loaded if changes have not yet been pushed
- No external libraries; uses QGIS’ provider connections
- Now includes column-level commenting!

## Requirements
- QGIS 3.22+
- A PostGIS or GeoPackage layer loaded in the project
- DB user with `COMMENT` privilege on the table

## License
GPL-3.0-or-later


