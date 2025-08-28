# -*- coding: utf-8 -*-
def classFactory(iface):  # QGIS calls this
	from .plugin import TableCommentNotepadPlugin
	return TableCommentNotepadPlugin(iface)

