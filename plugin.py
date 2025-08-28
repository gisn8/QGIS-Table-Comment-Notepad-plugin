# -*- coding: utf-8 -*-
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction
from qgis.core import QgsApplication
from .dialog import CommentNotepadDialog
from qgis.PyQt.QtCore import Qt

import os

MENU_TEXT = "Table Comment Notepad…"

class TableCommentNotepadPlugin:
	def __init__(self, iface):
		self.iface = iface
		self.action = None
		self.dlg = None

	def initGui(self):
		menu = self.iface.databaseMenu()

		# Defensive: remove any stale actions left from previous reloads
		for act in list(menu.actions()):
			if act.text().replace("&", "") == MENU_TEXT:
				menu.removeAction(act)
				act.deleteLater()

		# Make the QAction a child of the *menu* (not mainWindow)
		icon = QIcon(os.path.join(os.path.dirname(__file__), "icon.png"))
		self.action = QAction(icon, MENU_TEXT, menu)
		self.action.setObjectName("table_comment_notepad_action")
		self.action.triggered.connect(self.run)

		menu.addAction(self.action)

		# optional toolbar — if you keep it, parent it to mainWindow and clean up in unload()
		self.iface.addToolBarIcon(self.action)

	def unload(self):
		# If dialog is open, close it to break signal/slots before removing actions
		try:
			if self.dlg and self.dlg.isVisible():
				self.dlg.close()
		except Exception:
			pass
		if self.dlg:
			self.dlg.deleteLater()
			self.dlg = None

		# Remove our action from the Database menu (and any stragglers)
		menu = self.iface.databaseMenu()
		try:
			if self.action and self.action in menu.actions():
				menu.removeAction(self.action)
		except Exception:
			pass

		# Belt-and-suspenders: remove by text match too
		for act in list(menu.actions()):
			if act.text().replace("&", "") == MENU_TEXT:
				menu.removeAction(act)
				act.deleteLater()

		# Toolbar cleanup
		if self.action:
			try:
				self.iface.removeToolBarIcon(self.action)
			except Exception:
				pass
			self.action.deleteLater()
			self.action = None

	def run(self):
		# hold a reference so unload() can close it safely on reload
		self.dlg = CommentNotepadDialog(self.iface.mainWindow())
		
		if not getattr(self.dlg, "valid", True):
			self.iface.messageBar().pushWarning(
				"Table Comment Notepad",
				"No loaded PostGIS or GeoPackage layers found."
			)
			# self.dlg.deleteLater()
			return

		# Modeless (does NOT block the main window), but throws errors when layers get removed.
		# Fixable, but complicates things. 
		# self.dlg.setModal(False)  # optional; show() is modeless by default
		# self.dlg.show()
		# self.dlg.raise_()
		# self.dlg.activateWindow()
		
		# self.dlg.exec_()
		self.dlg.exec() if hasattr(self.dlg, "exec") else self.dlg.exec_() # Q4 uses dlg.exec()
