# -*- coding: utf-8 -*-
from qgis.PyQt import QtWidgets, QtCore, QtGui
from qgis.core import (
	QgsProject, QgsDataSourceUri, QgsProviderRegistry, QgsMessageLog, Qgis
)

import os
import sqlite3
import re

_ident_rx = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

# -------------------- GPKG helpers --------------------

def _is_gpkg_layer(lyr):
	try:
		if lyr.providerType() not in ("ogr", "gdal"):
			return False
		md = QgsProviderRegistry.instance().providerMetadata("ogr")
		parts = md.decodeUri(lyr.source())
		path = (parts.get("path") or "").lower()
		return path.endswith(".gpkg")
	except Exception:
		return False

def _gpkg_path_and_table(lyr):
	md = QgsProviderRegistry.instance().providerMetadata("ogr")
	parts = md.decodeUri(lyr.source())
	path = parts.get("path")
	# OGR may expose 'layerName' or 'layer'
	table = parts.get("layerName") or parts.get("layer")
	if not (path and table):
		raise RuntimeError("Could not parse GPKG path/table from layer source.")
	return path, table

def _gpkg_fetch_comment(path: str, table: str) -> str:
	con = sqlite3.connect(path, timeout=2)
	try:
		con.execute("PRAGMA busy_timeout=2000")
		cur = con.execute("SELECT description FROM gpkg_contents WHERE table_name = ?", (table,))
		row = cur.fetchone()
		return row[0] if row and row[0] else ""
	finally:
		con.close()

def _gpkg_set_comment(path: str, table: str, text: str):
	con = sqlite3.connect(path, timeout=2)
	try:
		con.execute("PRAGMA busy_timeout=2000")
		con.execute(
			"UPDATE gpkg_contents SET description = ? WHERE table_name = ?",
			(text if text != "" else None, table)
		)
		con.commit()
	finally:
		con.close()

# -------------------- PostgreSQL helpers --------------------

def _is_postgres_layer(lyr):
	try:
		return hasattr(lyr, "providerType") and lyr.providerType() == "postgres"
	except Exception:
		return False

def _quote_ident(name: str) -> str:
	return '"' + name.replace('"', '""') + '"'

def _qualify(schema: str, table: str) -> str:
	return f"{_quote_ident(schema)}.{_quote_ident(table)}"

def _pick_dollar_tag(text: str) -> str:
	for tag in ("$$", "$q$", "$qq$", "$zzz$"):
		if tag not in text:
			return tag
	import uuid
	return f"${str(uuid.uuid4()).replace('-','')[:6]}$"

def _layer_uri_parts(lyr):
	uri = QgsDataSourceUri(lyr.source())
	return uri, uri.schema(), uri.table()

def _pg_conn_from_uri(uri: QgsDataSourceUri):
	# Use QGIS provider connection (no psycopg2)
	md = QgsProviderRegistry.instance().providerMetadata('postgres')
	return md.createConnection(uri.connectionInfo(), {})  # QgsAbstractDatabaseProviderConnection

def _fetch_comment(conn, schema: str, table: str) -> str:
	regclass = _qualify(schema, table)
	sql = f"SELECT obj_description('{regclass}'::regclass);"
	rows = conn.executeSql(sql)
	if rows and rows[0]:
		return rows[0][0] or ""
	return ""

def _pg_comment_keyword(conn, schema: str, table: str) -> str:
	"""
	Return correct COMMENT ON keyword for this relation:
	TABLE, VIEW, MATERIALIZED VIEW, FOREIGN TABLE
	"""
	reg = _qualify(schema, table)
	rows = conn.executeSql(f"SELECT relkind FROM pg_class WHERE oid = '{reg}'::regclass;")
	relkind = rows[0][0] if rows and rows[0] else None
	mapping = {
		'r': 'TABLE',              # ordinary table
		'p': 'TABLE',              # partitioned table
		'v': 'VIEW',
		'm': 'MATERIALIZED VIEW',
		'f': 'FOREIGN TABLE',
	}
	return mapping.get(relkind, 'TABLE')

def _pg_type_label(conn, schema: str, table: str) -> str:
	try:
		kw = _pg_comment_keyword(conn, schema, table)
		return f"[{kw}]"
	except Exception:
		return ""

def _set_comment(conn, schema: str, table: str, text: str):
	"""COMMENT ON the correct relation type (table/view/mview/foreign table)."""
	qt = _qualify(schema, table)
	kw = _pg_comment_keyword(conn, schema, table)
	if text is None or text == "":
		sql = f"COMMENT ON {kw} {qt} IS NULL;"
	else:
		tag = _pick_dollar_tag(text)
		sql = f"COMMENT ON {kw} {qt} IS {tag}{text}{tag};"
	conn.executeSql(sql)

def _pg_is_query_layer(lyr):
	"""
	True if this Postgres layer is a subquery (no single base relation).
	We treat as simple relation only when both schema and table look like plain identifiers.
	"""
	try:
		if not _is_postgres_layer(lyr):
			return False
		uri = QgsDataSourceUri(lyr.source())
		schema = (uri.schema() or "").strip()
		table = (uri.table() or "").strip()
		# Subqueries typically start with '(' or contain spaces/SELECT
		if not schema or not table:
			return True
		tlow = table.lower()
		if table.startswith("(") or "select" in tlow or " join " in tlow or " from " in tlow:
			return True
		# Be strict: only accept simple identifiers (no dots, no spaces)
		if not _ident_rx.match(schema) or not _ident_rx.match(table):
			return True
		return False
	except Exception:
		# If we can't parse it safely, treat as query layer (skip)
		return True


# -------------------- Shared helpers --------------------

def _relation_key_from_layer(lyr):
	# Identify the underlying relation so we can find all matching layers
	if _is_postgres_layer(lyr) and not _pg_is_query_layer(lyr):
		uri = QgsDataSourceUri(lyr.source())
		host = (uri.host() or "").strip().lower()
		if host == "localhost":
			host = "127.0.0.1"
		return ("pg", host, str(uri.port() or ""), (uri.database() or "").lower(),
				uri.schema() or "", uri.table() or "")
	if _is_gpkg_layer(lyr):
		path, table = _gpkg_path_and_table(lyr)
		return ("gpkg", os.path.abspath(path), table)
	return None

def _layers_sharing_relation(lyr):
	key = _relation_key_from_layer(lyr)
	if not key:
		return [lyr]
	out = []
	for l in QgsProject.instance().mapLayers().values():
		try:
			if _relation_key_from_layer(l) == key:
				out.append(l)
		except Exception:
			pass
	return out

def supported_layers():
	layers = []
	for lyr in QgsProject.instance().mapLayers().values():
		if _is_gpkg_layer(lyr):
			layers.append(lyr)
		elif _is_postgres_layer(lyr) and not _pg_is_query_layer(lyr):
			layers.append(lyr)
	return layers


# -------------------- Dialog --------------------

class CommentNotepadDialog(QtWidgets.QDialog):
	def __init__(self, parent=None, layers=None):
		super().__init__(parent)
		self.setWindowTitle("Table Comment Notepad (PostgreSQL / GeoPackage)")

		# State/maps
		self.layers = layers if layers is not None else supported_layers()
		self._original_by_layerid = {}   # layer_id -> original comment
		self._group_by_label = {}  # full_label -> [layer_id, ...]

		# ---- Build UI (create widgets BEFORE any loads) ----
		# Clamp dialog size to screen and allow shrinking
		self.setSizeGripEnabled(True)
		screen = QtWidgets.QApplication.primaryScreen().availableGeometry()
		pad = 80
		base_w, base_h = 850, 600
		self.resize(min(base_w, screen.width() - pad), min(base_h, screen.height() - pad))
		self.setMinimumSize(360, 300)

		# Top row: label + layer picker
		top = QtWidgets.QHBoxLayout()
		lbl = QtWidgets.QLabel("Choose a loaded PostGIS / GeoPackage layer:")
		lbl.setWordWrap(True)
		top.addWidget(lbl)
		self.combo = QtWidgets.QComboBox()
		self.combo.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
		self.combo.setSizeAdjustPolicy(QtWidgets.QComboBox.AdjustToMinimumContentsLengthWithIcon)
		self.combo.setMinimumContentsLength(28)

		# Make the combo searchable without letting users add new items
		self.combo.setEditable(True)
		self.combo.setInsertPolicy(QtWidgets.QComboBox.NoInsert)

		# Nice UX: hint in the field
		self.combo.lineEdit().setPlaceholderText("Type to search…")

		# Autocomplete settings: popup list, case-insensitive, match ANYWHERE
		comp = self.combo.completer()
		comp.setCompletionMode(QtWidgets.QCompleter.PopupCompletion)
		comp.setCaseSensitivity(QtCore.Qt.CaseInsensitive)
		comp.setFilterMode(QtCore.Qt.MatchContains)

		top.addWidget(self.combo, 1)

		# Editor
		self.text = QtWidgets.QTextEdit()
		self.text.setLineWrapMode(QtWidgets.QTextEdit.NoWrap)
		self.text.setPlaceholderText("<NULL>")

		# Status + buttons
		self.status = QtWidgets.QLabel("")
		self.status.setStyleSheet("color: gray;")

		self.chkSyncAll = QtWidgets.QCheckBox("Sync Abstract to all layers using this datasource")
		# remember preference
		s = QtCore.QSettings()
		self.chkSyncAll.setChecked(s.value("table_comment_notepad/sync_all_layers", True, type=bool))
		self.chkSyncAll.toggled.connect(lambda v: QtCore.QSettings().setValue(
			"table_comment_notepad/sync_all_layers", bool(v)))

		self.btnUpdate = QtWidgets.QPushButton("Update comment → DB")
		self.btnRevert = QtWidgets.QPushButton("Revert to original")
		self.btnClose = QtWidgets.QPushButton("Close")

		btns = QtWidgets.QHBoxLayout()
		btns.addWidget(self.btnUpdate)
		btns.addWidget(self.btnRevert)
		btns.addStretch(1)
		btns.addWidget(self.btnClose)

		# Main layout
		lay = QtWidgets.QVBoxLayout(self)
		lay.addLayout(top)
		lay.addWidget(QtWidgets.QLabel("Edit relation comment:"))
		lay.addWidget(self.text, 1)
		lay.addWidget(self.status)
		lay.addWidget(self.chkSyncAll)
		lay.addLayout(btns)

		# If no compatible layers (plugin should guard, but be defensive)
		if not self.layers:
			self.combo.setEnabled(False)
			self.text.setEnabled(False)
			self.btnUpdate.setEnabled(False)
			self.btnRevert.setEnabled(False)
			self.status.setText("No compatible layers loaded.")
			return

		# Populate combo without firing signals
		self._populate_combo()

		# Connect signals
		self.combo.currentIndexChanged.connect(self._load_selected)
		self.btnUpdate.clicked.connect(self._update_comment)
		self.btnRevert.clicked.connect(self._revert_comment)
		self.btnClose.clicked.connect(self.accept)

		# Initial load
		self._load_selected()

		# Elide long entries in the dropdown view (after creation)
		if self.combo.view():
			self.combo.view().setTextElideMode(QtCore.Qt.ElideMiddle)

		# Clamp again just in case any sizeHints were huge
		self.adjustSize()
		self.resize(min(self.width(), screen.width() - pad),
					min(self.height(), screen.height() - pad))

	# ---------- Combo/list helpers ----------

	def _label_for_layer(self, lyr):
		try:
			if _is_postgres_layer(lyr):
				if _pg_is_query_layer(lyr):
					return f"{lyr.name()} — PG: [QUERY]"
				_, schema, table = _layer_uri_parts(lyr)
				return f"{lyr.name()} — PG: {schema}.{table}"
			if _is_gpkg_layer(lyr):
				path, table = _gpkg_path_and_table(lyr)
				return f"{lyr.name()} — GPKG: {os.path.basename(path)}::{table}"
		except Exception:
			pass
		return lyr.name()


	def _add_combo_item(self, lyr):
		"""
		Display an elided label; dedupe by the full label (layer name + source).
		Store one representative layer id in UserRole, and the full list in UserRole+1.
		"""
		label_full = self._label_for_layer(lyr)  # e.g., "corps — PG: public.corps"
		lid = lyr.id()

		# If we've already added this exact pair, just record the extra instance and skip adding.
		ids = self._group_by_label.get(label_full)
		if ids is not None:
			ids.append(lid)
			return

		# First time we see this label: create the group and add an item
		self._group_by_label[label_full] = [lid]

		fm = self.combo.fontMetrics()
		elided = fm.elidedText(label_full, QtCore.Qt.ElideMiddle, 40 * fm.averageCharWidth())

		self.combo.addItem(elided)
		idx = self.combo.count() - 1
		# Representative id for DB ops:
		self.combo.setItemData(idx, lid, QtCore.Qt.UserRole)
		# All ids that share this label (so we can refresh Abstract on every instance):
		self.combo.setItemData(idx, self._group_by_label[label_full], QtCore.Qt.UserRole + 1)
		# Keep the full label for tooltips and (if you later want) easy lookup:
		self.combo.setItemData(idx, label_full, QtCore.Qt.UserRole + 2)
		self.combo.setItemData(idx, label_full, QtCore.Qt.ToolTipRole)

	def _populate_combo(self):
		self._group_by_label = {}
		self.combo.blockSignals(True)
		self.combo.clear()
		for lyr in self.layers:
			self._add_combo_item(lyr)
		self.combo.blockSignals(False)

	def _current_layer(self):
		idx = self.combo.currentIndex()
		if idx < 0:
			return None
		layer_id = self.combo.itemData(idx, QtCore.Qt.UserRole)
		return QgsProject.instance().mapLayer(layer_id)

	# ---------- Load / Update / Revert ----------

	def _load_selected(self):
		lyr = self._current_layer()

		if _is_postgres_layer(lyr) and _pg_is_query_layer(lyr):
			self.text.setPlainText("")
			self.status.setText("Unsupported: Postgres query layers (no single base relation).")
			self.btnUpdate.setEnabled(False)
			self.btnRevert.setEnabled(False)
			return
		else:
			self.btnUpdate.setEnabled(True)
			self.btnRevert.setEnabled(True)

		comment = ""
		where = ""
		if not lyr:
			self.text.setPlainText("")
			self.status.setText("No compatible layers loaded.")
			return

		try:
			if _is_postgres_layer(lyr):
				uri, schema, table = _layer_uri_parts(lyr)
				conn = _pg_conn_from_uri(uri)
				comment = _fetch_comment(conn, schema, table) or ""
				kind = _pg_type_label(conn, schema, table)
				where = f"PG {kind} {schema}.{table}"
			elif _is_gpkg_layer(lyr):
				path, table = _gpkg_path_and_table(lyr)
				comment = _gpkg_fetch_comment(path, table) or ""
				where = f"GPKG {os.path.basename(path)}::{table}"
			else:
				where = "Unsupported layer"
		except Exception as e:
			where = f"Load error: {e}"
			QgsMessageLog.logMessage(f"[TableCommentNotepad] Load error: {e}", "TableCommentNotepad", Qgis.Warning)

		self.text.setPlainText(comment)
		self.status.setText(f"Loaded {where}" if where else "Loaded")
		# Set the original baseline for this layer id
		id_now = lyr.id()
		self._original_by_layerid[id_now] = comment

	def _update_comment(self):
		# Work with the currently selected layer (by id)
		lyr = self._current_layer()
		if not lyr:
			return

		txt = self.text.toPlainText()
		original = self._original_by_layerid.get(lyr.id(), "")

		try:
			# --- write only if changed ---
			if _is_postgres_layer(lyr):
				uri, schema, table = _layer_uri_parts(lyr)
				conn = _pg_conn_from_uri(uri)
				if txt != original:
					_set_comment(conn, schema, table, txt)
				where = f"PG { _pg_type_label(conn, schema, table) } {schema}.{table}"
			elif _is_gpkg_layer(lyr):
				path, table = _gpkg_path_and_table(lyr)
				if txt != original:
					_gpkg_set_comment(path, table, txt)
				where = f"GPKG {os.path.basename(path)}::{table}"
			else:
				return  # unsupported

			# --- sync Abstracts only if checkbox is ON ---
			updated_n = self._apply_abstract(lyr, txt)

			if txt == original:
				# no DB/file write; optionally report Abstract sync
				if updated_n:
					self.status.setText(f"No changes; Abstract synced to {updated_n} layer(s).")
					QtWidgets.QMessageBox.information(self, "No changes",
													  f"Abstract synced to {updated_n} layer(s).")
				else:
					self.status.setText("No changes.")
				return

			# mark new baseline & report success
			self._original_by_layerid[lyr.id()] = txt
			if updated_n:
				self.status.setText(f"Updated {where}; Abstract synced to {updated_n} layer(s).")
			else:
				self.status.setText(f"Updated {where}.")
			QtWidgets.QMessageBox.information(self, "Success", "Comment updated.")

		except Exception as e:
			self.status.setText(f"Update failed: {e}")
			QtWidgets.QMessageBox.critical(self, "Update failed", str(e))


	def _revert_comment(self):
		# Work with the currently selected layer (by id)
		lyr = self._current_layer()
		if not lyr:
			return

		original = self._original_by_layerid.get(lyr.id(), "")

		# --- if editor already matches original, just (optionally) sync Abstracts ---
		if self.text.toPlainText() == original:
			updated_n = self._apply_abstract(lyr, original)
			if updated_n:
				self.status.setText(f"No changes; Abstract synced to {updated_n} layer(s).")
				QtWidgets.QMessageBox.information(self, "No changes",
												  f"Abstract synced to {updated_n} layer(s).")
			else:
				self.status.setText("No changes.")
			return

		try:
			# --- write ORIGINAL back to the DB / file ---
			if _is_postgres_layer(lyr):
				uri, schema, table = _layer_uri_parts(lyr)
				conn = _pg_conn_from_uri(uri)
				_set_comment(conn, schema, table, original)
				where = f"PG { _pg_type_label(conn, schema, table) } {schema}.{table}"
			elif _is_gpkg_layer(lyr):
				path, table = _gpkg_path_and_table(lyr)
				_gpkg_set_comment(path, table, original)
				where = f"GPKG {os.path.basename(path)}::{table}"
			else:
				return  # unsupported

			# --- reset editor & baseline ---
			self.text.setPlainText(original)
			self._original_by_layerid[lyr.id()] = original

			# --- sync Abstracts only if checkbox is ON ---
			updated_n = self._apply_abstract(lyr, original)

			if updated_n:
				self.status.setText(f"Reverted {where}; Abstract synced to {updated_n} layer(s).")
			else:
				self.status.setText(f"Reverted {where}.")
			QtWidgets.QMessageBox.information(self, "Reverted", "Comment reverted.")

		except Exception as e:
			self.status.setText(f"Revert failed: {e}")
			QtWidgets.QMessageBox.critical(self, "Revert failed", str(e))



	# ---------- Layer abstract helper ----------

	def _apply_abstract(self, base_layer, txt: str) -> int:
		"""
		If the 'Sync Abstract...' checkbox is checked, update the Abstract
		on all layers that share this datasource. Otherwise, do nothing.
		Returns the number of layers updated.
		"""
		if not (getattr(self, "chkSyncAll", None) and self.chkSyncAll.isChecked()):
			return 0  # checkbox OFF => no Abstract changes at all

		targets = _layers_sharing_relation(base_layer) or []
		for l in targets:
			if l:
				self._apply_abstract_to_layer(l, txt or "")
		return len(targets)


	def _apply_abstract_to_layer(self, layer, txt: str):
		try:
			md = layer.metadata()
			md.setAbstract(txt or "")
			layer.setMetadata(md)
		except Exception:
			if hasattr(layer, "setAbstract"):
				layer.setAbstract(txt or "")
