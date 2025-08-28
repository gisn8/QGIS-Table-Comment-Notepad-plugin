# -*- coding: utf-8 -*-
from qgis.PyQt import QtWidgets
from qgis.core import (
	QgsProject, QgsDataSourceUri, QgsProviderRegistry, QgsMessageLog, Qgis
)

import os, sqlite3
from qgis.core import QgsProviderRegistry

def _is_gpkg_layer(lyr):
	try:
		if lyr.providerType() not in ("ogr", "gdal"):
			return False
		# decode OGR URI for path/layer name
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
	# Depending on how the layer was added, OGR may expose 'layerName' or 'layer'
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
	# Use QGIS’ provider connection (no psycopg2 needed)
	md = QgsProviderRegistry.instance().providerMetadata('postgres')
	return md.createConnection(uri.connectionInfo(), {})  # returns QgsAbstractDatabaseProviderConnection

def _fetch_comment(conn, schema: str, table: str) -> str:
	regclass = _qualify(schema, table)
	sql = f"SELECT obj_description('{regclass}'::regclass);"
	rows = conn.executeSql(sql)
	if rows and rows[0]:
		return rows[0][0] or ""
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

def _pg_comment_keyword(conn, schema: str, table: str) -> str:
	"""
	Return the correct COMMENT ON keyword for this relation:
	TABLE, VIEW, MATERIALIZED VIEW, FOREIGN TABLE
	"""
	reg = _qualify(schema, table)
	rows = conn.executeSql(f"SELECT relkind FROM pg_class WHERE oid = '{reg}'::regclass;")
	relkind = rows[0][0] if rows and rows[0] else None
	# Map relkind -> keyword
	# r = ordinary table, p = partitioned table
	# v = view, m = materialized view, f = foreign table
	mapping = {
		'r': 'TABLE',
		'p': 'TABLE',
		'v': 'VIEW',
		'm': 'MATERIALIZED VIEW',
		'f': 'FOREIGN TABLE',
	}
	return mapping.get(relkind, 'TABLE')

def _pg_type_label(conn, schema: str, table: str) -> str:
	"""Nice label like [VIEW] or [FOREIGN TABLE] for UI."""
	try:
		kw = _pg_comment_keyword(conn, schema, table)
		return f"[{kw}]"
	except Exception:
		return ""


class CommentNotepadDialog(QtWidgets.QDialog):
	def __init__(self, parent=None):
		super().__init__(parent)
		self.setWindowTitle("Table Comment Notepad (PostgreSQL)")
		self.resize(850, 600)

		self.layers = [
			lyr for lyr in QgsProject.instance().mapLayers().values()
			if (_is_postgres_layer(lyr) or _is_gpkg_layer(lyr))
		]

		if not self.layers:
			self.valid = False
			return

		self.combo = QtWidgets.QComboBox()
		for lyr in self.layers:
			try:
				if _is_postgres_layer(lyr):
					_, schema, table = _layer_uri_parts(lyr)
					label = f"{lyr.name()}  —  PG: {schema}.{table}"
				elif _is_gpkg_layer(lyr):
					path, table = _gpkg_path_and_table(lyr)
					label = f"{lyr.name()}  —  GPKG: {os.path.basename(path)}::{table}"
				else:
					label = lyr.name()
			except Exception:
				label = lyr.name()
			self.combo.addItem(label)


		self.text = QtWidgets.QTextEdit()
		self.text.setLineWrapMode(QtWidgets.QTextEdit.NoWrap)
		self.text.setPlaceholderText("<NULL>")

		self.status = QtWidgets.QLabel("")
		self.status.setStyleSheet("color: gray;")

		self.btnUpdate = QtWidgets.QPushButton("Update comment → DB")
		self.btnRevert = QtWidgets.QPushButton("Revert to original")
		self.btnClose = QtWidgets.QPushButton("Close")

		btns = QtWidgets.QHBoxLayout()
		btns.addWidget(self.btnUpdate)
		btns.addWidget(self.btnRevert)
		btns.addStretch(1)
		btns.addWidget(self.btnClose)

		layout = QtWidgets.QVBoxLayout(self)
		layout.addWidget(QtWidgets.QLabel("Choose a loaded PostGIS/GeoPackage layer:"))
		layout.addWidget(self.combo)
		layout.addWidget(QtWidgets.QLabel("Edit table comment:"))
		layout.addWidget(self.text, 1)
		layout.addWidget(self.status)
		layout.addLayout(btns)

		self._original_by_idx = {}

		self.combo.currentIndexChanged.connect(self._load_selected)
		self.btnUpdate.clicked.connect(self._update_comment)
		self.btnRevert.clicked.connect(self._revert_comment)
		self.btnClose.clicked.connect(self.accept)

		self._load_selected()

	def _current_layer(self):
		return self.layers[self.combo.currentIndex()]

	def _load_selected(self):
		lyr = self._current_layer()
		try:
			if _is_postgres_layer(lyr):
				uri, schema, table = _layer_uri_parts(lyr)
				conn = _pg_conn_from_uri(uri)
				comment = _fetch_comment(conn, schema, table)
				kind = _pg_type_label(conn, schema, table)
				where = f"PG {kind} {schema}.{table}"
			elif _is_gpkg_layer(lyr):
				path, table = _gpkg_path_and_table(lyr)
				comment = _gpkg_fetch_comment(path, table)
				where = f"GPKG {os.path.basename(path)}::{table}"
			else:
				raise RuntimeError("Unsupported layer type.")
			self.text.setPlainText(comment)
			self._original_by_idx[self.combo.currentIndex()] = comment
			self.status.setText(f"Loaded {where}")
		except Exception as e:
			self.text.setPlainText("")
			self.status.setText(f"Load error: {e}")
			QgsMessageLog.logMessage(f"[TableCommentNotepad] Load error: {e}", "TableCommentNotepad", Qgis.Warning)

	def _update_comment(self):
		lyr = self._current_layer()
		txt = self.text.toPlainText()
		try:
			if _is_postgres_layer(lyr):
				uri, schema, table = _layer_uri_parts(lyr)
				conn = _pg_conn_from_uri(uri)
				_set_comment(conn, schema, table, txt)
				kind = _pg_type_label(conn, schema, table)
				where = f"PG {kind} {schema}.{table}"
			elif _is_gpkg_layer(lyr):
				path, table = _gpkg_path_and_table(lyr)
				_gpkg_set_comment(path, table, txt)
				where = f"GPKG {os.path.basename(path)}::{table}"
			else:
				raise RuntimeError("Unsupported layer type.")

			self._original_by_idx[self.combo.currentIndex()] = txt
			self.status.setText(f"Updated {where}")
			QtWidgets.QMessageBox.information(self, "Success", f"Comment updated on {where}.")
			# Optional: make the change visible immediately in QGIS’ UI
			self._apply_abstract_to_layer(lyr, txt)
		except Exception as e:
			self.status.setText(f"Update failed: {e}")
			QtWidgets.QMessageBox.critical(self, "Update failed", str(e))

	def _revert_comment(self):
		idx = self.combo.currentIndex()
		original = self._original_by_idx.get(idx, "")
		lyr = self._current_layer()
		try:
			if _is_postgres_layer(lyr):
				uri, schema, table = _layer_uri_parts(lyr)
				conn = _pg_conn_from_uri(uri)
				_set_comment(conn, schema, table, original)
				kind = _pg_type_label(conn, schema, table)
				where = f"PG {kind} {schema}.{table}"
			elif _is_gpkg_layer(lyr):
				path, table = _gpkg_path_and_table(lyr)
				_gpkg_set_comment(path, table, original)
				where = f"GPKG {os.path.basename(path)}::{table}"
			else:
				raise RuntimeError("Unsupported layer type.")

			self.text.setPlainText(original)
			self.status.setText(f"Reverted {where}")
			QtWidgets.QMessageBox.information(self, "Reverted", f"Comment reverted on {where}.")
			# Optional session update:
			# lyr.setAbstract(original)
		except Exception as e:
			self.status.setText(f"Revert failed: {e}")
			QtWidgets.QMessageBox.critical(self, "Revert failed", str(e))

	def _apply_abstract_to_layer(self, layer, txt: str):
		try:
			# Preferred, works across QGIS 3.x
			md = layer.metadata()
			md.setAbstract(txt or "")
			layer.setMetadata(md)
		except Exception:
			# Fallback for older builds that still expose setAbstract()
			if hasattr(layer, "setAbstract"):
				layer.setAbstract(txt or "")

