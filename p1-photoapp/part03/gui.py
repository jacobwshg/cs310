import sys
import os
from PyQt6.QtWidgets import (
	QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
	QPushButton, QTableWidget, QTableWidgetItem, QLineEdit, 
	QLabel, QFileDialog, QMessageBox, QTabWidget, QHeaderView, QInputDialog
)
from PyQt6.QtGui import QPixmap
from PyQt6.QtCore import Qt

#import part02.photoapp as photoapp 
from part02 import photoapp

class ImagePopup(QWidget):
	"""Simple popup window to display the downloaded image."""
	def __init__(self, image_path):
		super().__init__()

		self.setWindowTitle(f"Image Preview: { image_path }")
		label = QLabel()
		pixmap = QPixmap(image_path)
		# Scale image to fit reasonable window size while keeping aspect ratio
		label.setPixmap(pixmap.scaled(800, 600, Qt.AspectRatioMode.KeepAspectRatio))

		layout = QVBoxLayout()
		layout.addWidget(label)
		self.setLayout(layout)

class PhotoAppGUI(QMainWindow):
	def __init__(self):
		super().__init__()
		self.setWindowTitle("S3 PhotoApp")
		self.resize(1000, 600)

		# Main Layout
		self.tabs = QTabWidget()
		self.setCentralWidget(self.tabs)

		# Tab 1: Image Gallery / Download
		self.search_images_tab = QWidget()
		self.setup_search_images_tab()
		self.tabs.addTab(self.search_images_tab, "Search images")

		# Tab 2: Upload
		self.upload_tab = QWidget()
		self.setup_upload_tab()
		self.tabs.addTab(self.upload_tab, "Upload new image")

		self.view_users_tab = QWidget()
		self.setup_view_users_tab()
		self.tabs.addTab( self.view_users_tab, "View users" )

		self.label_search_tab = QWidget()
		self.setup_label_search_tab()
		self.tabs.addTab( self.label_search_tab, "Search by label" )

	def setup_search_images_tab(self):
		
		# Search & Filter Row
		search_layout = QHBoxLayout()
		
		self.user_id_input = QLineEdit()
		self.user_id_input.setPlaceholderText("Filter by user ID (leave blank for all)")
		
		search_btn = QPushButton("List all / user images")
		search_btn.clicked.connect( self.refresh_image_list )
 
		search_layout.addWidget( QLabel("User:") )
		search_layout.addWidget( self.user_id_input )
		search_layout.addWidget( search_btn )

		# Image Table
		column_names = [ "asset id", "userid", "filename", "bucketkey", "labels", "download" ]

		self.img_tbl = QTableWidget( 0, len( column_names ) ) 
		self.img_tbl.setHorizontalHeaderLabels( column_names )
		self.img_tbl.horizontalHeader()\
			.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

		del_btn = QPushButton( "Delete all images" )
		del_btn.clicked.connect( self.delete_handler )

		layout = QVBoxLayout()
		layout.addLayout( search_layout )
		layout.addWidget( self.img_tbl )
		layout.addWidget( del_btn )

		self.search_images_tab.setLayout(layout)

	def setup_view_users_tab( self ):

		column_names = [ "userid", "username", "given name", "family name" ]

		self.usr_tbl = QTableWidget( 0, len( column_names ) )
		self.usr_tbl.setHorizontalHeaderLabels( column_names )
		self.usr_tbl.horizontalHeader()\
			.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

		def get_users_handler():
			try:
				usrlist = photoapp.get_users()
				self.usr_tbl.setRowCount(0)
				for row_idx, ( uid, uname, gn, fn ) in enumerate( usrlist ):
					self.usr_tbl.insertRow( row_idx )
					self.usr_tbl.setItem( row_idx, 0, QTableWidgetItem( str( uid ) ) )
					self.usr_tbl.setItem( row_idx, 1, QTableWidgetItem( uname ) )
					self.usr_tbl.setItem( row_idx, 2, QTableWidgetItem( gn ) )
					self.usr_tbl.setItem( row_idx, 3, QTableWidgetItem( fn ) )
			except Exception as e:
				QMessageBox.critical(
					self,
					"Error",
					f"Failed to get user list: {str(e)}"
				)

		get_users_btn = QPushButton("View All Users")
		get_users_btn.clicked.connect( get_users_handler )

		layout = QVBoxLayout()
		layout.addWidget( get_users_btn )
		layout.addWidget( self.usr_tbl )

		self.view_users_tab.setLayout( layout )   

	def setup_upload_tab(self):

		input_layout = QHBoxLayout()
 
		self.upload_user_id = QLineEdit()
		self.upload_user_id.setPlaceholderText("Enter user ID for upload")

		input_layout.addWidget( QLabel("User ID:") )
		input_layout.addWidget( self.upload_user_id )

		select_file_btn = QPushButton("Select and upload image")
		select_file_btn.clicked.connect(self.upload_handler)

		layout = QVBoxLayout()
		layout.addLayout( input_layout )
		layout.addWidget( select_file_btn )
		layout.addStretch()

		self.upload_tab.setLayout( layout )

	def setup_label_search_tab(self):

		column_names = [ "assetid", "label", "confidence", "all labels", "download" ]

		self.label_tbl = QTableWidget( 0, len( column_names ) )
		self.label_tbl.setHorizontalHeaderLabels( column_names )
		self.label_tbl.horizontalHeader()\
			.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

		self.label_srch_input = QLineEdit()
		self.label_srch_input.setPlaceholderText("animal") 

		label_srch_btn = QPushButton("Search")
		label_srch_btn.clicked.connect( self.search_by_label_handler )

		input_layout = QHBoxLayout()
		input_layout.addWidget( QLabel("Label pattern: ") )
		input_layout.addWidget( self.label_srch_input )
		input_layout.addWidget( label_srch_btn )

		layout = QVBoxLayout()
		layout.addLayout( input_layout )
		layout.addWidget( self.label_tbl )

		self.label_search_tab.setLayout( layout )

	# --- Logic Handlers ---

	def refresh_image_list(self):
		""" Refreshes image list upon search/upload/delete """
		try:
			uid = self.user_id_input.text().strip() or None
			images = photoapp.get_images(userid=uid)
			
			self.img_tbl.setRowCount(0)
			for row_idx, (assetid, userid, localname, bucketkey) in enumerate(images):
				self.img_tbl.insertRow(row_idx)
				self.img_tbl.setItem(row_idx, 0, QTableWidgetItem(str(assetid)))
				self.img_tbl.setItem(row_idx, 1, QTableWidgetItem(str(userid)))
				self.img_tbl.setItem(row_idx, 2, QTableWidgetItem(localname))
				self.img_tbl.setItem(row_idx, 3, QTableWidgetItem(bucketkey))

				# Button to see labels for this specific image
				labels_btn = QPushButton("View all labels")
				labels_btn.clicked.connect(
					lambda _ch, aid=assetid: self.show_labels_popup(aid)
				)
				self.img_tbl.setCellWidget(row_idx, 4, labels_btn)

				# Download Button for each row
				dl_btn = QPushButton("Download")
				dl_btn.clicked.connect(
					lambda _ch, aid=assetid: self.download_and_display(aid)
				)
				self.img_tbl.setCellWidget(row_idx, 5, dl_btn)
		except Exception as e:
			QMessageBox.critical(self, "Error", f"Failed to fetch images: {str(e)}")

	def upload_handler(self):
		uid = self.upload_user_id.text().strip()
		if not uid:
			QMessageBox.warning(self, "Input error", "Please enter a user ID.")
			return

		file_path, _ = QFileDialog.getOpenFileName(
			self,
			"Open image",
			"",
			"Images (*.png *.jpg *.jpeg)"
		 )
		if file_path:
			try:
				asset_id = photoapp.post_image(uid, file_path)
				QMessageBox.information(self, 
					"Success",
					f"Uploaded! Asset ID: {asset_id}"
				)
				self.refresh_image_list()
			except Exception as e:
				QMessageBox.critical(self, "Upload failed", str(e))

	def search_by_label_handler(self):
		"""Logic for get_images_with_label(label)"""
		label_text = self.label_srch_input.text().strip()
		if not label_text:
			QMessageBox.warning(self, "Input error", "Please enter a label to search.")
			return

		try:
			results = photoapp.get_images_with_label( label_text )
			self.label_tbl.setRowCount(0)
			for row_idx, (assetid, label, confidence) in enumerate(results):
				self.label_tbl.insertRow(row_idx)
				self.label_tbl.setItem(row_idx, 0, QTableWidgetItem( str(assetid)) )
				self.label_tbl.setItem(row_idx, 1, QTableWidgetItem(f"{label}"))
				self.label_tbl.setItem(row_idx, 2, QTableWidgetItem(f"{confidence:.1f}%"))

				# Button to see ALL labels for this specific image
				all_labels_btn = QPushButton("View all labels")
				all_labels_btn.clicked.connect(
					lambda _ch, aid=assetid: self.show_labels_popup( aid )
				)
				self.label_tbl.setCellWidget(row_idx, 3, all_labels_btn)

				# Button to download
				dl_btn = QPushButton("Download")
				dl_btn.clicked.connect(
					lambda _ch, aid=assetid: self.download_and_display( aid )
				)
				self.label_tbl.setCellWidget(row_idx, 4, dl_btn)
		except Exception as e:
			QMessageBox.critical(self, "Search error", str(e))

	def delete_handler( self ):
		try:
			success = photoapp.delete_images()
			if success:
				QMessageBox.information(
					self,
					"Success",
					"All images deleted"
				)
				self.refresh_image_list()
		except Exception as e:
			QMessageBox.critical( self, "Delete failed", str(e) )

	def download_and_display(self, assetid):
		local_name, ok = QInputDialog.getText(
			self,
			"Save as",
			"Enter new local filename (leave blank for remote filename): "
		)

		if ok:
			local_name = local_name.strip() or None

			try:
				# Download file
				filename = photoapp.get_image(
					assetid, local_filename=local_name
				) 
			
				# Show in a popup window
				QMessageBox.information(
					self,
					"Success",
					f"Image downloaded to {filename}"
				)
				self.viewer = ImagePopup( filename )
				self.viewer.show()
			except Exception as e:
				QMessageBox.critical(
					self,
					"Error",
					f"Could not download image: {str(e)}"
			)

	def show_labels_popup(self, assetid):
		"""Logic for get_image_labels(assetid)"""
		try:
			labels_data = photoapp.get_image_labels(assetid)
			if not labels_data:
				QMessageBox.information(
					self, 
					"Labels", 
					"No labels found for this image."
				)
				return

			# Format the list of (label, confidence) tuples into a string
			label_str = "\n".join(
				[ f"{label}: {conf:.2f}%" for label, conf in labels_data ]
			)
			QMessageBox.information(
				self,
				f"Labels for asset {assetid}",
				label_str
			)
		except Exception as e:
			QMessageBox.critical(
				self,
				"Error",
				f"Could not retrieve labels: {str(e)}"
			)

if __name__ == "__main__":
	CONFIG = "photoapp-config.ini"
	S3_UNAME = "s3readwrite"
	RDS_UNAME = "photoapp-read-write"

	if not photoapp.initialize( CONFIG, S3_UNAME, RDS_UNAME ):
		print( "Photoapp initialize() failed" )
		sys.exit(2)

	app = QApplication(sys.argv)
	window = PhotoAppGUI()
	window.show()
	sys.exit(app.exec())

