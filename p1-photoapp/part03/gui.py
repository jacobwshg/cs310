import sys
import os
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QPushButton, QTableWidget, QTableWidgetItem, QLineEdit, 
    QLabel, QFileDialog, QMessageBox, QTabWidget, QHeaderView, QInputDialog
)
from PyQt6.QtGui import QPixmap
from PyQt6.QtCore import Qt

from part02.photoapp import *

class ImageApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("S3 Image & Rekognition Manager")
        self.resize(1000, 600)

        # Main Layout
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        # Tab 1: Image Gallery / Download
        self.gallery_tab = QWidget()
        self.setup_gallery_tab()
        self.tabs.addTab(self.gallery_tab, "Image Gallery")

        # Tab 2: Upload
        self.upload_tab = QWidget()
        self.setup_upload_tab()
        self.tabs.addTab(self.upload_tab, "Upload New Image")

        # Main Layout setup remains the same as previous response...
        # [Assuming setup_gallery_tab and setup_upload_tab are called ]

    def setup_gallery_tab(self):
        layout = QVBoxLayout()
        
        # User ID Filter
        filter_layout = QHBoxLayout()
        self.user_id_input = QLineEdit()
        self.user_id_input.setPlaceholderText("Filter by User ID (leave blank for all)")
        btn_refresh = QPushButton("Refresh List")
        btn_refresh.clicked.connect(self.refresh_image_list)
        
        filter_layout.addWidget(self.user_id_input)
        filter_layout.addWidget(btn_refresh)
        layout.addLayout(filter_layout)

        # Image Table
        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(["Asset ID", "User ID", "Local Name", "S3 Key", "Action"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        layout.addWidget(self.table)

        self.gallery_tab.setLayout(layout)

    def setup_upload_tab(self):
        layout = QVBoxLayout()
        
        self.upload_user_id = QLineEdit()
        self.upload_user_id.setPlaceholderText("Enter User ID for Upload")
        layout.addWidget(QLabel("User ID:"))
        layout.addWidget(self.upload_user_id)

        btn_select_file = QPushButton("Select & Upload Image")
        btn_select_file.clicked.connect(self.upload_handler)
        layout.addWidget(btn_select_file)
        
        layout.addStretch()
        self.upload_tab.setLayout(layout)

    # --- Logic Handlers ---

    def refresh_image_list(self):
        try:
            uid = self.user_id_input.text().strip() or None
            images = get_images(userid=uid) # Your backend call
            
            self.table.setRowCount(0)
            for row_idx, (assetid, userid, localname, bucketkey) in enumerate(images):
                self.table.insertRow(row_idx)
                self.table.setItem(row_idx, 0, QTableWidgetItem(str(assetid)))
                self.table.setItem(row_idx, 1, QTableWidgetItem(str(userid)))
                self.table.setItem(row_idx, 2, QTableWidgetItem(localname))
                self.table.setItem(row_idx, 3, QTableWidgetItem(bucketkey))

                # Download Button for each row
                btn_download = QPushButton("Download & Open")
                btn_download.clicked.connect(lambda ch, aid=assetid: self.download_and_display(aid))
                self.table.setCellWidget(row_idx, 4, btn_download)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to fetch images: {str(e)}")

    def upload_handler(self):
        uid = self.upload_user_id.text().strip()
        if not uid:
            QMessageBox.warning(self, "Input Error", "Please enter a User ID.")
            return

        file_path, _ = QFileDialog.getOpenFileName(self, "Open Image", "", "Images (*.png *.jpg *.jpeg)")
        if file_path:
            try:
                asset_id = post_image(uid, file_path) # Your backend call
                QMessageBox.information(self, "Success", f"Uploaded! Asset ID: {asset_id}")
                self.refresh_image_list()
            except Exception as e:
                QMessageBox.critical(self, "Upload Failed", str(e))

    def download_and_display(self, assetid):
        try:
            # Download file
            saved_path = get_image(assetid) # Your backend call
            
            # Show in a popup window
            self.viewer = ImagePopup(saved_path)
            self.viewer.show()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not download image: {str(e)}")

    def setup_gallery_tab(self):
        layout = QVBoxLayout()
        
        # Search & Filter Row
        search_layout = QHBoxLayout()
        
        self.user_id_input = QLineEdit()
        self.user_id_input.setPlaceholderText("Filter by User ID")
        
        self.label_search_input = QLineEdit()
        self.label_search_input.setPlaceholderText("Search Label (e.g. 'Dog')")
        
        btn_refresh = QPushButton("List All / User")
        btn_refresh.clicked.connect(self.refresh_image_list)
        
        btn_label_search = QPushButton("Search by Label")
        btn_label_search.clicked.connect(self.search_by_label_handler)
        
        search_layout.addWidget(QLabel("User:"))
        search_layout.addWidget(self.user_id_input)
        search_layout.addWidget(btn_refresh)
        search_layout.addSpacing(20)
        search_layout.addWidget(QLabel("Label:"))
        search_layout.addWidget(self.label_search_input)
        search_layout.addWidget(btn_label_search)
        
        layout.addLayout(search_layout)

        # Image Table
        self.table = QTableWidget(0, 6) # Increased to 6 columns
        self.table.setHorizontalHeaderLabels(["Asset ID", "User ID", "Info", "S3 Key", "Labels", "Download"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        layout.addWidget(self.table)

        self.gallery_tab.setLayout(layout)

    # --- New Logic Handlers ---

    def search_by_label_handler(self):
        """Logic for get_images_with_label(label)"""
        label_text = self.label_search_input.text().strip()
        if not label_text:
            QMessageBox.warning(self, "Input Error", "Please enter a label to search.")
            return
            
        try:
            results = get_images_with_label(label_text)
            self.table.setRowCount(0)
            for row_idx, (assetid, label, confidence) in enumerate(results):
                self.table.insertRow(row_idx)
                self.table.setItem(row_idx, 0, QTableWidgetItem(str(assetid)))
                self.table.setItem(row_idx, 1, QTableWidgetItem("N/A")) # get_images_with_label doesn't return UID
                self.table.setItem(row_idx, 2, QTableWidgetItem(f"Match: {label}"))
                self.table.setItem(row_idx, 3, QTableWidgetItem(f"{confidence:.1f}%"))
                
                # Button to see ALL labels for this specific image
                btn_labels = QPushButton("View All Labels")
                btn_labels.clicked.connect(lambda ch, aid=assetid: self.show_labels_popup(aid))
                self.table.setCellWidget(row_idx, 4, btn_labels)
                
                # Button to download
                btn_dl = QPushButton("Download")
                btn_dl.clicked.connect(lambda ch, aid=assetid: self.download_and_display(aid))
                self.table.setCellWidget(row_idx, 5, btn_dl)
        except Exception as e:
            QMessageBox.critical(self, "Search Error", str(e))

    def show_labels_popup(self, assetid):
        """Logic for get_image_labels(assetid)"""
        try:
            labels_data = get_image_labels(assetid)
            if not labels_data:
                QMessageBox.information(self, "Labels", "No labels found for this image.")
                return

            # Format the list of (label, confidence) tuples into a string
            label_str = "\n".join([f"{label}: {conf:.2f}%" for label, conf in labels_data])
            QMessageBox.information(self, f"Labels for Asset {assetid}", label_str)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not retrieve labels: {str(e)}")

class ImagePopup(QWidget):
    """Simple popup window to display the downloaded image."""
    def __init__(self, image_path):
        super().__init__()
        self.setWindowTitle("Image Preview")
        layout = QVBoxLayout()
        label = QLabel()
        pixmap = QPixmap(image_path)
        
        # Scale image to fit reasonable window size while keeping aspect ratio
        label.setPixmap(pixmap.scaled(600, 600, Qt.AspectRatioMode.KeepAspectRatio))
        layout.addWidget(label)
        self.setLayout(layout)

if __name__ == "__main__":
    CONFIG = "photoapp-config.ini"
    S3_UNAME = "s3readwrite"
    RDS_UNAME = "photoapp-read-write"

    if not initialize( CONFIG, S3_UNAME, RDS_UNAME ):
        print( "Photoapp initialize() failed" )
        sys.exit(2)

    app = QApplication(sys.argv)
    window = ImageApp()
    window.show()
    sys.exit(app.exec())

