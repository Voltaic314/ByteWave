import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QStackedWidget
from PyQt5.QtGui import QIcon
from pages.main import MainPage
from pages.settings import SettingsPage


def windows_taskbar_icon_fix():
    import ctypes
    myappid = u'mycompany.myproduct.subproduct.version' # arbitrary string
    ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        # Set window title and default size
        self.setWindowTitle("Data Migration Tool")
        self.setGeometry(100, 100, 640, 480)  # Default resolution: 640x480

        # Set the window icon (affects title bar and taskbar)
        app.setWindowIcon(QIcon("D:\\golde\\Documents\\GitHub\\Data_Migration_Tool\\UI\\assets\\DMT - Cloud Only Transparent - Less Visual Padding.png"))

        # Set up stacked widget for page navigation
        self.stacked_widget = QStackedWidget()
        self.setCentralWidget(self.stacked_widget)

        # Add pages with a reference to MainWindow
        self.main_page = MainPage(self)
        self.settings_page = SettingsPage(self)

        self.stacked_widget.addWidget(self.main_page)
        self.stacked_widget.addWidget(self.settings_page)

        # Set initial page
        self.stacked_widget.setCurrentWidget(self.main_page)

if __name__ == "__main__":
    windows_taskbar_icon_fix()
    app = QApplication(sys.argv)

    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
