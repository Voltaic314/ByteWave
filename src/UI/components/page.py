from PyQt5.QtWidgets import QWidget
from PyQt5.QtGui import QPalette, QColor

class BasePage(QWidget):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Set the background color to black
        palette = QPalette()
        black = QColor(30, 30, 30)  # Dark grey/black -- well which is it? 
        palette.setColor(QPalette.Window, black)
        self.setAutoFillBackground(True)
        self.setPalette(palette)
