from PyQt5.QtWidgets import QPushButton
from PyQt5.QtGui import QFontMetrics

class StyledButton(QPushButton):
    def __init__(self, text="", *args, **kwargs):
        super().__init__(text, *args, **kwargs)

        # Apply default styles
        self.setStyleSheet(
            """
            QPushButton {
                background-color: rgb(128, 0, 128);
                color: white;
                border-radius: 5px;
                font-size: 14px;
                padding: 10px 10px;  /* Vertical and Horizontal padding */
            }
            QPushButton:hover {
                background-color: rgb(150, 50, 150);
            }
            QPushButton:pressed {
                background-color: rgb(100, 0, 100);
            }
            """
        )

        # Adjust the button size dynamically
        self.adjust_size_to_text(text)

    def adjust_size_to_text(self, text):
        font_metrics = QFontMetrics(self.font())

        # Calculate text width using tight bounding rect
        text_width = font_metrics.tightBoundingRect(text).width()
        padding = 40  # Extra padding for both sides
        self.setFixedWidth(text_width + padding)

        # Ensure height includes descenders
        line_height = font_metrics.height() + 25  # Base height + vertical padding
        self.setFixedHeight(line_height)

        # Set internal margins to avoid clipping
        self.setContentsMargins(10, 10, 10, 10)
