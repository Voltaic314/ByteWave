from components.page import BasePage
from components.styled_button import StyledButton
from PyQt5.QtWidgets import QVBoxLayout, QHBoxLayout
from PyQt5.QtCore import Qt

class SettingsPage(BasePage):
    def __init__(self, main_window):
        super().__init__(main_window)
        self.main_window = main_window

        # Create a layout to center the button
        self.outer_layout = QVBoxLayout(self)
        self.inner_layout = QHBoxLayout()

        # Add a button
        self.main_button = StyledButton("Back to Main")
        self.main_button.clicked.connect(self.go_to_main)

        # Add the button to the inner layout and center it
        self.inner_layout.addWidget(self.main_button)
        self.inner_layout.setAlignment(Qt.AlignCenter)

        # Add the inner layout to the outer layout and center it vertically
        self.outer_layout.addLayout(self.inner_layout)
        self.outer_layout.setAlignment(Qt.AlignCenter)

    def go_to_main(self):
        # Switch back to the main page
        self.main_window.stacked_widget.setCurrentWidget(self.main_window.main_page)
