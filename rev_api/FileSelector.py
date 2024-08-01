import tkinter as tk
from tkinter import filedialog

class JsonFileSelector:
    def __init__(self):
        self.open_file_path = None
        self.save_file_path = None
        self.fileypes = (
            ("json files", "*.json"),
            ("all files", "*.*")
        )

    def open_file(self):
        root = tk.Tk()
        root.withdraw()
        self.open_file_path = filedialog.askopenfilename(
            title="Select a file",
            filetypes=self.fileypes
        )
        root.destroy()
        return self.open_file_path

    def save_file(self):
        root = tk.Tk()
        root.withdraw()
        self.save_file_path = filedialog.asksaveasfilename(
            title="Save as",
            filetypes=self.fileypes
        )
        root.destroy()
        return self.save_file_path

    def reset(self):
        self.open_file_path = None
        self.save_file_path = None
        return None
