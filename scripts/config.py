import os

# Determine the absolute path of the project root
# This file is in: project3/scripts/config.py
# So we go up 2 levels to get to project3/
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

# Data Paths (Absolute paths are safer)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
BOOKS_DIR = os.path.join(DATA_DIR, "books")
METADATA_FILE = os.path.join(DATA_DIR, "metadata.json")

# Gutendex API Configuration
GUTENBERG_API = "http://gutendex.com/books/"

# Project Constraints
TARGET_BOOK_COUNT = 1670  # Requirement: > 1664 books
MIN_WORD_COUNT = 10000    # Requirement: > 10^4 words