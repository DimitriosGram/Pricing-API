# Makefile for running pytest

# Define the Python interpreter (change as needed)
PYTHON = python

# Define the directory containing your tests
TEST_DIR = tests

# Default target: Run pytest
test:
	$(PYTHON) -m pytest $(TEST_DIR)

# Install dependencies (if you have a requirements.txt file)
install:
	pip install -r requirements.txt

# Clean up any generated files (optional)
clean:
	# Add commands to clean up here

# Help target: Display usage information
help:
	@echo "Usage:"
	@echo "  make test         Run pytest"
	@echo "  make install      Install dependencies from requirements.txt"
	@echo "  make clean        Clean up generated files"
	@echo "  make help         Display this help message"

# By default, display the help message
.DEFAULT_GOAL := help
