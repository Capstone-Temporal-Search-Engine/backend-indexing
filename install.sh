#!/bin/bash

# Upgrade pip
python -m pip install --upgrade pip

# Install all dependencies from requirements.txt
pip install -r requirements.txt

# Download the spaCy model
python -m spacy download en_core_web_sm