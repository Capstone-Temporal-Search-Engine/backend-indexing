#!/bin/bash

# Upgrade pip
python3 -m pip install --upgrade pip

# Install all dependencies from requirements.txt
pip install -r requirements.txt --prefer-binary -v

# Download the spaCy model
python3 -m spacy download en_core_web_sm