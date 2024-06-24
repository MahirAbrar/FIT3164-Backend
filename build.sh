#!/bin/bash
# Force use of Python 3.9
export PATH="/opt/python3.9/bin:$PATH"
python3.9 -m ensurepip
python3.9 -m pip install --upgrade pip
python3.9 -m pip install setuptools wheel
python3.9 -m pip install -r requirements.txt