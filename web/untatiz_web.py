#!/usr/bin/env python3
"""Untatiz web application - thin entrypoint.

This module serves as the entry point for the Flask web application.
Uses the application factory pattern from web/app.py.

Usage:
    python web/untatiz_web.py  # Development server
    gunicorn untatiz_web:app   # Production (Gunicorn)
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from web.app import create_app

# Create the application instance
app = create_app()


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5000)
