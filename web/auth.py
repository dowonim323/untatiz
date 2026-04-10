"""Authentication utilities for web application."""

from __future__ import annotations

import hashlib
import functools
from flask import session, jsonify, request, current_app


def hash_password(password: str) -> str:
    """Hash a password using SHA256.
    
    Args:
        password: Plain text password
        
    Returns:
        str: Hashed password
    """
    return hashlib.sha256(password.encode()).hexdigest()


def login_required(f):
    """Decorator to require authentication for a route.
    
    Returns 401 if user is not authenticated.
    """
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        if 'authenticated' not in session or not session['authenticated']:
            return jsonify({"success": False, "message": "인증이 필요합니다."}), 401
        return f(*args, **kwargs)
    return decorated_function


def check_auth() -> bool:
    """Check if the current user is authenticated.
    
    Returns:
        bool: True if authenticated
    """
    return session.get('authenticated', False)


def login(password: str, admin_password_hash: str) -> bool:
    """Attempt to log in with the given password.
    
    Args:
        password: Plain text password to check
        admin_password_hash: Expected hashed password
        
    Returns:
        bool: True if login successful
    """
    if hash_password(password) == admin_password_hash:
        session['authenticated'] = True
        session.permanent = True
        return True
    return False


def logout():
    """Log out the current user."""
    session.pop('authenticated', None)
