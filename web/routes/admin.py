"""Admin blueprint - authentication routes."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from flask import Blueprint, request, jsonify, render_template

from web.auth import hash_password, login, logout, check_auth, login_required
from web.utils import get_db, get_all_seasons
from app.core.cache import get_stats, invalidate_all

bp = Blueprint('admin', __name__)

ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'jijaeok')
ADMIN_PASSWORD_HASH = hash_password(ADMIN_PASSWORD)


@bp.route('/admin_login', methods=['POST'])
def admin_login():
    """Admin login endpoint."""
    password = request.form.get('password')
    
    if not password:
        return jsonify({"success": False, "message": "비밀번호를 입력해주세요."})
    
    if login(password, ADMIN_PASSWORD_HASH):
        return jsonify({"success": True, "message": "인증되었습니다."})
    else:
        return jsonify({"success": False, "message": "비밀번호가 일치하지 않습니다."})


@bp.route('/admin_logout', methods=['POST'])
def admin_logout():
    """Admin logout endpoint."""
    logout()
    return jsonify({"success": True, "message": "로그아웃되었습니다."})


@bp.route('/check_auth', methods=['GET'])
def check_auth_endpoint():
    """Check authentication status."""
    return jsonify({"authenticated": check_auth()})


@bp.route('/admin', methods=['GET'])
@login_required
def admin_page():
    """Admin dashboard page."""
    seasons = get_all_seasons()
    return render_template('admin.html', seasons=seasons)


@bp.route('/admin/cache/stats', methods=['GET'])
@login_required
def cache_stats():
    """Get cache statistics."""
    stats = get_stats()
    return jsonify(stats)


@bp.route('/admin/cache/invalidate', methods=['POST'])
@login_required
def invalidate_cache():
    """Invalidate all cache entries."""
    count = invalidate_all()
    return jsonify({
        'success': True,
        'message': f'캐시가 삭제되었습니다. ({count}개 항목)',
        'cleared_count': count
    })


@bp.route('/admin/season/create', methods=['POST'])
@login_required
def create_season():
    """Create a new season."""
    year = request.form.get('year', type=int)
    if not year or year < 2020 or year > 2050:
        return jsonify({'success': False, 'message': '유효한 연도를 입력해주세요.'})
    
    db = get_db()
    existing = db.execute("SELECT id FROM seasons WHERE year = ?", (year,)).fetchone()
    if existing:
        return jsonify({'success': False, 'message': f'{year}시즌이 이미 존재합니다.'})
    
    db.execute("INSERT INTO seasons (year, is_active) VALUES (?, 0)", (year,))
    db.commit()
    
    new_season = db.execute("SELECT id FROM seasons WHERE year = ?", (year,)).fetchone()
    
    prev_config = db.execute("""
        SELECT roster_size, supplemental_bonus, min_pitchers, min_catchers, 
               min_infielders, min_outfielders
        FROM fa_config ORDER BY season_id DESC LIMIT 1
    """).fetchone()
    
    if prev_config:
        db.execute("""
            INSERT INTO fa_config (season_id, roster_size, supplemental_bonus, 
                                   min_pitchers, min_catchers, min_infielders, min_outfielders)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (new_season['id'], prev_config['roster_size'], prev_config['supplemental_bonus'],
              prev_config['min_pitchers'], prev_config['min_catchers'],
              prev_config['min_infielders'], prev_config['min_outfielders']))
        db.commit()
    
    return jsonify({'success': True, 'message': f'{year}시즌이 생성되었습니다.', 'season_id': new_season['id']})


@bp.route('/admin/season/<int:season_id>/activate', methods=['POST'])
@login_required
def activate_season(season_id):
    """Activate a season (deactivate others)."""
    db = get_db()
    db.execute("UPDATE seasons SET is_active = 0")
    db.execute("UPDATE seasons SET is_active = 1 WHERE id = ?", (season_id,))
    db.commit()
    return jsonify({'success': True, 'message': '시즌이 활성화되었습니다.'})


@bp.route('/admin/draft/upload', methods=['POST'])
@login_required
def upload_draft():
    """Upload and import draft JSON or CSV file."""
    file = request.files.get('draft_file')
    if not file or not file.filename:
        return jsonify({'success': False, 'message': 'JSON 또는 CSV 파일을 업로드해주세요.'})
    
    filename = file.filename.lower()
    if not (filename.endswith('.json') or filename.endswith('.csv')):
        return jsonify({'success': False, 'message': 'JSON 또는 CSV 파일만 업로드 가능합니다.'})
    
    try:
        suffix = '.csv' if filename.endswith('.csv') else '.json'
        with tempfile.NamedTemporaryFile(mode='w', suffix=suffix, delete=False) as f:
            content = file.read().decode('utf-8')
            f.write(content)
            temp_path = Path(f.name)
        
        from app.core.draft_loader import load_draft_from_json, load_draft_from_csv
        
        if suffix == '.csv':
            result = load_draft_from_csv(temp_path, force=True)
        else:
            result = load_draft_from_json(temp_path, force=True)
        
        temp_path.unlink()
        
        if result.success:
            return jsonify({
                'success': True,
                'message': f'드래프트 업로드 완료: {result.inserted_count}명 등록',
                'inserted': result.inserted_count,
                'skipped': result.skipped_count,
                'warnings': result.warnings[:5] if result.warnings else []
            })
        else:
            return jsonify({
                'success': False,
                'message': result.errors[0] if result.errors else '알 수 없는 오류',
                'errors': result.errors
            })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
