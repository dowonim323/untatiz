"""News blueprint - news page routes."""

from __future__ import annotations

import json
from flask import Blueprint, render_template

from app.config.settings import load_config

bp = Blueprint('news', __name__)


@bp.route('/category/news')
def news_view():
    """News page."""
    news_file_path = load_config().news_dir / 'news.json'
    
    try:
        with open(news_file_path, 'r', encoding='utf-8') as f:
            news_data = json.load(f)
        
        # 날짜 기준으로 정렬된 뉴스 아이템 리스트 생성 (최신순)
        news_items = sorted(
            [(date, news) for date, news in news_data.items()],
            key=lambda x: x[0],
            reverse=True
        )
        
        return render_template('news.html',
                             category='news',
                             news_items=news_items,
                             news_data=news_data)
    except Exception as e:
        return f"뉴스 데이터를 불러오는 중 오류가 발생했습니다: {str(e)}", 500
