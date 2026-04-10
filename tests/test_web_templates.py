from pathlib import Path


TEMPLATES_DIR = Path(__file__).resolve().parents[1] / "web" / "templates"


def test_layout_template_includes_mobile_viewport_and_menu_hooks():
    content = (TEMPLATES_DIR / "layout.html").read_text(encoding="utf-8")

    assert '<meta name="viewport" content="width=device-width, initial-scale=1">' in content
    assert 'id="mobileMenu"' in content
    assert 'id="overlay"' in content
    assert "function toggleMobileMenu" in content
    assert "mobile-menu-open" in content


def test_layout_template_keeps_mobile_hamburger_rule_after_default_hidden_rule():
    content = (TEMPLATES_DIR / "layout.html").read_text(encoding="utf-8")

    hidden_rule = """.hamburger {
            display: none;"""
    mobile_rule = """@media (max-width: 800px) {
            body {
                padding: 12px;
            }

            .header-container {
                height: auto; /* 높이 자동 조정 */
                flex-direction: column; /* 세로 방향으로 배치 */
                align-items: flex-start;
                gap: 10px;
                padding: 12px 56px 12px 12px;
                margin-bottom: 12px;
            }
            
            .title {
                width: 100%;
                justify-content: flex-start;
                margin-bottom: 0;
            }

            .title-text {
                margin-right: 0;
                font-size: 24px;
            }
            
            .nav {
                display: none; /* 모바일에서 기본 네비 숨김 */
            }
            
            .hamburger {
                display: flex;"""

    assert hidden_rule in content
    assert mobile_rule in content
    assert content.index(hidden_rule) < content.index(mobile_rule)


def test_mobile_problem_pages_include_scroll_or_stack_hooks():
    expected_hooks = {
        "league.html": "table-scroll",
        "team_info.html": "table-period-",
        "gboat.html": "table-scroll",
        "roster.html": "transaction-table",
    }

    for template_name, hook in expected_hooks.items():
        content = (TEMPLATES_DIR / template_name).read_text(encoding="utf-8")
        assert hook in content


def test_team_info_mobile_does_not_force_inner_vertical_scroll():
    content = (TEMPLATES_DIR / "team_info.html").read_text(encoding="utf-8")

    assert "overflow-y: visible !important;" in content
    assert "max-height: none !important;" in content
