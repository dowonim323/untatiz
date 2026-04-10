"""Roster blueprint - roster and transaction routes."""

from __future__ import annotations

from flask import Blueprint, jsonify, render_template, request

from app.core.cache import invalidate_after_update
from app.services.transaction_backfill import backfill_transaction_history
from web.auth import login_required
from web.services.roster_service import (
    get_draft_data,
    get_roster_seed,
    get_transaction_data,
    validate_transaction_save,
)
from web.utils import get_db, get_selected_season, get_team_order

bp = Blueprint('roster', __name__)


def _normalize_team_id(team_value: str | None) -> str | None:
    if team_value is None:
        return None

    normalized = team_value.strip()
    if normalized.startswith('팀 '):
        normalized = normalized[2:].strip()

    return None if normalized == '퐈' else normalized


def _get_active_season_id(db) -> int:
    season_row = db.execute("SELECT id FROM seasons WHERE is_active = 1 LIMIT 1").fetchone()
    return season_row['id'] if season_row else 1


def _resolve_transaction_season_id(
    db,
    transaction_id: str,
    fallback_season_id: int | None = None,
) -> int:
    if fallback_season_id is not None:
        return fallback_season_id

    transaction_row = db.execute(
        "SELECT season_id FROM transactions WHERE id = ?",
        (transaction_id,),
    ).fetchone()
    if transaction_row:
        return transaction_row['season_id']

    return _get_active_season_id(db)


def _rebuild_player_roster(db, player_id: str, season_id: int) -> None:
    seed = get_roster_seed(db, player_id, season_id)
    transactions = db.execute(
        """SELECT from_team_id, to_team_id, transaction_date
           FROM transactions
           WHERE player_id = ? AND season_id = ?
           ORDER BY transaction_date, id""",
        (player_id, season_id),
    ).fetchall()

    db.execute(
        "DELETE FROM roster WHERE player_id = ? AND season_id = ?",
        (player_id, season_id),
    )

    if seed:
        db.execute(
            "INSERT INTO roster (team_id, player_id, season_id, joined_date) VALUES (?, ?, ?, ?)",
            (seed[0], player_id, season_id, seed[1]),
        )

    for transaction in transactions:
        from_team = transaction['from_team_id']
        to_team = transaction['to_team_id']
        transaction_date = transaction['transaction_date']

        if from_team:
            db.execute(
                """UPDATE roster
                   SET left_date = ?
                   WHERE team_id = ? AND player_id = ? AND season_id = ? AND left_date IS NULL""",
                (transaction_date, from_team, player_id, season_id),
            )

        if to_team:
            db.execute(
                """INSERT OR IGNORE INTO roster (team_id, player_id, season_id, joined_date)
                   VALUES (?, ?, ?, ?)""",
                (to_team, player_id, season_id, transaction_date),
            )


@bp.route('/category/roster')
def roster_view():
    """Roster page."""
    db = get_db()
    season_id, season_year = get_selected_season(request.args)

    sub_category = request.args.get('sub', 'draft')

    if sub_category == 'draft':
        rows, column_order = get_draft_data(db, season_id=season_id)

        return render_template('roster.html',
                             category='roster',
                             sub_category=sub_category,
                             rows=rows,
                             columns=column_order)
    else:  # transaction
        rows = get_transaction_data(db, season_id=season_id, season_year=season_year)
        team_order = get_team_order(season_id)

        return render_template('roster.html',
                             category='roster',
                             sub_category=sub_category,
                             rows=rows,
                             team_order=team_order)


@bp.route('/add_transaction', methods=['POST'])
@login_required
def add_transaction():
    """Add transaction endpoint."""
    try:
        date_value = request.form.get('date')
        name_value = request.form.get('name')
        player_id_value = request.form.get('player_id')
        old_team_value = request.form.get('old_team')
        new_team_value = request.form.get('new_team')
        season_id = request.form.get('season_id', type=int)

        if (
            date_value is None
            or name_value is None
            or player_id_value is None
            or old_team_value is None
            or new_team_value is None
        ):
            return jsonify({"success": False, "message": "모든 필드를 입력해주세요."})

        date = date_value.replace('/', '-')
        name = name_value
        player_id = player_id_value
        old_team = old_team_value
        new_team = new_team_value

        db = get_db()
        if season_id is None:
            season_id = _get_active_season_id(db)

        cursor = db.execute(
            """SELECT war FROM war_daily
               WHERE player_id = ? AND season_id = ? AND date < ?
               ORDER BY date DESC LIMIT 1""",
            (player_id, season_id, date)
        )
        war_row = cursor.fetchone()
        war_float = war_row['war'] if war_row else 0.0

        from_team = _normalize_team_id(old_team)
        to_team = _normalize_team_id(new_team)
        validation_error = validate_transaction_save(
            db,
            player_id=player_id,
            season_id=season_id,
            transaction_date=date,
            from_team_id=from_team,
            to_team_id=to_team,
        )
        if validation_error:
            return jsonify({"success": False, "message": validation_error})

        db.execute(
            """INSERT INTO transactions
               (
                   player_id, season_id, from_team_id,
                   to_team_id, transaction_date, war_at_transaction
               )
               VALUES (?, ?, ?, ?, ?, ?)""",
            (player_id, season_id, from_team, to_team, date, war_float)
        )
        _rebuild_player_roster(db, player_id, season_id)
        backfill_transaction_history(db, season_id, date)
        db.commit()
        invalidate_after_update()

        return jsonify({
            "success": True,
            "message": f"로스터 변동 정보가 추가되었습니다. (WAR: {war_float:.2f})",
        })
    except Exception as e:
        return jsonify({"success": False, "message": f"오류가 발생했습니다: {str(e)}"})


@bp.route('/delete_transaction', methods=['POST'])
@login_required
def delete_transaction():
    """Delete transaction endpoint."""
    try:
        transaction_id = request.form.get('transaction_id')

        if not transaction_id:
            return jsonify({"success": False, "message": "삭제할 항목을 선택해주세요."})

        db = get_db()
        transaction_row = db.execute(
            "SELECT player_id, season_id, transaction_date FROM transactions WHERE id = ?",
            (transaction_id,),
        ).fetchone()
        if not transaction_row:
            return jsonify({"success": False, "message": "해당 항목을 찾을 수 없습니다."})

        db.execute("DELETE FROM transactions WHERE id = ?", (transaction_id,))
        _rebuild_player_roster(db, transaction_row['player_id'], transaction_row['season_id'])
        backfill_transaction_history(
            db,
            transaction_row['season_id'],
            transaction_row['transaction_date'],
        )
        db.commit()
        invalidate_after_update()

        return jsonify({"success": True, "message": "로스터 변동 정보가 삭제되었습니다."})
    except Exception as e:
        return jsonify({"success": False, "message": f"오류가 발생했습니다: {str(e)}"})


@bp.route('/get_transaction', methods=['POST'])
@login_required
def get_transaction():
    """Get transaction for editing."""
    try:
        transaction_id = request.form.get('transaction_id')

        if not transaction_id:
            return jsonify({"success": False, "message": "수정할 항목을 선택해주세요."})

        db = get_db()
        cursor = db.execute(
            """SELECT
                   t.id, t.transaction_date, p.name, t.player_id,
                   t.from_team_id, t.to_team_id, t.war_at_transaction
               FROM transactions t
               LEFT JOIN players p ON t.player_id = p.id
               WHERE t.id = ?""",
            (transaction_id,)
        )
        row = cursor.fetchone()

        if not row:
            return jsonify({"success": False, "message": "해당 항목을 찾을 수 없습니다."})

        return jsonify({
            "success": True,
            "data": {
                "rowid": row['id'],
                "date": row['transaction_date'],
                "name": row['name'] or '',
                "id": row['player_id'],
                "old_team": row['from_team_id'] or '퐈',
                "new_team": row['to_team_id'] or '퐈',
                "war": row['war_at_transaction']
            }
        })
    except Exception as e:
        return jsonify({"success": False, "message": f"오류가 발생했습니다: {str(e)}"})


@bp.route('/update_transaction', methods=['POST'])
@login_required
def update_transaction():
    """Update transaction endpoint."""
    try:
        transaction_id_value = request.form.get('transaction_id')
        date_value = request.form.get('date')
        name_value = request.form.get('name')
        player_id_value = request.form.get('player_id')
        old_team_value = request.form.get('old_team')
        new_team_value = request.form.get('new_team')

        if (
            transaction_id_value is None
            or date_value is None
            or name_value is None
            or player_id_value is None
            or old_team_value is None
            or new_team_value is None
        ):
            return jsonify({"success": False, "message": "모든 필드를 입력해주세요."})

        transaction_id = transaction_id_value
        date = date_value.replace('/', '-')
        name = name_value
        player_id = player_id_value
        old_team = old_team_value
        new_team = new_team_value

        db = get_db()
        previous_row = db.execute(
            "SELECT player_id, season_id, transaction_date FROM transactions WHERE id = ?",
            (transaction_id,),
        ).fetchone()
        if not previous_row:
            return jsonify({"success": False, "message": "해당 항목을 찾을 수 없습니다."})

        season_id = _resolve_transaction_season_id(db, transaction_id)

        cursor = db.execute(
            """SELECT war FROM war_daily
               WHERE player_id = ? AND season_id = ? AND date < ?
               ORDER BY date DESC LIMIT 1""",
            (player_id, season_id, date)
        )
        war_row = cursor.fetchone()
        war_float = war_row['war'] if war_row else 0.0

        from_team = _normalize_team_id(old_team)
        to_team = _normalize_team_id(new_team)
        validation_error = validate_transaction_save(
            db,
            player_id=player_id,
            season_id=season_id,
            transaction_date=date,
            from_team_id=from_team,
            to_team_id=to_team,
            transaction_id=int(transaction_id),
            previous_player_id=previous_row['player_id'],
        )
        if validation_error:
            return jsonify({"success": False, "message": validation_error})

        db.execute(
            """UPDATE transactions
               SET transaction_date = ?, player_id = ?, from_team_id = ?,
                   to_team_id = ?, war_at_transaction = ?
               WHERE id = ?""",
            (date, player_id, from_team, to_team, war_float, transaction_id)
        )
        if previous_row['player_id'] != player_id:
            _rebuild_player_roster(db, previous_row['player_id'], previous_row['season_id'])
        _rebuild_player_roster(db, player_id, season_id)
        backfill_start = min(previous_row['transaction_date'], date)
        backfill_transaction_history(db, season_id, backfill_start)
        db.commit()
        invalidate_after_update()

        return jsonify({
            "success": True,
            "message": f"로스터 변동 정보가 수정되었습니다. (WAR: {war_float:.2f})",
        })
    except Exception as e:
        return jsonify({"success": False, "message": f"오류가 발생했습니다: {str(e)}"})
