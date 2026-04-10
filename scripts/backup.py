#!/usr/bin/env python
"""Daily database backup script."""

import logging
import shutil
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "db" / "untatiz_db.db"
BACKUP_DIR = BASE_DIR / "backup"
RETENTION_DAYS = 7

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


def backup_database():
    """Create dated backup and cleanup old backups.
    
    Returns:
        Path: Path to backup file created
    """
    BACKUP_DIR.mkdir(exist_ok=True)
    
    # Create backup with date
    date_str = datetime.now().strftime("%Y%m%d")
    backup_path = BACKUP_DIR / f"untatiz_db_{date_str}.db"
    
    # Don't overwrite if already exists today
    if backup_path.exists():
        logger.info(f"Backup already exists for today: {backup_path}")
        return backup_path
    
    # Copy database file with metadata preservation
    shutil.copy2(DB_PATH, backup_path)
    logger.info(f"Backup created: {backup_path}")
    
    # Cleanup old backups (keep last RETENTION_DAYS)
    backups = sorted(BACKUP_DIR.glob("untatiz_db_*.db"))
    if len(backups) > RETENTION_DAYS:
        for old_backup in backups[:-RETENTION_DAYS]:
            old_backup.unlink()
            logger.info(f"Deleted old backup: {old_backup}")
    
    return backup_path


if __name__ == "__main__":
    path = backup_database()
    print(f"Backup created: {path}")
