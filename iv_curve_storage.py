from datetime import datetime, timedelta

import mysql.connector
import pandas as pd


CREATE_SNAPSHOT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS iv_curve_snapshot (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    underlying_id VARCHAR(32) NOT NULL,
    spot_price DOUBLE NOT NULL,
    risk_free_rate DOUBLE NOT NULL,
    dividend_yield DOUBLE NOT NULL,
    otm_range_pct DOUBLE NOT NULL,
    curve_mode VARCHAR(16) NOT NULL,
    evaluation_date DATE NOT NULL,
    captured_at DATETIME NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT 'web_app',
    notes VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_snapshot_underlying_time (underlying_id, captured_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


CREATE_POINT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS iv_curve_point (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    snapshot_id BIGINT NOT NULL,
    instrument_id VARCHAR(32) NOT NULL,
    option_type VARCHAR(8) NOT NULL,
    strike_price DOUBLE NOT NULL,
    expire_date DATE NOT NULL,
    side VARCHAR(16) NOT NULL,
    price DOUBLE NOT NULL,
    iv DOUBLE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_iv_curve_point_snapshot
        FOREIGN KEY (snapshot_id) REFERENCES iv_curve_snapshot(id)
        ON DELETE CASCADE,
    UNIQUE KEY uq_snapshot_instrument_side (snapshot_id, instrument_id, side),
    KEY idx_point_snapshot_strike (snapshot_id, strike_price, side),
    KEY idx_point_snapshot_expiry (snapshot_id, expire_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


def get_connection(db_config):
    return mysql.connector.connect(**db_config)


def ensure_tables(db_config):
    conn = get_connection(db_config)
    cursor = conn.cursor()
    try:
        cursor.execute(CREATE_SNAPSHOT_TABLE_SQL)
        cursor.execute(CREATE_POINT_TABLE_SQL)
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def save_curve_snapshot(db_config, snapshot, curve_points):
    conn = get_connection(db_config)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO iv_curve_snapshot (
                underlying_id,
                spot_price,
                risk_free_rate,
                dividend_yield,
                otm_range_pct,
                curve_mode,
                evaluation_date,
                captured_at,
                source,
                notes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                snapshot["underlying_id"],
                snapshot["spot_price"],
                snapshot["risk_free_rate"],
                snapshot["dividend_yield"],
                snapshot["otm_range_pct"],
                snapshot["curve_mode"],
                snapshot["evaluation_date"],
                snapshot["captured_at"],
                snapshot.get("source", "web_app"),
                snapshot.get("notes"),
            ),
        )
        snapshot_id = cursor.lastrowid

        cursor.executemany(
            """
            INSERT INTO iv_curve_point (
                snapshot_id,
                instrument_id,
                option_type,
                strike_price,
                expire_date,
                side,
                price,
                iv
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    snapshot_id,
                    point["instrument_id"],
                    point["option_type"],
                    point["strike_price"],
                    point["expire_date"],
                    point["side"],
                    point["price"],
                    point["iv"],
                )
                for point in curve_points
            ],
        )
        conn.commit()
        return snapshot_id
    finally:
        cursor.close()
        conn.close()


def load_recent_curve_points(db_config, underlying_id, curve_mode, days):
    since_time = datetime.now() - timedelta(days=days)
    conn = get_connection(db_config)
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT
                s.id AS snapshot_id,
                s.captured_at,
                p.instrument_id,
                p.option_type,
                p.strike_price,
                p.expire_date,
                p.side,
                p.price,
                p.iv
            FROM iv_curve_snapshot s
            JOIN iv_curve_point p ON p.snapshot_id = s.id
            WHERE s.underlying_id = %s
              AND s.curve_mode = %s
              AND s.captured_at >= %s
            ORDER BY s.captured_at, p.strike_price, p.side
            """,
            (underlying_id, curve_mode, since_time),
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    if not rows:
        return pd.DataFrame()

    history_df = pd.DataFrame(rows)
    history_df["captured_at"] = pd.to_datetime(history_df["captured_at"])
    history_df["CurveLabel"] = history_df["captured_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
    history_df["CurveSource"] = "History"
    return history_df
