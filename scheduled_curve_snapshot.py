import argparse
from datetime import datetime, time as dt_time

from curve_snapshot_service import (
    build_snapshot_payload,
    calculate_curve_data,
    load_curve_save_config,
    load_config_from_xml,
    update_eval_date,
)
from iv_curve_storage import ensure_tables, replace_curve_snapshot
from quote_engine import CTPMarketEngine


SCHEDULE_SLOTS = (
    dt_time(9, 30),
    dt_time(13, 45),
    dt_time(22, 0),
)


def parse_args():
    parser = argparse.ArgumentParser(description="Capture and save scheduled IV curve snapshots.")
    parser.add_argument("--config", default="config.xml", help="Path to XML config file.")
    parser.add_argument(
        "--save-config",
        default="curve_save_config.json",
        help="Path to scheduled/web save target config file.",
    )
    parser.add_argument("--products", help="Comma-separated underlying IDs. Overrides configured scheduled targets.")
    parser.add_argument("--risk-free", type=float, default=0.05, help="Risk free rate.")
    parser.add_argument("--dividend", type=float, default=0.05, help="Dividend yield.")
    parser.add_argument("--otm-range-pct", type=float, default=0.10, help="OTM range as decimal.")
    parser.add_argument(
        "--slot",
        choices=["09:30", "13:45", "22:00"],
        help="Scheduled slot label. If omitted, infer from current local time.",
    )
    return parser.parse_args()


def resolve_products(raw_products, save_config):
    if raw_products:
        return [item.strip() for item in raw_products.split(",") if item.strip()]
    return save_config["scheduled_underlyings"]


def infer_slot_label(now):
    current_minutes = now.hour * 60 + now.minute
    best_slot = None
    best_distance = None

    for slot in SCHEDULE_SLOTS:
        slot_minutes = slot.hour * 60 + slot.minute
        distance = abs(current_minutes - slot_minutes)
        if best_distance is None or distance < best_distance:
            best_distance = distance
            best_slot = slot

    if best_distance is not None and best_distance <= 30:
        return best_slot.strftime("%H:%M")
    return now.strftime("%H:%M")


def main():
    args = parse_args()
    now = datetime.now()
    slot_label = args.slot or infer_slot_label(now)
    eval_date = now.date()
    curve_mode = "Mid"

    md_front, broker_id, user_id, password, db_config = load_config_from_xml(args.config)
    save_config = load_curve_save_config(args.save_config)
    ensure_tables(db_config)
    update_eval_date(eval_date)

    products = resolve_products(args.products, save_config)
    if not products:
        raise RuntimeError("No scheduled underlyings configured for snapshot capture.")

    engine = CTPMarketEngine(md_front, broker_id, user_id, password)
    engine.start()

    saved_snapshot_ids = []
    try:
        for product_id in products:
            print(f"Recalculating {product_id} for scheduled slot {slot_label}...")
            underlying_price, data, error_message = calculate_curve_data(
                engine,
                product_id,
                args.risk_free,
                args.dividend,
                db_config,
                args.otm_range_pct,
                curve_mode,
            )
            if error_message:
                print(f"Skipped {product_id}: {error_message}")
                continue

            payload = build_snapshot_payload(
                product_id,
                underlying_price,
                args.risk_free,
                args.dividend,
                eval_date,
                args.otm_range_pct,
                curve_mode,
                data,
                source="scheduler",
                notes=f"scheduled@{slot_label}",
                captured_at=now,
            )
            snapshot_id = replace_curve_snapshot(
                db_config,
                payload["snapshot"],
                payload["curve_points"],
            )
            saved_snapshot_ids.append((product_id, snapshot_id))
            print(f"Saved {product_id} snapshot #{snapshot_id}")
    finally:
        engine.stop()

    print("Completed scheduled curve capture.")
    for product_id, snapshot_id in saved_snapshot_ids:
        print(f"{product_id}: {snapshot_id}")


if __name__ == "__main__":
    main()
