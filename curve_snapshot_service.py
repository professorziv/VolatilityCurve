import os
import json
import time
import xml.etree.ElementTree as ET
from datetime import datetime

try:
    import XHPricingPy as xh
except ImportError as exc:
    raise RuntimeError(
        "Missing dependency XHPricingPy. Please install required runtime packages first."
    ) from exc

from VanillaOption import VanillaOption
from get_option_codes import get_filtered_options


def load_config_from_xml(file_path="config.xml"):
    if not os.path.isabs(file_path):
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), file_path)

    tree = ET.parse(file_path)
    root = tree.getroot()

    ctp_config = root.find("ctp")
    md_front = ctp_config.find("md_front").text
    broker_id = ctp_config.find("broker_id").text
    user_id = ctp_config.find("user_id").text
    password = ctp_config.find("password").text

    db_config_elem = root.find("database")
    db_config = {
        "host": db_config_elem.find("host").text,
        "user": db_config_elem.find("user").text,
        "password": db_config_elem.find("password").text,
        "database": db_config_elem.find("database").text,
    }
    return md_front, broker_id, user_id, password, db_config


def load_curve_save_config(file_path="curve_save_config.json"):
    if not os.path.isabs(file_path):
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), file_path)

    if not os.path.exists(file_path):
        return {
            "scheduled_underlyings": [],
        }

    with open(file_path, "r", encoding="utf-8") as file:
        config = json.load(file)

    return {
        "scheduled_underlyings": config.get("scheduled_underlyings", []),
    }


def create_bs_process(underlying_price, r, q, vol=0.20):
    return xh.FastGeneralizedBlackScholesProcessMaker(
        underlying_price, q, r, vol, xh.Business244
    )


def update_eval_date(selected_date):
    month_map = {
        1: xh.Jan,
        2: xh.Feb,
        3: xh.Mar,
        4: xh.Apr,
        5: xh.May,
        6: xh.Jun,
        7: xh.Jul,
        8: xh.Aug,
        9: xh.Sep,
        10: xh.Oct,
        11: xh.Nov,
        12: xh.Dec,
    }
    xh_date = xh.Date(selected_date.day, month_map[selected_date.month], selected_date.year)
    xh.setEvaluationDate(xh_date)


def format_greek(value):
    if value is None:
        return None
    return float(value)


def build_snapshot_payload(
    product_id,
    underlying_price,
    r,
    q,
    eval_date,
    otm_range_pct,
    curve_mode,
    data,
    source="web_app",
    notes=None,
    captured_at=None,
):
    if captured_at is None:
        captured_at = datetime.now()

    curve_points = [
        {
            "instrument_id": point["InstrumentID"],
            "option_type": point["Type"],
            "strike_price": point["Strike"],
            "expire_date": point["ExpireDate"],
            "side": point["Side"],
            "price": point["Price"],
            "iv": point["IV"],
        }
        for point in data
    ]
    snapshot = {
        "underlying_id": product_id,
        "spot_price": underlying_price,
        "risk_free_rate": r,
        "dividend_yield": q,
        "otm_range_pct": otm_range_pct,
        "curve_mode": curve_mode,
        "evaluation_date": eval_date,
        "captured_at": captured_at,
        "source": source,
        "notes": notes,
    }
    return {"snapshot": snapshot, "curve_points": curve_points}


def filter_otm_curve_data(data, underlying_price):
    return [
        point
        for point in data
        if (
            (point["Type"] == "Call" and point["Strike"] > underlying_price)
            or (point["Type"] == "Put" and point["Strike"] < underlying_price)
        )
    ]


def calculate_curve_data(engine, product_id, r, q, db_config, otm_range_pct, curve_mode):
    engine.subscribe([product_id.encode("utf-8")])

    underlying_price = 0.0
    for _ in range(30):
        quote = engine.get_quote(product_id)
        if quote and quote.get("last", 0) > 0:
            underlying_price = quote["last"]
            break
        time.sleep(0.1)

    if underlying_price <= 0:
        return None, None, f"Could not fetch price for {product_id}. Market may be closed."

    specs = get_filtered_options(
        product_id,
        underlying_price,
        db_config,
        otm_range_pct=otm_range_pct,
    )
    if not specs:
        return None, None, f"No matching options found in the configured strike range for {product_id}."

    inst_ids = [spec["InstrumentID"].encode("utf-8") for spec in specs]
    engine.subscribe(inst_ids)
    time.sleep(1.5)

    bs_process = create_bs_process(underlying_price, r, q)
    data = []

    for spec in specs:
        inst = spec["InstrumentID"]
        quote = engine.get_quote(inst)
        if not quote:
            continue

        opt = VanillaOption(inst, spec["StrikePrice"], spec["ExpireDate"], spec["OptionsType"])
        bid_iv = opt.calculate_implied_vol(quote["bid"], bs_process)
        ask_iv = opt.calculate_implied_vol(quote["ask"], bs_process)
        expire_date = datetime.strptime(spec["ExpireDate"], "%Y%m%d").date()

        if curve_mode == "Bid/Ask":
            if bid_iv is not None:
                bid_greeks = opt.calculate_greeks(underlying_price, r, q, bid_iv)
                data.append(
                    {
                        "InstrumentID": inst,
                        "Strike": spec["StrikePrice"],
                        "Type": spec["OptionsType"],
                        "ExpireDate": expire_date,
                        "IV": bid_iv,
                        "Side": "Bid",
                        "Price": quote["bid"],
                        "VolumeMultiple": spec.get("VolumeMultiple", 1),
                        "Delta": format_greek(bid_greeks["delta"]),
                        "Gamma": format_greek(bid_greeks["gamma"]),
                        "Theta": format_greek(bid_greeks["theta"]),
                        "Vega": format_greek(bid_greeks["vega"]),
                    }
                )
            if ask_iv is not None:
                ask_greeks = opt.calculate_greeks(underlying_price, r, q, ask_iv)
                data.append(
                    {
                        "InstrumentID": inst,
                        "Strike": spec["StrikePrice"],
                        "Type": spec["OptionsType"],
                        "ExpireDate": expire_date,
                        "IV": ask_iv,
                        "Side": "Ask",
                        "Price": quote["ask"],
                        "VolumeMultiple": spec.get("VolumeMultiple", 1),
                        "Delta": format_greek(ask_greeks["delta"]),
                        "Gamma": format_greek(ask_greeks["gamma"]),
                        "Theta": format_greek(ask_greeks["theta"]),
                        "Vega": format_greek(ask_greeks["vega"]),
                    }
                )
        elif bid_iv is not None and ask_iv is not None:
            mid_iv = (bid_iv + ask_iv) / 2
            mid_greeks = opt.calculate_greeks(underlying_price, r, q, mid_iv)
            data.append(
                {
                    "InstrumentID": inst,
                    "Strike": spec["StrikePrice"],
                    "Type": spec["OptionsType"],
                    "ExpireDate": expire_date,
                    "IV": mid_iv,
                    "Side": "Mid",
                    "Price": (quote["bid"] + quote["ask"]) / 2,
                    "VolumeMultiple": spec.get("VolumeMultiple", 1),
                    "Delta": format_greek(mid_greeks["delta"]),
                    "Gamma": format_greek(mid_greeks["gamma"]),
                    "Theta": format_greek(mid_greeks["theta"]),
                    "Vega": format_greek(mid_greeks["vega"]),
                }
            )

    if not data:
        return underlying_price, None, f"No volatility data calculated for {product_id}."

    otm_data = filter_otm_curve_data(data, underlying_price)
    if not otm_data:
        return underlying_price, None, f"No OTM volatility data calculated for {product_id}."

    return underlying_price, otm_data, None
