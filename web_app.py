import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st

try:
    import XHPricingPy as xh
except (ImportError, RuntimeError):
    st.error("Missing dependencies. Please run: pip install XHPricingPy mysql-connector-python streamlit pandas altair")
    st.stop()

from VanillaOption import VanillaOption
from get_option_codes import get_available_underlyings, get_filtered_options
from iv_curve_storage import ensure_tables, load_recent_curve_points, save_curve_snapshot
from quote_engine import CTPMarketEngine

SCHEDULE_SLOTS = (
    ("09:30", 9 * 60 + 30),
    ("13:45", 13 * 60 + 45),
    ("22:00", 22 * 60),
)


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


@st.cache_resource
def get_market_engine(md_front, broker_id, user_id, password):
    engine = CTPMarketEngine(md_front, broker_id, user_id, password)
    engine.start()
    return engine


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


def build_snapshot_payload(product_id, underlying_price, r, q, eval_date, otm_range_pct, curve_mode, data):
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
        "source": "web_app",
        "notes": None,
    }
    return {"snapshot": snapshot, "curve_points": curve_points}


def format_greek(value):
    if value is None or pd.isna(value):
        return None
    return float(value)


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

    return underlying_price, data, None


def build_chart_dataframe(current_df, current_label, history_df):
    current_chart_df = current_df.copy()
    current_chart_df["CurveLabel"] = current_label
    current_chart_df["CurveSource"] = "Current"

    if history_df.empty:
        return current_chart_df

    history_chart_df = history_df.rename(
        columns={
            "option_type": "Type",
            "strike_price": "Strike",
            "side": "Side",
            "price": "Price",
            "iv": "IV",
        }
    )
    return pd.concat([history_chart_df, current_chart_df], ignore_index=True, sort=False)


def filter_otm_curve_dataframe(df, underlying_price):
    return df[
        ((df["Type"] == "Call") & (df["Strike"] > underlying_price))
        | ((df["Type"] == "Put") & (df["Strike"] < underlying_price))
    ].copy()


def build_position_exposure_dataframe(df, underlying_price):
    exposure_df = df.copy()
    if "OrderLots" not in exposure_df.columns:
        exposure_df["OrderLots"] = 0

    exposure_df["OrderLots"] = pd.to_numeric(exposure_df["OrderLots"], errors="coerce").fillna(0).astype(int)
    exposure_df["DailyMove"] = underlying_price * exposure_df["IV"] / 15.56
    exposure_df["TotalDeltaLots"] = exposure_df["Delta"] * exposure_df["OrderLots"]
    exposure_df["TotalGammaPnl"] = (
        0.5
        * exposure_df["Gamma"]
        * exposure_df["DailyMove"]
        * exposure_df["DailyMove"]
        * exposure_df["OrderLots"]
        * exposure_df["VolumeMultiple"].fillna(1)
    )
    exposure_df["TotalThetaDaily"] = (
        exposure_df["Theta"] * exposure_df["OrderLots"] * exposure_df["VolumeMultiple"].fillna(1) / 244
    )
    exposure_df["TotalVega1Pct"] = (
        exposure_df["Vega"] * 0.01 * exposure_df["OrderLots"] * exposure_df["VolumeMultiple"].fillna(1)
    )
    return exposure_df


def build_t_quote_dataframe(df, underlying_price):
    exposure_df = build_position_exposure_dataframe(df, underlying_price)
    exposure_df = exposure_df.rename(
        columns={
            "TotalDeltaLots": "TotalDelta",
            "TotalGammaPnl": "TotalGamma",
            "TotalThetaDaily": "TotalTheta",
            "TotalVega1Pct": "TotalVega",
        }
    )

    key_columns = ["ExpireDate", "Strike", "Side"]
    value_columns = [
        "Price",
        "IV",
        "Delta",
        "TotalDelta",
        "TotalGamma",
        "TotalTheta",
        "TotalVega",
        "OrderLots",
    ]

    t_quote_df = None
    for option_type in ("Call", "Put"):
        side_df = exposure_df[exposure_df["Type"] == option_type][key_columns + value_columns].copy()
        side_df = side_df.rename(columns={column: f"{option_type} {column}" for column in value_columns})
        if t_quote_df is None:
            t_quote_df = side_df
        else:
            t_quote_df = pd.merge(t_quote_df, side_df, on=key_columns, how="outer")

    if t_quote_df is None:
        return pd.DataFrame()

    call_columns = [f"Call {column}" for column in reversed(value_columns)]
    put_columns = [f"Put {column}" for column in value_columns]
    ordered_columns = ["ExpireDate", "Side"] + call_columns + ["Strike"] + put_columns
    t_quote_df = t_quote_df.reindex(columns=ordered_columns)
    return t_quote_df.sort_values(["ExpireDate", "Strike", "Side"], ignore_index=True)


def apply_t_quote_order_lots(source_df, edited_t_quote_df):
    edited_df = source_df.copy()
    if "OrderLots" not in edited_df.columns:
        edited_df["OrderLots"] = 0

    key_columns = [
        column
        for column in ["ExpireDate", "Strike", "Side"]
        if column in edited_t_quote_df.columns
    ]
    if "Strike" not in key_columns:
        return edited_df

    edited_lots = []
    for option_type in ("Call", "Put"):
        lots_column = f"{option_type} OrderLots"
        if lots_column not in edited_t_quote_df.columns:
            continue

        side_lots = edited_t_quote_df[key_columns + [lots_column]].copy()
        side_lots["Type"] = option_type
        side_lots = side_lots.rename(columns={lots_column: "EditedOrderLots"})
        edited_lots.append(side_lots)

    if not edited_lots:
        return edited_df

    lots_df = pd.concat(edited_lots, ignore_index=True)
    edited_df = pd.merge(edited_df, lots_df, on=key_columns + ["Type"], how="left")
    edited_df["OrderLots"] = edited_df["EditedOrderLots"].combine_first(edited_df["OrderLots"])
    return edited_df.drop(columns=["EditedOrderLots"])


def make_order_lots_key(row):
    return f"{row['ExpireDate']}|{row['Strike']}|{row['Side']}|{row['Type']}"


def build_total_greeks_summary(df, underlying_price):
    exposure_df = build_position_exposure_dataframe(df, underlying_price)
    greek_totals = exposure_df[
        ["TotalDeltaLots", "TotalGammaPnl", "TotalThetaDaily", "TotalVega1Pct"]
    ].sum(numeric_only=True)
    return pd.DataFrame(
        [
            {
                "Total Delta (Hedge)": -greek_totals.get("TotalDeltaLots", 0.0),
                "Total Delta": greek_totals.get("TotalDeltaLots", 0.0),
                "Total Gamma": greek_totals.get("TotalGammaPnl", 0.0),
                "Total Theta": greek_totals.get("TotalThetaDaily", 0.0),
                "Total Vega": greek_totals.get("TotalVega1Pct", 0.0),
            }
        ]
    )


def highlight_derived_columns(styler, derived_columns):
    def apply_style(series):
        if series.name in derived_columns:
            return ["background-color: #fff3cd; color: #5c4400"] * len(series)
        return [""] * len(series)

    return styler.apply(apply_style, axis=0)


def filter_visible_curves(chart_df, history_df, selected_history_labels):
    if not selected_history_labels:
        visible_chart_df = chart_df[chart_df["CurveSource"] == "Current"].copy()
        visible_history_df = history_df.iloc[0:0].copy()
        return visible_chart_df, visible_history_df

    visible_chart_df = chart_df[
        (chart_df["CurveSource"] == "Current") | (chart_df["CurveLabel"].isin(selected_history_labels))
    ].copy()
    visible_history_df = history_df[history_df["CurveLabel"].isin(selected_history_labels)].copy()
    return visible_chart_df, visible_history_df


def infer_active_slot_label(reference_time=None):
    if reference_time is None:
        reference_time = datetime.now()

    current_minutes = reference_time.hour * 60 + reference_time.minute
    return min(SCHEDULE_SLOTS, key=lambda item: abs(current_minutes - item[1]))[0]


def resolve_default_history_labels(history_df, reference_time=None):
    if history_df.empty:
        return []

    slot_label = infer_active_slot_label(reference_time)
    notes_series = history_df.get("notes")
    if notes_series is not None:
        normalized_notes = notes_series.fillna("").astype(str)
        scheduled_mask = normalized_notes.str.startswith("scheduled@")
        if scheduled_mask.any():
            slot_note = f"scheduled@{slot_label}"
            labels = history_df.loc[
                scheduled_mask & (normalized_notes == slot_note),
                "CurveLabel",
            ].drop_duplicates().tolist()
            if labels:
                return labels

    captured_at_series = pd.to_datetime(history_df.get("captured_at"), errors="coerce")
    if captured_at_series.isna().all():
        return history_df["CurveLabel"].drop_duplicates().tolist()

    row_minutes = captured_at_series.dt.hour * 60 + captured_at_series.dt.minute
    slot_minutes = [minutes for _, minutes in SCHEDULE_SLOTS]
    distance_columns = [(row_minutes - minutes).abs() for minutes in slot_minutes]
    nearest_slot_index = pd.concat(distance_columns, axis=1).idxmin(axis=1)
    target_slot_index = next(i for i, (label, _) in enumerate(SCHEDULE_SLOTS) if label == slot_label)

    labels = history_df.loc[nearest_slot_index == target_slot_index, "CurveLabel"].drop_duplicates().tolist()
    if labels:
        return labels
    return history_df["CurveLabel"].drop_duplicates().tolist()


def render_curve_section(display_payload):
    df = display_payload["current_df"]
    history_df = display_payload["history_df"]
    chart_df = display_payload["chart_df"]
    product_id = display_payload["product_id"]
    underlying_price = display_payload["underlying_price"]
    curve_mode = display_payload["curve_mode"]
    otm_range_pct = display_payload["otm_range_pct"]
    last_update_time = display_payload["last_update_time"]
    history_days = display_payload["history_days"]
    show_history = display_payload["show_history"]

    st.subheader(f"Volatility Smile: {product_id} (Spot: {underlying_price})")
    caption_parts = [
        f"Curve mode: {curve_mode}",
        f"Strike Range: {otm_range_pct:.0%}",
        f"Last update: {last_update_time}",
    ]
    if show_history:
        caption_parts.append(f"History window: {history_days} day(s)")
    st.caption(" | ".join(caption_parts))

    visible_chart_df = chart_df
    visible_history_df = history_df
    if show_history and not history_df.empty:
        history_labels = history_df["CurveLabel"].drop_duplicates().tolist()
        default_history_labels = resolve_default_history_labels(history_df, datetime.now())
        selected_history_labels = st.multiselect(
            "Visible history timestamps",
            options=history_labels,
            default=default_history_labels,
            help="By default, only historical curves in the current scheduled time slot are shown. You can check more.",
        )
        visible_chart_df, visible_history_df = filter_visible_curves(
            chart_df,
            history_df,
            selected_history_labels,
        )

    chart_width = 780
    base = alt.Chart(visible_chart_df).encode(
        x=alt.X("Strike:Q", scale=alt.Scale(zero=False), title="Strike Price"),
        y=alt.Y(
            "IV:Q",
            axis=alt.Axis(format="%"),
            title="Implied Volatility",
            scale=alt.Scale(zero=False),
        ),
        color=alt.Color("CurveLabel:N", title="Curve"),
        strokeDash=alt.StrokeDash("Side:N", title="Side"),
        tooltip=[
            "Strike",
            "Type",
            "Side",
            "CurveLabel",
            "Price",
            alt.Tooltip("Delta", format=".4f"),
            alt.Tooltip("Gamma", format=".6f"),
            alt.Tooltip("Theta", format=".4f"),
            alt.Tooltip("Vega", format=".4f"),
            alt.Tooltip("IV", format=".2%"),
        ],
    ).properties(width=chart_width, height=450)

    line = base.mark_line(interpolate="cardinal", tension=0.8)
    current_points = alt.Chart(visible_chart_df[visible_chart_df["CurveSource"] == "Current"]).mark_point(
        filled=True,
        size=60,
    ).encode(
        x=alt.X("Strike:Q", scale=alt.Scale(zero=False), title="Strike Price"),
        y=alt.Y(
            "IV:Q",
            axis=alt.Axis(format="%"),
            title="Implied Volatility",
            scale=alt.Scale(zero=False),
        ),
        color=alt.Color("CurveLabel:N", title="Curve"),
        strokeDash=alt.StrokeDash("Side:N", title="Side"),
        tooltip=[
            "Strike",
            "Type",
            "Side",
            "CurveLabel",
            "Price",
            alt.Tooltip("Delta", format=".4f"),
            alt.Tooltip("Gamma", format=".6f"),
            alt.Tooltip("Theta", format=".4f"),
            alt.Tooltip("Vega", format=".4f"),
            alt.Tooltip("IV", format=".2%"),
        ],
    )

    chart = (line + current_points).interactive()
    left_spacer, center_col, right_spacer = st.columns([1, 4, 1])
    with center_col:
        st.altair_chart(chart, use_container_width=False)

    with st.expander("Raw Data"):
        editable_df = df.copy()
        if "OrderLots" not in editable_df.columns:
            editable_df["OrderLots"] = 0

        order_lots_state_key = f"order_lots_{product_id}_{curve_mode}"
        stored_order_lots = st.session_state.get(order_lots_state_key, {})
        if stored_order_lots:
            editable_df["OrderLots"] = editable_df.apply(
                lambda row: stored_order_lots.get(make_order_lots_key(row), row["OrderLots"]),
                axis=1,
            )

        t_quote_editor_df = build_t_quote_dataframe(editable_df, underlying_price)
        editable_order_columns = ["Call OrderLots", "Put OrderLots"]
        disabled_columns = [
            column
            for column in t_quote_editor_df.columns
            if column not in editable_order_columns
        ]
        column_config = {
            "ExpireDate": None,
            "Side": None,
            "Call Price": st.column_config.NumberColumn("C Px", format="%.3f", width="small"),
            "Call IV": st.column_config.NumberColumn("C IV", format="%.3f", width="small"),
            "Call Delta": st.column_config.NumberColumn("C Delta", format="%.3f", width="small"),
            "Call TotalDelta": st.column_config.NumberColumn("C TDelta", format="%.3f", width="small"),
            "Call TotalGamma": st.column_config.NumberColumn("C TGamma", format="%.3f", width="small"),
            "Call TotalTheta": st.column_config.NumberColumn("C TTheta", format="%.3f", width="small"),
            "Call TotalVega": st.column_config.NumberColumn("C TVega", format="%.3f", width="small"),
            "Call OrderLots": st.column_config.NumberColumn(
                "C Lots",
                step=1,
                format="%d",
                width="small",
            ),
            "Strike": st.column_config.NumberColumn("Strike", format="%.3f", width="small"),
            "Put Price": st.column_config.NumberColumn("P Px", format="%.3f", width="small"),
            "Put IV": st.column_config.NumberColumn("P IV", format="%.3f", width="small"),
            "Put Delta": st.column_config.NumberColumn("P Delta", format="%.3f", width="small"),
            "Put TotalDelta": st.column_config.NumberColumn("P TDelta", format="%.3f", width="small"),
            "Put TotalGamma": st.column_config.NumberColumn("P TGamma", format="%.3f", width="small"),
            "Put TotalTheta": st.column_config.NumberColumn("P TTheta", format="%.3f", width="small"),
            "Put TotalVega": st.column_config.NumberColumn("P TVega", format="%.3f", width="small"),
            "Put OrderLots": st.column_config.NumberColumn(
                "P Lots",
                step=1,
                format="%d",
                width="small",
            ),
        }
        column_order = [
            "Call OrderLots",
            "Call TotalVega",
            "Call TotalTheta",
            "Call TotalGamma",
            "Call TotalDelta",
            "Call Delta",
            "Call Price",
            "Call IV",
            "Strike",
            "Put IV",
            "Put Price",
            "Put Delta",
            "Put TotalDelta",
            "Put TotalGamma",
            "Put TotalTheta",
            "Put TotalVega",
            "Put OrderLots",
        ]
        st.markdown(
            """
            <style>
            div[data-testid="stDataFrame"] [role="gridcell"],
            div[data-testid="stDataFrame"] [role="columnheader"] {
                font-size: 11px;
                line-height: 1.1;
                padding: 2px 4px;
                text-align: center;
            }
            div[data-testid="stDataFrame"] [aria-colindex="1"],
            div[data-testid="stDataFrame"] [aria-colindex="9"],
            div[data-testid="stDataFrame"] [aria-colindex="17"] {
                font-weight: 700;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        summary_df = build_total_greeks_summary(editable_df, underlying_price)
        st.dataframe(
            summary_df.style.format("{:.3f}"),
            hide_index=True,
            use_container_width=True,
        )

        edited_df = st.data_editor(
            t_quote_editor_df,
            hide_index=True,
            use_container_width=True,
            key=f"order_lots_editor_{product_id}_{curve_mode}",
            column_order=column_order,
            disabled=disabled_columns,
            column_config=column_config,
        )

        edited_position_df = apply_t_quote_order_lots(editable_df, edited_df)
        normalized_order_lots = pd.to_numeric(
            edited_position_df["OrderLots"],
            errors="coerce",
        ).fillna(0).astype(int)
        next_order_lots = {
            make_order_lots_key(row): int(normalized_order_lots.loc[index])
            for index, row in edited_position_df.iterrows()
        }
        if next_order_lots != stored_order_lots:
            st.session_state[order_lots_state_key] = next_order_lots
            st.rerun()
        st.caption(
            "Definition"
            " | TotalDelta: Delta x OrderLots, unit = hands"
            " | TotalDelta (Hedge): -TotalDelta, unit = futures hands"
            " | TotalGamma: 0.5 x Gamma x dS^2 x OrderLots x VolumeMultiple, unit = CNY"
            " | TotalTheta: Theta x OrderLots x VolumeMultiple / 244, unit = CNY/day"
            " | TotalVega: Vega x 1% x OrderLots x VolumeMultiple, unit = CNY/1%"
        )
        _ = (
            "Definition"
            " | TotalDelta: Delta × OrderLots, unit = hands"
            " | TotalGamma: 0.5 × Gamma × dS^2 × OrderLots × VolumeMultiple, unit = CNY"
            " | TotalTheta: Theta × OrderLots × VolumeMultiple / 244, unit = CNY/day"
            " | TotalVega: Vega × 1% × OrderLots × VolumeMultiple, unit = CNY/1%"
        )

        if show_history and not visible_history_df.empty:
            st.markdown("Historical Curves")
            history_raw_df = visible_history_df.rename(
                columns={
                    "option_type": "Type",
                    "strike_price": "Strike",
                    "side": "Side",
                    "price": "Price",
                    "iv": "IV",
                }
            )
            st.dataframe(history_raw_df, use_container_width=True)


def main():
    st.set_page_config(page_title="CTP Volatility Visualizer", layout="wide")
    st.title("Real-time Option Volatility Visualizer")

    md_front, broker_id, user_id, password, db_config = load_config_from_xml()
    ensure_tables(db_config)

    st.sidebar.header("Pricing Configuration")

    @st.cache_data
    def fetch_underlyings(config):
        return get_available_underlyings(config)

    available_unds = fetch_underlyings(db_config)
    default_idx = available_unds.index("cu2604") if "cu2604" in available_unds else 0

    if available_unds:
        product_id = st.sidebar.selectbox("Underlying ID", available_unds, index=default_idx)
    else:
        product_id = st.sidebar.text_input("Underlying ID", value="cu2604")

    risk_free = st.sidebar.number_input("Risk Free Rate", 0.0, 0.2, 0.05, 0.001)
    dividend = st.sidebar.number_input("Dividend Yield", 0.0, 0.2, 0.05, 0.001)
    otm_range_pct = st.sidebar.slider("Strike Range (%)", min_value=1, max_value=20, value=10, step=1) / 100.0
    curve_mode = st.sidebar.radio("Curve Mode", options=["Bid/Ask", "Mid"], index=0)
    eval_date = st.sidebar.date_input("Evaluation Date", value=datetime.now())
    show_history = st.sidebar.checkbox("Overlay Historical Curves", value=False)
    history_days = st.sidebar.selectbox(
        "History Range",
        options=[1, 2, 3, 7, 15],
        index=0,
        format_func=lambda days: f"{days} day(s)",
    )

    st.sidebar.markdown("---")
    auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh (1 min)", value=False)

    update_eval_date(eval_date)

    status_placeholder = st.empty()
    try:
        engine = get_market_engine(md_front, broker_id, user_id, password)
        status_placeholder.success(f"CTP Engine Connected to {md_front}")
    except Exception as exc:
        status_placeholder.error(f"Failed to connect CTP: {exc}")
        st.stop()

    st.markdown("---")
    manual_trigger = st.button("Fetch & Plot Volatility", type="primary", use_container_width=True)

    if "last_update_time" in st.session_state:
        st.caption(f"Last update: {st.session_state['last_update_time']}")

    if st.session_state.get("save_feedback"):
        st.info(st.session_state["save_feedback"])

    save_trigger = st.button("Save Current Curve", use_container_width=True)

    if save_trigger:
        pending_snapshot = st.session_state.get("pending_snapshot")
        if pending_snapshot is None:
            st.warning("There is no curve to save. Please fetch and plot a curve first.")
        else:
            snapshot_id = save_curve_snapshot(
                db_config,
                pending_snapshot["snapshot"],
                pending_snapshot["curve_points"],
            )
            st.session_state["save_feedback"] = f"Saved current curve snapshot #{snapshot_id}"
            st.rerun()

    if manual_trigger or auto_refresh:
        run_process(
            engine,
            product_id,
            risk_free,
            dividend,
            db_config,
            otm_range_pct,
            curve_mode,
            eval_date,
            show_history,
            history_days,
        )

    if st.session_state.get("curve_display"):
        render_curve_section(st.session_state["curve_display"])

    if auto_refresh:
        time.sleep(60)
        st.rerun()


def run_process(engine, product_id, r, q, db_config, otm_range_pct, curve_mode, eval_date, show_history, history_days):
    with st.status("Processing Market Data...", expanded=True) as status:
        status.write(f"Fetching and calculating curve for {product_id}...")
        underlying_price, data, error_message = calculate_curve_data(
            engine,
            product_id,
            r,
            q,
            db_config,
            otm_range_pct,
            curve_mode,
        )
        if error_message:
            status.update(label="Error", state="error")
            st.error(error_message)
            return

        status.update(label="Complete", state="complete")

    st.session_state["last_update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df = pd.DataFrame(data)
    otm_df = filter_otm_curve_dataframe(df, underlying_price)
    if otm_df.empty:
        st.error(f"No OTM volatility data calculated for {product_id}.")
        return

    current_label = f"Current {st.session_state['last_update_time']}"
    history_df = pd.DataFrame()
    if show_history:
        history_df = load_recent_curve_points(db_config, product_id, curve_mode, history_days)

    chart_df = build_chart_dataframe(otm_df, current_label, history_df)
    st.session_state["pending_snapshot"] = build_snapshot_payload(
        product_id,
        underlying_price,
        r,
        q,
        eval_date,
        otm_range_pct,
        curve_mode,
        otm_df.to_dict("records"),
    )
    st.session_state["curve_display"] = {
        "current_df": df,
        "history_df": history_df,
        "chart_df": chart_df,
        "product_id": product_id,
        "underlying_price": underlying_price,
        "curve_mode": curve_mode,
        "otm_range_pct": otm_range_pct,
        "last_update_time": st.session_state["last_update_time"],
        "history_days": history_days,
        "show_history": show_history,
    }


if __name__ == "__main__":
    main()
