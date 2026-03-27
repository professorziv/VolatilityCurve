import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st

# --- Check Dependencies ---
try:
    import XHPricingPy as xh
except ImportError:
    st.error("Missing dependencies. Please run: pip install XHPricingPy mysql-connector-python streamlit pandas altair")
    st.stop()

# --- Local Imports ---
from VanillaOption import VanillaOption
from get_option_codes import get_available_underlyings, get_filtered_options
from iv_curve_storage import ensure_tables, load_recent_curve_points, save_curve_snapshot
from quote_engine import CTPMarketEngine


def load_config_from_xml(file_path="config.xml"):
    """Loads CTP and DB configuration from an XML file."""
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
    """Initializes and caches the CTP Market Engine."""
    print("Initializing CTP Market Engine...")
    engine = CTPMarketEngine(md_front, broker_id, user_id, password)
    engine.start()
    return engine


def create_bs_process(underlying_price, r, q):
    """Creates the Black-Scholes process for pricing."""
    return xh.FastGeneralizedBlackScholesProcessMaker(
        underlying_price, q, r, 0.20, xh.Business244
    )


def update_eval_date(selected_date):
    """Updates the global XHPricingPy evaluation date."""
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
        f"OTM Range: {otm_range_pct:.0%}",
        f"Last update: {last_update_time}",
    ]
    if show_history:
        caption_parts.append(f"History window: {history_days} day(s)")
    st.caption(" | ".join(caption_parts))

    chart_width = 780
    base = alt.Chart(chart_df).encode(
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
            alt.Tooltip("IV", format=".2%"),
        ],
    ).properties(width=chart_width, height=450)

    line = base.mark_line(interpolate="cardinal", tension=0.8)
    current_points = alt.Chart(chart_df[chart_df["CurveSource"] == "Current"]).mark_point(
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
            alt.Tooltip("IV", format=".2%"),
        ],
    )

    chart = (line + current_points).interactive()
    left_spacer, center_col, right_spacer = st.columns([1, 4, 1])
    with center_col:
        st.altair_chart(chart, use_container_width=False)

    with st.expander("Raw Data"):
        st.dataframe(df, use_container_width=True)
        if show_history and not history_df.empty:
            st.markdown("Historical Curves")
            history_raw_df = history_df.rename(
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
    otm_range_pct = st.sidebar.slider("OTM Range (%)", min_value=1, max_value=20, value=10, step=1) / 100.0
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
        st.caption(f"最近一次更新: {st.session_state['last_update_time']}")

    if st.session_state.get("save_feedback"):
        st.info(st.session_state["save_feedback"])

    save_trigger = st.button("保存当前曲线", use_container_width=True)

    if save_trigger:
        pending_snapshot = st.session_state.get("pending_snapshot")
        if pending_snapshot is None:
            st.warning("当前没有可保存的曲线，请先刷新并绘制曲线。")
        else:
            snapshot_id = save_curve_snapshot(
                db_config,
                pending_snapshot["snapshot"],
                pending_snapshot["curve_points"],
            )
            st.session_state["save_feedback"] = f"已保存曲线快照 #{snapshot_id}"
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
    """Executes the data fetching and calculation pipeline."""
    with st.status("Processing Market Data...", expanded=True) as status:
        status.write(f"Fetching spot price for {product_id}...")
        engine.subscribe([product_id.encode("utf-8")])

        underlying_price = 0.0
        for _ in range(30):
            quote = engine.get_quote(product_id)
            if quote and quote.get("last", 0) > 0:
                underlying_price = quote["last"]
                break
            time.sleep(0.1)

        if underlying_price <= 0:
            status.update(label="Error: No underlying price", state="error")
            st.error(f"Could not fetch price for {product_id}. Market may be closed.")
            return

        status.write(f"Underlying Price: {underlying_price}")

        specs = get_filtered_options(
            product_id,
            underlying_price,
            db_config,
            otm_range_pct=otm_range_pct,
        )
        if not specs:
            status.update(label="Error: No options found", state="error")
            st.error("No matching OTM options found in database.")
            return

        status.write(f"Found {len(specs)} OTM options within {otm_range_pct:.0%}. Subscribing...")

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
                    data.append(
                        {
                            "InstrumentID": inst,
                            "Strike": spec["StrikePrice"],
                            "Type": spec["OptionsType"],
                            "ExpireDate": expire_date,
                            "IV": bid_iv,
                            "Side": "Bid",
                            "Price": quote["bid"],
                        }
                    )
                if ask_iv is not None:
                    data.append(
                        {
                            "InstrumentID": inst,
                            "Strike": spec["StrikePrice"],
                            "Type": spec["OptionsType"],
                            "ExpireDate": expire_date,
                            "IV": ask_iv,
                            "Side": "Ask",
                            "Price": quote["ask"],
                        }
                    )
            elif bid_iv is not None and ask_iv is not None:
                data.append(
                    {
                        "InstrumentID": inst,
                        "Strike": spec["StrikePrice"],
                        "Type": spec["OptionsType"],
                        "ExpireDate": expire_date,
                        "IV": (bid_iv + ask_iv) / 2,
                        "Side": "Mid",
                        "Price": (quote["bid"] + quote["ask"]) / 2,
                    }
                )

        status.update(label="Complete", state="complete")

    st.session_state["last_update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not data:
        st.warning("No volatility data calculated. Check if market data is active.")
        return

    df = pd.DataFrame(data)
    current_label = f"Current {st.session_state['last_update_time']}"
    history_df = pd.DataFrame()
    if show_history:
        history_df = load_recent_curve_points(db_config, product_id, curve_mode, history_days)

    chart_df = build_chart_dataframe(df, current_label, history_df)
    st.session_state["pending_snapshot"] = build_snapshot_payload(
        product_id,
        underlying_price,
        r,
        q,
        eval_date,
        otm_range_pct,
        curve_mode,
        data,
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
