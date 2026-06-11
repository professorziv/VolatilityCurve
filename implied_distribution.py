"""
Breeden-Litzenberger implied risk-neutral density from an IV smile,
plus a log-normal (Black-Scholes) reference curve at ATM vol.

Core idea:
  q(K) = e^(rT) * d²C/dK²

where C(K) is the Black-Scholes call price using a polynomial-fit IV at K.
Input IV should be OTM options only (put IV below spot, call IV above).
"""

import numpy as np
import pandas as pd
from datetime import date
from scipy.stats import norm


# ── Black-Scholes call price ──────────────────────────────────────────────────
def _bs_call(S: float, K: float, T: float, r: float, q: float, sigma: float) -> float:
    if T <= 0 or sigma <= 0:
        return max(S * np.exp(-q * T) - K * np.exp(-r * T), 0.0)
    d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return S * np.exp(-q * T) * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)


# ── Log-normal (BS) reference density ────────────────────────────────────────
def lognormal_rnd(
    K_grid: np.ndarray,
    S: float,
    T: float,
    r: float,
    q: float,
    sigma: float,
) -> np.ndarray:
    """
    Risk-neutral log-normal density at strikes K_grid under constant vol sigma.

    In the BS risk-neutral measure:
        ln(S_T) ~ N( ln(F) - 0.5*sigma²*T,  sigma²*T )
    where F = S * exp((r-q)*T) is the forward.

    Returns a PDF array normalized to integrate to 1 over K_grid.
    """
    F      = S * np.exp((r - q) * T)
    mu_ln  = np.log(F) - 0.5 * sigma ** 2 * T   # mean of ln(S_T)
    sig_T  = sigma * np.sqrt(T)                   # std of ln(S_T)
    pdf    = norm.pdf(np.log(K_grid), loc=mu_ln, scale=sig_T) / K_grid
    area   = np.trapz(pdf, K_grid)
    return pdf / area if area > 1e-12 else pdf


# ── Core RND computation ──────────────────────────────────────────────────────
def compute_rnd(
    strikes: np.ndarray,
    ivs: np.ndarray,
    S: float,
    T: float,
    r: float,
    q: float,
    n_grid: int = 300,
    poly_deg: int = 4,
) -> tuple[np.ndarray | None, np.ndarray | None, float | None]:
    """
    Breeden-Litzenberger RND for a single expiry.

    Returns (K_grid, pdf, atm_iv) or (None, None, None) on failure.
    atm_iv is the polynomial-fit IV evaluated at K = S (log-moneyness = 0).

    Notes
    -----
    Interpolating *through* every noisy market IV point causes the second
    derivative to oscillate wildly. Instead we **least-squares fit** a
    low-degree polynomial to the IV smile in log-moneyness space so that
    bid-ask noise is averaged out. The uniform K grid then ensures the
    centred finite-difference formula is exact.
    """
    strikes = np.asarray(strikes, dtype=float)
    ivs     = np.asarray(ivs,     dtype=float)

    valid   = np.isfinite(strikes) & np.isfinite(ivs) & (ivs > 0) & (strikes > 0)
    strikes, ivs = strikes[valid], ivs[valid]

    if len(strikes) < 4:
        return None, None, None

    idx     = np.argsort(strikes)
    strikes = strikes[idx]
    ivs     = ivs[idx]

    try:
        log_m = np.log(strikes / S)

        # Smooth IV via polynomial least-squares (does NOT pass through every point)
        deg     = max(2, min(poly_deg, len(log_m) - 2))
        coeffs  = np.polyfit(log_m, ivs, deg=deg)
        poly_iv = np.poly1d(coeffs)

        # ATM vol = polynomial evaluated at log-moneyness 0
        atm_iv = float(np.clip(poly_iv(0.0), 1e-4, 10.0))

        # Uniform (arithmetic) K grid — exact centred differences
        K_grid = np.linspace(strikes[0] * 1.005, strikes[-1] * 0.995, n_grid)
        dK     = K_grid[1] - K_grid[0]

        sig_grid = np.clip(poly_iv(np.log(K_grid / S)), 1e-4, 10.0)

        C = np.array([_bs_call(S, K, T, r, q, s) for K, s in zip(K_grid, sig_grid)])

        d2C         = np.empty_like(C)
        d2C[1:-1]   = (C[2:] - 2.0 * C[1:-1] + C[:-2]) / dK ** 2
        d2C[0]      = d2C[1]
        d2C[-1]     = d2C[-2]

        pdf  = np.exp(r * T) * d2C
        pdf  = np.maximum(pdf, 0.0)
        area = np.trapz(pdf, K_grid)
        if area > 1e-12:
            pdf /= area

        return K_grid, pdf, atm_iv

    except Exception:
        return None, None, None


# ── Date utility ──────────────────────────────────────────────────────────────
def _to_date(x) -> date | None:
    if isinstance(x, date):
        return x
    try:
        return pd.Timestamp(x).date()
    except Exception:
        return None


# ── DataFrame-level wrapper ───────────────────────────────────────────────────
def compute_distributions_from_df(
    df: pd.DataFrame,
    S: float,
    r: float,
    q: float,
    eval_date,
) -> dict[str, dict]:
    """
    Compute RND for every expiry in a Mid-curve DataFrame.

    Returns {expire_str: {"K_grid", "pdf", "atm_iv", "T", "S", "r", "q"}}.
    """
    col_map = {
        "strike_price": "Strike",
        "iv":           "IV",
        "expire_date":  "ExpireDate",
        "side":         "Side",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    if "Side" in df.columns:
        df = df[df["Side"] == "Mid"]

    if df.empty or not {"Strike", "IV", "ExpireDate"}.issubset(df.columns):
        return {}

    eval_d  = _to_date(eval_date) or date.today()
    results = {}

    for expire_date, group in df.groupby("ExpireDate"):
        exp_d = _to_date(expire_date)
        if exp_d is None:
            continue
        T = max((exp_d - eval_d).days / 365.0, 1.0 / 365.0)

        K_grid, pdf, atm_iv = compute_rnd(group["Strike"].values, group["IV"].values,
                                           S, T, r, q)
        if K_grid is not None:
            results[str(expire_date)] = {
                "K_grid": K_grid, "pdf": pdf, "atm_iv": atm_iv,
                "T": T, "S": S, "r": r, "q": q,
            }

    return results


# ── Chart DataFrame builders ──────────────────────────────────────────────────
def build_dist_chart_df(
    dists_by_label: dict[str, dict[str, dict]],
    current_label: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Flatten distributions into two Altair-friendly DataFrames:
      - implied_df : implied RND curves (current + history)
      - ref_df     : log-normal BS reference curves (current curve only)

    implied_df columns : K, pdf, CurveLabel, ExpireDate, IsCurrentCurve
    ref_df columns     : K, pdf, RefLabel
    """
    implied_rows = []
    ref_rows     = []

    for label, expire_dists in dists_by_label.items():
        is_current = label == current_label
        for expire_str, info in expire_dists.items():
            K_grid = info["K_grid"]
            pdf    = info["pdf"]
            S_val  = info.get("S", 1.0)

            for k, p in zip(K_grid, pdf):
                implied_rows.append({
                    "Moneyness": np.log(k / S_val),
                    "pdf": p,
                    "CurveLabel": label,
                    "ExpireDate": expire_str,
                    "IsCurrentCurve": is_current,
                })

            # Log-normal reference only for the current curve
            if is_current:
                atm_iv = info.get("atm_iv")
                T, S, r_val, q_val = (info.get(x) for x in ("T", "S", "r", "q"))
                if all(v is not None for v in [atm_iv, T, S, r_val, q_val]):
                    ln_pdf    = lognormal_rnd(K_grid, S, T, r_val, q_val, atm_iv)
                    ref_label = f"Log-Normal  σ_ATM={atm_iv:.1%}"
                    for k, p in zip(K_grid, ln_pdf):
                        ref_rows.append({"Moneyness": np.log(k / S_val), "pdf": p, "RefLabel": ref_label})

    return pd.DataFrame(implied_rows), pd.DataFrame(ref_rows)
