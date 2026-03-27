import sys
try:
    import XHPricingPy as xh
except ImportError:
    print("Please install XHPricingPy: pip install XHPricingPy")
    sys.exit(1)

class VanillaOption:
    """
    Represents a European vanilla option and provides methods for pricing.
    """
    def __init__(self, instrument_id, strike_price, expiry_date_str, option_type_str):
        """
        Initializes the static properties of the option.
        :param instrument_id: The CTP instrument ID.
        :param strike_price: The strike price of the option.
        :param expiry_date_str: An expiry date string in 'YYYYMMDD' format.
        :param option_type_str: 'Call' or 'Put'.
        """
        self.instrument_id = instrument_id
        self.strike_price = strike_price

        # Convert string date to xh.Date
        year, month_int, day = int(expiry_date_str[:4]), int(expiry_date_str[4:6]), int(expiry_date_str[6:])
        
        # Map integer month to the required XH month enum
        month_map = {
            1: xh.Jan, 2: xh.Feb, 3: xh.Mar, 4: xh.Apr,
            5: xh.May, 6: xh.Jun, 7: xh.Jul, 8: xh.Aug,
            9: xh.Sep, 10: xh.Oct, 11: xh.Nov, 12: xh.Dec
        }
        xh_month = month_map[month_int]

        self.expiry_date = xh.Date(day, xh_month, year)

        # Convert string type to xh.Call/xh.Put
        self.option_type = xh.Call if option_type_str == 'Call' else xh.Put

        self.xh_option = xh.EuropeanVanillaOptionMaker(self.option_type, self.strike_price, self.expiry_date)

    def calculate_implied_vol(self, market_price, bs_process):
        """
        Calculates the implied volatility for a given market price.
        :param market_price: The current market price of the option.
        :param bs_process: The Black-Scholes process to use for calculation.
        :return: The implied volatility as a float, or None if calculation fails.
        """
        if bs_process is None:
            return None

        if market_price <= 0:
            return 0.0

        try:
            implied_vol = xh.ImpliedVolatility(self.xh_option, market_price, bs_process)
            return implied_vol
        except Exception:
            # Suppress error printing for cleaner output in a loop
            return None

    def calculate_greeks(self, underlying_price, r, q, vol):
        """
        Calculates option greeks using the supplied implied volatility.
        :return: Dict with delta/gamma/theta/vega, or None values if calculation fails.
        """
        if vol is None or vol <= 0:
            return {
                "delta": None,
                "gamma": None,
                "theta": None,
                "vega": None,
            }

        try:
            proc = xh.FastGeneralizedBlackScholesProcessMaker(
                underlying_price, q, r, vol, xh.Business244
            )
            eng = xh.AnalyticEuropeanEngineMaker()
            rst = xh.OneAssetOptionCalculator(self.xh_option, eng, proc)
            return {
                "delta": rst.delta(),
                "gamma": rst.gamma(),
                "theta": rst.theta(),
                "vega": rst.vega(),
            }
        except Exception:
            return {
                "delta": None,
                "gamma": None,
                "theta": None,
                "vega": None,
            }
