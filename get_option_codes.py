import sys

try:
    import mysql.connector
except ImportError:
    print("Please install mysql-connector-python")
    sys.exit(1)


def get_filtered_options(product_id, underlying_price, db_config, otm_range_pct=0.10):
    """
    Fetch option contracts from MySQL and keep all Call/Put contracts whose
    strikes are within the configured range around the spot price.
    """
    selected_specs = []
    upper_multiplier = 1 + otm_range_pct
    lower_multiplier = 1 - otm_range_pct

    print(f"Querying option contracts for {product_id} from the database...")

    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = (
            "SELECT InstrumentID, StrikePrice, OptionsType, ExpireDate, VolumeMultiple "
            "FROM contract_specs WHERE UPPER(UnderlyingInstrID) = UPPER(%s)"
        )
        cursor.execute(query, (product_id.strip(),))
        all_options = cursor.fetchall()

        print(
            f"Found {len(all_options)} related contracts in the database. "
            f"Filtering with spot price {underlying_price} and strike range {otm_range_pct:.0%}..."
        )

        for opt in all_options:
            strike = opt["StrikePrice"]
            otype = opt["OptionsType"]
            if (
                otype in {"Call", "Put"}
                and underlying_price * lower_multiplier <= strike <= underlying_price * upper_multiplier
            ):
                selected_specs.append(opt)

    except mysql.connector.Error as err:
        print(f"Database query error: {err}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

    return selected_specs


def get_available_underlyings(db_config):
    """
    Fetches all unique underlying instrument IDs from the database.
    """
    underlyings = []
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT UnderlyingInstrID FROM contract_specs ORDER BY UnderlyingInstrID")
        underlyings = [row[0] for row in cursor.fetchall()]
    except mysql.connector.Error as err:
        print(f"Database query error: {err}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
    return underlyings
