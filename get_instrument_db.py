import sys
import threading
import time
from datetime import datetime

try:
    from openctp_ctp import tdapi
except ImportError:
    print("Please install openctp-ctp: pip install openctp-ctp")
    sys.exit(1)

try:
    import mysql.connector
except ImportError:
    print("Please install mysql driver: pip install mysql-connector-python")
    sys.exit(1)

# --- Configuration ---
TRADER_FRONT = "tcp://182.254.243.31:30001"
BROKER_ID = "9999"
USER_ID = "256432"
PASSWORD = "Ljh~970507"
APP_ID = "simnow_client_test"
AUTH_CODE = "0000000000000000"

# --- MySQL Configuration ---
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "xhrf@123"
DB_NAME = "ctp_options_db"
TABLE_NAME = "contract_specs"

EXCHANGES_TO_QUERY = ["SHFE", "DCE", "CZCE", "CFFEX", "INE"]


class InstrumentDbSpi(tdapi.CThostFtdcTraderSpi):
    """SPI for handling instrument query callbacks."""

    def __init__(self, api):
        super().__init__()
        self.api = api
        self.instruments_data = []
        self.seen_instruments = set()
        self.received_count = 0

        self.connected = threading.Event()
        self.authenticated = threading.Event()
        self.logged_in = threading.Event()
        self.query_completed = threading.Event()
        self.expected_request_id = None
        self.current_exchange = ""
        self.last_query_error = None

    @staticmethod
    def _normalize_ctp_char(value):
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", errors="ignore").strip()
        return str(value).strip()

    def start_query(self, exchange_id, request_id):
        self.current_exchange = exchange_id
        self.expected_request_id = request_id
        self.last_query_error = None
        self.received_count = 0
        self.query_completed.clear()

    def OnFrontConnected(self):
        print(">>> Trader front connected.")
        self.connected.set()

    def OnRspAuthenticate(self, pRspAuthenticateField, pRspInfo, nRequestID, bIsLast):
        if pRspInfo and pRspInfo.ErrorID == 0:
            print(">>> Authentication successful.")
            self.authenticated.set()
        else:
            error_msg = pRspInfo.ErrorMsg if pRspInfo else "No response info"
            print(f">>> Authentication failed: {error_msg}")
            self.connected.clear()

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        if pRspInfo and pRspInfo.ErrorID == 0:
            print(f">>> Login successful (User: {pRspUserLogin.UserID}).")
            self.logged_in.set()
        else:
            error_msg = pRspInfo.ErrorMsg if pRspInfo else "No response info"
            print(f">>> Login failed: {error_msg}")
            self.connected.clear()

    def OnRspQryInstrument(self, pInstrument, pRspInfo, nRequestID, bIsLast):
        """Callback for instrument query, collects option contract details."""
        if self.expected_request_id is not None and nRequestID != self.expected_request_id:
            return

        if pRspInfo and pRspInfo.ErrorID != 0:
            self.last_query_error = (pRspInfo.ErrorID, pRspInfo.ErrorMsg)

        if pInstrument:
            self.received_count += 1
            if self.received_count % 500 == 0:
                print(
                    f"    ...scanned {self.received_count} instruments "
                    f"(Current: {pInstrument.InstrumentID})..."
                )

            product_class = self._normalize_ctp_char(pInstrument.ProductClass)
            options_type = self._normalize_ctp_char(pInstrument.OptionsType)
            instrument_id = pInstrument.InstrumentID
            exchange_id = pInstrument.ExchangeID

            if (
                product_class == "2"
                and exchange_id == self.current_exchange
                and instrument_id not in self.seen_instruments
            ):
                self.seen_instruments.add(instrument_id)
                self.instruments_data.append(
                    {
                        "InstrumentID": instrument_id,
                        "ExchangeID": exchange_id,
                        "InstrumentName": pInstrument.InstrumentName,
                        "ProductID": pInstrument.ProductID,
                        "OptionsType": "Call" if options_type == "1" else ("Put" if options_type == "2" else "Unknown"),
                        "StrikePrice": pInstrument.StrikePrice,
                        "UnderlyingInstrID": pInstrument.UnderlyingInstrID,
                        "ExpireDate": pInstrument.ExpireDate,
                        "VolumeMultiple": pInstrument.VolumeMultiple,
                        "PriceTick": pInstrument.PriceTick,
                    }
                )

        if bIsLast:
            if self.last_query_error:
                print(
                    f">>> Query finished with error for {self.current_exchange} "
                    f"(RequestID={nRequestID}): {self.last_query_error[0]} {self.last_query_error[1]}"
                )
            print(
                f">>> Query finished for {self.current_exchange} "
                f"(RequestID={nRequestID}). Total instruments scanned: {self.received_count}."
            )
            self.query_completed.set()


def save_to_mysql(data_list):
    """Compares with existing DB data, reports changes, then replaces all data."""
    conn = None
    cursor = None
    try:
        print(f"Connecting to MySQL ({DB_HOST})...")
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cursor = conn.cursor()

        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} DEFAULT CHARACTER SET utf8mb4")
        cursor.execute(f"USE {DB_NAME}")

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            InstrumentID VARCHAR(30) PRIMARY KEY,
            ExchangeID VARCHAR(10),
            InstrumentName VARCHAR(100),
            ProductID VARCHAR(10),
            OptionsType VARCHAR(10),
            StrikePrice DOUBLE,
            UnderlyingInstrID VARCHAR(30),
            ExpireDate VARCHAR(20),
            VolumeMultiple INT,
            PriceTick DOUBLE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """
        cursor.execute(create_table_sql)

        print("Fetching existing data for comparison...")
        cursor.execute(f"SELECT * FROM {TABLE_NAME}")
        old_data_map = {row[0]: dict(zip(cursor.column_names, row)) for row in cursor.fetchall()}

        new_data_map = {d["InstrumentID"]: d for d in data_list}
        new_ids = set(new_data_map.keys())
        old_ids = set(old_data_map.keys())

        added = new_ids - old_ids
        removed = old_ids - new_ids
        common = new_ids & old_ids
        changed = []

        compare_fields = [
            "ExchangeID",
            "InstrumentName",
            "ProductID",
            "OptionsType",
            "StrikePrice",
            "UnderlyingInstrID",
            "ExpireDate",
            "VolumeMultiple",
            "PriceTick",
        ]

        for uid in common:
            old_rec = old_data_map[uid]
            new_rec = new_data_map[uid]
            diffs = []
            for field in compare_fields:
                if str(new_rec.get(field)) != str(old_rec.get(field)):
                    diffs.append(f"{field}: {old_rec.get(field)} -> {new_rec.get(field)}")
            if diffs:
                changed.append((uid, diffs))

        print("\n" + "=" * 50)
        print(f"Change Report ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
        print("=" * 50)
        if not added and not removed and not changed:
            print("No changes detected. Database is already up to date.")
            return

        if added:
            print(f"[+] Added {len(added)} contracts (e.g., {list(added)[:3]}...)")
        if removed:
            print(f"[-] Removed {len(removed)} contracts (e.g., {list(removed)[:3]}...)")
        if changed:
            print(f"[*] Changed {len(changed)} contracts:")
            for uid, diffs in changed[:10]:
                print(f"    - {uid}: {', '.join(diffs)}")
            if len(changed) > 10:
                print(f"    ... and {len(changed) - 10} more.")
        print("=" * 50 + "\n")

        print(f"Updating database: deleting old data and inserting {len(data_list)} new records...")
        cursor.execute(f"TRUNCATE TABLE {TABLE_NAME}")

        insert_sql = f"""
        INSERT INTO {TABLE_NAME}
        (InstrumentID, ExchangeID, InstrumentName, ProductID, OptionsType, StrikePrice, UnderlyingInstrID, ExpireDate, VolumeMultiple, PriceTick)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        values = [
            (
                d["InstrumentID"],
                d["ExchangeID"],
                d["InstrumentName"],
                d["ProductID"],
                d["OptionsType"],
                d["StrikePrice"],
                d["UnderlyingInstrID"],
                d["ExpireDate"],
                d["VolumeMultiple"],
                d["PriceTick"],
            )
            for d in data_list
        ]

        cursor.executemany(insert_sql, values)
        conn.commit()
        print(f"Successfully refreshed table '{TABLE_NAME}'. Total records: {cursor.rowcount}")

    except mysql.connector.Error as err:
        if err.errno == 1045:
            print("\n[Connection failed] Invalid username or password.")
            print("Open this script and update the 'DB_PASSWORD' value to your local MySQL root password.")
        elif err.errno == 2003:
            print("\n[Connection failed] Unable to connect to the MySQL server. Make sure MySQL is installed and running.")
        else:
            print(f"MySQL Error: {err}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def main():
    """Main function to connect, query, and save instrument data."""
    api = tdapi.CThostFtdcTraderApi.CreateFtdcTraderApi()
    spi = InstrumentDbSpi(api)
    api.RegisterSpi(spi)
    api.RegisterFront(TRADER_FRONT)
    api.Init()

    try:
        if not spi.connected.wait(10):
            print("Error: connection to trader front timed out.")
            return

        auth_req = tdapi.CThostFtdcReqAuthenticateField()
        auth_req.BrokerID = BROKER_ID
        auth_req.UserID = USER_ID
        auth_req.AppID = APP_ID
        auth_req.AuthCode = AUTH_CODE
        api.ReqAuthenticate(auth_req, 0)
        if not spi.authenticated.wait(5):
            print("Error: authentication timed out.")
            return

        login_req = tdapi.CThostFtdcReqUserLoginField()
        login_req.BrokerID = BROKER_ID
        login_req.UserID = USER_ID
        login_req.Password = PASSWORD
        api.ReqUserLogin(login_req, 0)
        if not spi.logged_in.wait(5):
            print("Error: login timed out.")
            return

        print("\nStarting instrument query for all exchanges...")
        request_id = 1
        for exchange in EXCHANGES_TO_QUERY:
            print(f"\nQuerying exchange: {exchange}")
            spi.start_query(exchange, request_id)

            qry_req = tdapi.CThostFtdcQryInstrumentField()
            qry_req.ExchangeID = exchange
            ret = api.ReqQryInstrument(qry_req, request_id)
            if ret != 0:
                print(f"Warning: ReqQryInstrument failed for {exchange}. ret={ret}, request_id={request_id}")
                request_id += 1
                continue

            print(f"Request sent for {exchange} (request_id={request_id}), waiting for data stream...")

            if not spi.query_completed.wait(300):
                print(f"Warning: query for {exchange} timed out (request_id={request_id}).")
                request_id += 1
                continue

            request_id += 1
            time.sleep(0.5)

        if not spi.instruments_data:
            print("\nNo option contracts found. Database will not be updated.")
            return

        print(f"\nAPI query complete. Found {len(spi.instruments_data)} option contracts.")
        save_to_mysql(spi.instruments_data)

    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    finally:
        print("Releasing CTP API resources.")
        api.Release()


if __name__ == "__main__":
    main()
