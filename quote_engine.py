import sys
import threading

try:
    from openctp_ctp import mdapi
except ImportError as e:
    import platform

    print(f"[Import error] {e}")
    print(f"[Python executable] {sys.executable}")
    print(f"[Python version] {sys.version}")
    print(f"[System architecture] {platform.architecture()}")
    print(f"[Module search path] {sys.path}")
    print("Hint: if you see 'DLL load failed', install Visual C++ Redistributable.")
    sys.exit(1)


class CTPMdSpi(mdapi.CThostFtdcMdSpi):
    def __init__(self, api, broker_id, user_id, password, data_storage, login_event):
        super().__init__()
        self.api = api
        self.broker_id = broker_id
        self.user_id = user_id
        self.password = password
        self.data_storage = data_storage
        self.login_event = login_event

    def OnFrontConnected(self):
        print(">>> OnFrontConnected: market data front connected")

        req = mdapi.CThostFtdcReqUserLoginField()
        req.BrokerID = self.broker_id
        req.UserID = self.user_id
        req.Password = self.password

        self.api.ReqUserLogin(req, 0)

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        if pRspInfo and pRspInfo.ErrorID == 0:
            print(f">>> OnRspUserLogin: login successful (User: {pRspUserLogin.UserID})")
            self.login_event.set()
        else:
            error_id = pRspInfo.ErrorID if pRspInfo else "Unknown"
            error_msg = pRspInfo.ErrorMsg if pRspInfo else "No response info"
            print(f">>> OnRspUserLogin: login failed ErrorID={error_id}, Msg={error_msg}")
            self.login_event.set()

    def OnRtnDepthMarketData(self, pDepthMarketData):
        self.data_storage[pDepthMarketData.InstrumentID] = {
            "bid": pDepthMarketData.BidPrice1,
            "ask": pDepthMarketData.AskPrice1,
            "last": pDepthMarketData.LastPrice,
        }


class CTPMarketEngine:
    def __init__(self, front_addr, broker_id, user_id, password):
        self.api = mdapi.CThostFtdcMdApi.CreateFtdcMdApi()
        self.data_storage = {}
        self.login_event = threading.Event()

        self.spi = CTPMdSpi(
            self.api,
            broker_id,
            user_id,
            password,
            self.data_storage,
            self.login_event,
        )
        self.api.RegisterSpi(self.spi)
        self.api.RegisterFront(front_addr)

    def start(self):
        self.api.Init()
        print("Connecting to market data server, please wait...")
        if not self.login_event.wait(timeout=10):
            print("Warning: login timed out or failed")

    def subscribe(self, instruments):
        inst_bytes = [inst if isinstance(inst, bytes) else inst.encode("utf-8") for inst in instruments]
        if inst_bytes:
            self.api.SubscribeMarketData(inst_bytes, len(inst_bytes))
            print(f"Subscription request sent: {instruments}")

    def get_quote(self, instrument_id):
        key = instrument_id.decode("utf-8") if isinstance(instrument_id, bytes) else instrument_id
        return self.data_storage.get(key)

    def stop(self):
        self.api.Release()
        self.api.Join()
