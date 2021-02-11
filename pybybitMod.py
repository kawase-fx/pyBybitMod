import sys
import hashlib
import hmac
import json
from time import time, sleep
import urllib.parse
from threading import Thread
from collections import deque
from pprint import pprint
import traceback

from requests import Request, Session
from requests.exceptions import HTTPError
from websocket import WebSocketApp
import pandas as pd

class Constants():
    ENGINE = 'python'
    UTF8 = 'utf-8'

    ID = 'id'
    TPC = 'topic'
    SNAP_SHOT = 'snap_shot'
    TYP = 'type'

    INS = 'insert'
    UPD = 'update'
    DEL = 'delete'
    DAT = 'data'

    HEADER = {'Content-Type': 'application/json'}

class REST():
    MAIN = 'https://api.bybit.com'
    TEST = 'https://api-testnet.bybit.com'
    POST = 'POST'
    GET = 'GET'

class WS():
    MAIN = 'wss://stream.bybit.com/realtime'
    TEST = 'wss://stream-testnet.bybit.com/realtime'
    class CH():
        TRADE = 'trade.'
        INST = 'instrument_info.100ms.'
        BOOK = 'orderBook_200.100ms.'
        POS = 'position'
        EXEC = 'execution'
        ORDER = 'order'

class SIDE():
    BUY = 'Buy'
    SELL = 'Sell'

class ORDER_TYPE():
    LIMIT = 'Limit'
    MARKET = 'Market'

class ORDER_STATUS():
    CREATED = 'Created'  # order accepted by the system but not yet put through matching engine
    REJECTED = 'Rejected'
    NEW = 'New'  # order has placed successfully
    PARTIALLY_FILLED = 'PartiallyFilled'
    FILLED = 'Filled'
    CANCELED = 'Cancelled'
    PENDING_CANCEL = 'PendingCancel'  # the matching engine has received the cancellation but there is no guarantee that it will be successful

class TIME_IN_FORCE():
    GOOD_TILL_CANCEL = 'GoodTillCancel'
    POST_ONLY = 'PostOnly'

class ENTRY_POINT():
    REALTIME = '/realtime'
    OPEN_API = '/open-api'
    USER = '/user'
    POSITION = '/position'
    V2 = '/v2'

    ORDER = '/order'
    STOP_ORDER = '/stop-order'
    CHG_POS_MARGIN = '/change-position-margin'
    FUNDING = '/funding'
    PUB = '/public'
    PRV = '/private'

    LEVERAGE = '/leverage'
    CREATE = '/create'
    LIST = '/list'
    CANCEL = '/cancel'
    SAVE = '/save'
    PFRATE = '/prev-funding-rate'
    PFUNDING = '/prev-funding'
    PREDF = '/predicted-funding'
    EXEC = '/execution'
    SYMBOLS = '/symbols'
    KLINE = '/kline'
    ORDER_BOOK = '/orderBook'
    TICKER = '/tickers'
    L2 = '/L2'

E = ENTRY_POINT()

class Properties():
    SUBSC = 'subscribe'

    OP = 'op'
    AUTH = 'auth'
    ARGS = 'args'
    SIGN = 'sign'
    API_KEY = 'api_key'
    TS = 'timestamp'
    RECEIVE_WINDOW = 'recv_window'

    ID = 'order_id'
    SIDE = 'side'
    SYM = 'symbol'
    TYPE = 'order_type'
    QTY = 'qty'
    PRICE = 'price'
    TIF = 'time_in_force'
    TP = 'take_profit'
    SL = 'stop_loss'
    OL_ID = 'order_link_id'
    DIRECTION = 'direction'
    CURSOR = 'cursor'
    SORT = 'sort'
    ORDER = 'order'
    PAGE = 'page'
    LIMIT = 'limit'
    STAT = 'order_status'
    BASE_PRICE = 'base_price'
    LAST_PRICE = 'last_price'
    STOP_PX = 'stop_px'
    COT = 'close_on_trigger'
    REDUCE_ONLY = 'reduce_only'
    STOP_ID = 'stop_order_id'
    LEVERAGE = 'leverage'
    MARGIN = 'margin'
    INTERVAL = 'interval'
    FROM = 'from'
    RESULT = 'result'

P = Properties()

class Bybit():
    def __init__(self, api_key, secret, symbol, ws=True, test=False):
        self.api_key = api_key
        self.secret = secret

        self.symbol = symbol
        self.s = Session()
        self.s.headers.update(Constants.HEADER)

        self.url = REST.MAIN if not test else REST.TEST
        self.ws_url = WS.MAIN if not test else WS.TEST

        self.last_trade_price = float(self.get_ticker(self.symbol)[P.RESULT][-1][P.LAST_PRICE])

        self.ws = ws
        if ws:
            self._connect()

    # WebSocket
    def _connect(self):
        self.ws = WebSocketApp(url=self.ws_url, on_open=self._on_open, on_message=self._on_message)
        self.ws_data = {WS.CH.TRADE + str(self.symbol): deque(maxlen=200),WS.CH.INST + str(self.symbol): {},WS.CH.BOOK + str(self.symbol): pd.DataFrame(),WS.CH.POS: {},WS.CH.EXEC: deque(maxlen=200),WS.CH.ORDER: deque(maxlen=200)}
        positions = self.get_position_list()
        if positions!=None:
            self.ws_data[WS.CH.POS].update(positions)

        Thread(target=self.ws.run_forever, daemon=True).start()

    def _send(self,args):
        #pprint(args)
        self.ws.send(json.dumps(args))

    def _sign(self,param_str):
        return hmac.new(self.secret.encode(Constants.UTF8), param_str.encode(Constants.UTF8), hashlib.sha256).hexdigest()

    def _ts(self,offsetMs):
        return int(time()*1000+offsetMs)

    def _on_open(self):
        timestamp = self._ts(1000)
        param_str = REST.GET+E.REALTIME+str(timestamp)
        sign = self._sign(param_str)
        self._send({P.OP: P.AUTH,P.ARGS: [self.api_key,timestamp,sign]})
        self._send({P.OP: P.SUBSC,P.ARGS: [WS.CH.TRADE+str(self.symbol),WS.CH.BOOK+str(self.symbol),WS.CH.POS,WS.CH.EXEC,WS.CH.ORDER]})

    def _on_message(self, incoming):
        message = json.loads(incoming)
        topic = message.get(Constants.TPC)
        if topic == WS.CH.BOOK + str(self.symbol):
            data_set = message[Constants.DAT]
            if message[Constants.TYP] == Constants.SNAP_SHOT:
                pprint(WS.CH.BOOK + str(self.symbol)+':'+Constants.SNAP_SHOT)
                self.ws_data[topic] = pd.json_normalize(data_set).set_index(Constants.ID).sort_index(ascending=False)
            else:
                if len(data_set[Constants.DEL]) != 0:
                    drop_list = [x[Constants.ID] for x in data_set[Constants.DEL]]
                    self.ws_data[topic].drop(index=drop_list)
                elif len(data_set[Constants.UPD]) != 0:
                    update_list = pd.json_normalize(data_set[Constants.UPD]).set_index(Constants.ID)
                    self.ws_data[topic].update(update_list)
                    self.ws_data[topic] = self.ws_data[topic].sort_index(ascending=False)
                elif len(data_set[Constants.INS]) != 0:
                    insert_list = pd.json_normalize(data_set[Constants.INS]).set_index(Constants.ID)
                    self.ws_data[topic].update(insert_list)
                    self.ws_data[topic] = self.ws_data[topic].sort_index(ascending=False)

        elif topic in [WS.CH.TRADE + str(self.symbol), WS.CH.EXEC, WS.CH.ORDER]:
            self.ws_data[topic].append(message[Constants.DAT][0])
            if topic == WS.CH.TRADE + str(self.symbol):
                self.last_trade_price = float(message[Constants.DAT][-1][P.PRICE])

        elif topic in [WS.CH.INST + str(self.symbol), WS.CH.POS]:
            self.ws_data[topic].update(message[Constants.DAT][0])

    def get_trade(self):
        if not self.ws: return None
        return self.ws_data[WS.CH.TRADE + str(self.symbol)]

    def get_instrument(self):
        if not self.ws: return None
        while len(self.ws_data[WS.CH.INST + str(self.symbol)]) != 4:
            sleep(1.0)
        return self.ws_data[WS.CH.INST + str(self.symbol)]

    def get_orderbook(self, side=None):
        if not self.ws: return None
        topic = WS.CH.BOOK + str(self.symbol)
        if self.ws_data[topic].empty:
            recv_data = self.orderbookL2(self.symbol)[P.RESULT]
            try:
                self.ws_data[topic] = pd.json_normalize(recv_data).set_index(P.PRICE).sort_index(ascending=False)
            except Exception:
                pprint(recv_data)
                pprint(traceback.format_exc())

        if side == SIDE.SELL:
            orderbook = self.ws_data[topic].query('side.str.contains("Sell")', engine=Constants.ENGINE)
        elif side == SIDE.BUY:
            orderbook = self.ws_data[topic].query('side.str.contains("Buy")', engine=Constants.ENGINE)
        else:
            orderbook = self.ws_data[WS.CH.BOOK + str(self.symbol)]
        return orderbook

    def get_position(self):
        if not self.ws: return None
        return self.ws_data[WS.CH.POS]

    def get_my_executions(self):
        if not self.ws: return None
        return self.ws_data[WS.CH.EXEC]

    def get_order(self):
        if not self.ws: return None
        return self.ws_data[WS.CH.ORDER]

    # HTTP API
    def _request(self, method, path, payload):
        payload[P.API_KEY] = self.api_key
        payload[P.TS] = self._ts(-100000)
        payload[P.RECEIVE_WINDOW] = 1000 * 500
        payload = dict(sorted(payload.items()))
        for k, v in list(payload.items()):
            if v is None:
                del payload[k]

        param_str = urllib.parse.urlencode(payload)
        sign = self._sign(param_str)
        payload[P.SIGN] = sign

        if method == REST.GET:
            query = payload
            body = None
        else:
            query = None
            body = json.dumps(payload)

        req = Request(method, self.url + path, data=body, params=query)
        prepped = self.s.prepare_request(req)

        resp = None
        try:
            resp = self.s.send(prepped)
            resp.raise_for_status()
        except HTTPError as e:
            pprint(e)

        try:
            return resp.json()
        except json.decoder.JSONDecodeError as e:
            pprint('json.decoder.JSONDecodeError: ', e)
            return resp.text

    def get_ticker(self, symbol=None):
        payload = {P.SYM: symbol if symbol else self.symbol,}
        return self._request(REST.GET, E.V2 + E.PUB + E.TICKER, payload=payload)

    def place_conditional_order(self, side=None, symbol=None, order_type=None, qty=None, price=None, base_price=None, stop_px=None, time_in_force=TIME_IN_FORCE.GOOD_TILL_CANCEL,close_on_trigger=None, reduce_only=None, order_link_id=None):
        payload = {P.SIDE: side,P.SYM: symbol if symbol else self.symbol,P.TYPE: order_type,P.QTY: qty,P.PRICE: price,P.BASE_PRICE: base_price,P.STOP_PX: stop_px,P.TIF: time_in_force,P.COT: close_on_trigger,P.REDUCE_ONLY: reduce_only,P.OL_ID: order_link_id}
        return self._request(REST.POST, E.OPEN_API + E.STOP_ORDER + E.CANCEL, payload=payload)

    def get_conditional_order(self, stop_order_id=None, order_link_id=None, symbol=None, sort=None, order=None, page=None,limit=None):
        payload = {P.STOP_ID: stop_order_id,P.OL_ID: order_link_id,P.SYM: symbol if symbol else self.symbol,P.SORT: sort,P.ORDER: order,P.PAGE: page,P.LIMIT: limit}
        return self._request(REST.GET, E.OPEN_API + E.STOP_ORDER + E.LIST, payload=payload)

    def cancel_conditional_order(self, order_id=None):
        return self._request(REST.POST, E.OPEN_API + E.STOP_ORDER + E.CANCEL, payload={P.ID: order_id})

    def get_leverage(self):
        return self._request(REST.GET, E.USER + E.LEVERAGE, payload={})

    def change_leverage(self, symbol=None, leverage=None):
        payload = {P.SYM: symbol if symbol else self.symbol,P.LEVERAGE: leverage}
        return self._request(REST.POST, E.USER + E.LEVERAGE + E.SAVE, payload=payload)

    def get_position_list(self):
        payload = {P.SYM: self.symbol}
        return self._request(REST.GET, E.POSITION + E.LIST, payload=payload)[P.RESULT][-1]

    def change_position_margin(self, symbol=None, margin=None):
        payload = {P.SYM: symbol if symbol else self.symbol,P.MARGIN: margin}
        return self._request(REST.POST, E.POSITION + E.CHG_POS_MARGIN, payload=payload)

    def get_prev_funding_rate(self, symbol=None):
        payload = {P.SYM: symbol if symbol else self.symbol,}
        return self._request(REST.GET, E.OPEN_API + E.FUNDING + E.PFRATE, payload=payload)

    def get_prev_funding(self, symbol=None):
        payload = {P.SYM: symbol if symbol else self.symbol,}
        return self._request(REST.GET, E.OPEN_API + E.FUNDING + E.PFUNDING, payload=payload)

    def get_predicted_funding(self, symbol=None):
        payload = {P.SYM: symbol if symbol else self.symbol,}
        return self._request(REST.GET, E.OPEN_API + E.FUNDING + E.PREDF, payload=payload)

    def get_my_execution(self, order_id=None):
        payload = {P.ID: order_id}
        return self._request(REST.GET, E.V2 + E.PRV + E.EXEC + E.LIST, payload=payload)

    def symbols(self):
        return self._request(REST.GET, E.V2 + E.PUB + E.SYMBOLS, payload={})

    def orderbookL2(self, symbol=None):
        return self._request(REST.GET, E.V2 + E.PUB + E.ORDER_BOOK + E.L2, payload={P.SYM: symbol if symbol else self.symbol})

    def kline(self, symbol=None, interval=None, _from=None, limit=None):
        payload = {P.SYM: symbol if symbol else self.symbol,P.INTERVAL: interval,P.FROM: _from,P.LIMIT: limit}
        return self._request(REST.GET, E.V2 + E.PUB + E.KLINE + E.LIST, payload=payload)

    def place_active_order(self,side=None,order_type=None,qty=None,price=None,time_in_force=TIME_IN_FORCE.GOOD_TILL_CANCEL,order_link_id=None):
        payload = {P.SYM: self.symbol,P.SIDE: side,P.TYPE: order_type,P.QTY: qty,P.PRICE: price,P.TIF: time_in_force,P.OL_ID: order_link_id}
        return self._request(REST.POST, E.V2 + E.PRV + E.ORDER + E.CREATE, payload=payload)

    def cancel_active_order(self, order_id=None):
        r = self._request(REST.POST, E.V2 + E.PRV + E.ORDER + E.CANCEL, payload={P.SYM:self.symbol,P.ID: order_id})
        if P.RESULT in r:
            return r[P.RESULT]
        else:
            pprint(r)
            return r

    def get_active_order(self,order_status=None,direction=None,limit=None,cursor=None):
        payload = {P.SYM:self.symbol,P.STAT:order_status,P.DIRECTION:direction,P.LIMIT:limit,P.CURSOR:cursor}
        return self._request(REST.GET, E.V2 + E.PRV + E.ORDER + E.LIST, payload=payload)[P.RESULT][Constants.DAT]
