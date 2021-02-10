import sys
import hashlib
import hmac
import json
from time import time, sleep
import urllib.parse
from threading import Thread
from collections import deque

from requests import Request, Session
from requests.exceptions import HTTPError
from websocket import WebSocketApp
import pandas as pd
from pprint import pprint

import traceback

URL_REST_MAIN = 'https://api.bybit.com'
URL_REST_TEST = 'https://api-testnet.bybit.com'
URL_WS_MAIN = 'wss://stream.bybit.com/realtime'
URL_WS_TEST = 'wss://stream-testnet.bybit.com/realtime'
HEADER = {'Content-Type': 'application/json'}

CH_WS_TRADE = 'trade.'
CH_WS_INST = 'instrument_info.100ms.'
CH_WS_BOOK = 'orderBook_200.100ms.'
CH_WS_POS = 'position'
CH_WS_EXEC = 'execution'
CH_WS_ORDER = 'order'
RQ_POST = 'POST'
RQ_GET = 'GET'

EGS = 'python'

ID = 'id'
TPC = 'topic'
SNAP_SHOT = 'snap_shot'
TYP = 'type'
INS = 'insert'
UPD = 'update'
DEL = 'delete'
DAT = 'data'

PV_SUBSC = 'subscribe'

PL_OP = 'op'
PL_AUTH = 'auth'
PL_ARGS = 'args'
PL_SIGN = 'sign'
PL_API_KEY = 'api_key'
PL_TS = 'timestamp'
PL_RECEIVE_WINDOW = 'recv_window'

PL_ID = 'order_id'
PL_SIDE = 'side'
PL_SYM = 'symbol'
PL_TYPE = 'order_type'
PL_QTY = 'qty'
PL_PRICE = 'price'
PL_TIF = 'time_in_force'
PL_TP = 'take_profit'
PL_SL = 'stop_loss'
PL_OL_ID = 'order_link_id'
PL_SORT = 'sort'
PL_ORDER = 'order'
PL_PAGE = 'page'
PL_LIMIT = 'limit'
PL_STAT = 'order_status'
PL_BASE_PRICE = 'base_price'
PL_STOP_PX = 'stop_px'
PL_COT = 'close_on_trigger'
PL_REDUCE_ONLY = 'reduce_only'
PL_STOP_ID = 'stop_order_id'
PL_LEVERAGE = 'leverage'
PL_MARGIN = 'margin'
PL_INTERVAL = 'interval'
PL_FROM = 'from'
PL_RESULT = 'result'

TIF_GTC = 'GoodTillCancel'
TIF_PO = 'PostOnly'

BUY = 'Buy'
SELL = 'Sell'

UTF8 = 'utf-8'

D_OPEN_API = '/open-api'
D_USER = '/user'
D_POSITION = '/position'
D_V2 = '/v2'

D_ORDER = '/order'
D_STOP_ORDER = '/stop-order'
D_CHG_POS_MARGIN = '/change-position-margin'
D_FUNDING = '/funding'
D_PUB = '/public'
D_PRV = '/private'

D_LEVERAGE = '/leverage'
D_CREATE = '/create'
D_LIST = '/list'
D_CANCEL = '/cancel'
D_SAVE = '/save'
D_PFRATE = '/prev-funding-rate'
D_PFUNDING = '/prev-funding'
D_PREDF = '/predicted-funding'
D_EXEC = '/execution'
D_SYM = '/symbols'
D_KLINE = '/kline'
D_ORDER_BOOK = '/orderBook'
D_L2 = '/L2'

class Bybit():
    def __init__(self, api_key, secret, symbol, ws=True, test=False):
        self.api_key = api_key
        self.secret = secret

        self.symbol = symbol
        self.s = Session()
        self.s.headers.update(HEADER)

        self.url = URL_REST_MAIN if not test else URL_REST_TEST
        self.ws_url = URL_WS_MAIN if not test else URL_WS_TEST

        self.ws = ws
        if ws:
            self._connect()

    # WebSocket
    def _connect(self):
        self.ws = WebSocketApp(url=self.ws_url, on_open=self._on_open, on_message=self._on_message)
        self.ws_data = {CH_WS_TRADE + str(self.symbol): deque(maxlen=200),CH_WS_INST + str(self.symbol): {},CH_WS_BOOK + str(self.symbol): pd.DataFrame(),CH_WS_POS: {},CH_WS_EXEC: deque(maxlen=200),CH_WS_ORDER: deque(maxlen=200)}
        positions = self.get_position_http()[PL_RESULT]
        if positions!=None:
            for p in positions:
                if p[PL_SYM] == self.symbol:
                    self.ws_data[CH_WS_POS].update(p)
                    break

        Thread(target=self.ws.run_forever, daemon=True).start()

    def _send(self, args):
        #pprint(args)
        self.ws.send(json.dumps(args))

    def _ts(self):
        return int(time()*1000-100000)

    def _on_open(self):
        timestamp = self._ts()
        param_str = RQ_GET + '/realtime' + str(timestamp)
        sign = hmac.new(self.secret.encode(UTF8), param_str.encode(UTF8), hashlib.sha256).hexdigest()
        self._send({PL_OP: PL_AUTH, PL_ARGS: [self.api_key, timestamp, sign]})
        self._send({PL_OP: PV_SUBSC,PL_ARGS: [CH_WS_TRADE + str(self.symbol),CH_WS_BOOK + str(self.symbol),CH_WS_POS,CH_WS_EXEC,CH_WS_ORDER]})

    def _on_message(self, message):
        message = json.loads(message)
        topic = message.get(TPC)
        #pprint(topic)
        if topic == CH_WS_BOOK + str(self.symbol):
            data_set = message[DAT]
            if message[TYP] == SNAP_SHOT:
                pprint(CH_WS_BOOK + str(self.symbol)+':'+SNAP_SHOT)
                self.ws_data[topic] = pd.json_normalize(data_set).set_index(ID).sort_index(ascending=False)
            else:
                if len(data_set[DEL]) != 0:
                    drop_list = [x[ID] for x in data_set[DEL]]
                    self.ws_data[topic].drop(index=drop_list)
                elif len(data_set[UPD]) != 0:
                    update_list = pd.json_normalize(data_set[UPD]).set_index(ID)
                    self.ws_data[topic].update(update_list)
                    self.ws_data[topic] = self.ws_data[topic].sort_index(ascending=False)
                elif len(data_set[INS]) != 0:
                    insert_list = pd.json_normalize(data_set[INS]).set_index(ID)
                    self.ws_data[topic].update(insert_list)
                    self.ws_data[topic] = self.ws_data[topic].sort_index(ascending=False)

        elif topic in [CH_WS_TRADE + str(self.symbol), CH_WS_EXEC, CH_WS_ORDER]:
            self.ws_data[topic].append(data_set[0])

        elif topic in [CH_WS_INST + str(self.symbol), CH_WS_POS]:
            self.ws_data[topic].update(data_set[0])

    def get_trade(self):
        if not self.ws: return None
        return self.ws_data[CH_WS_TRADE + str(self.symbol)]

    def get_instrument(self):
        if not self.ws: return None
        while len(self.ws_data[CH_WS_INST + str(self.symbol)]) != 4:
            sleep(1.0)
        return self.ws_data[CH_WS_INST + str(self.symbol)]

    def get_orderbook(self, side=None):
        if not self.ws: return None
        topic = CH_WS_BOOK + str(self.symbol)
        if self.ws_data[topic].empty:
            recv_data = self.orderbookL2(self.symbol)[PL_RESULT]
            try:
                self.ws_data[topic] = pd.json_normalize(recv_data).set_index(PL_PRICE).sort_index(ascending=False)
            except Exception:
                pprint(recv_data)
                pprint(traceback.format_exc())

        if side == SELL:
            orderbook = self.ws_data[topic].query('side.str.contains("Sell")', engine=EGS)
        elif side == BUY:
            orderbook = self.ws_data[topic].query('side.str.contains("Buy")', engine=EGS)
        else:
            orderbook = self.ws_data[CH_WS_BOOK + str(self.symbol)]
        return orderbook

    def get_position(self):
        if not self.ws: return None
        return self.ws_data[CH_WS_POS]

    def get_my_executions(self):
        if not self.ws: return None
        return self.ws_data[CH_WS_EXEC]

    def get_order(self):
        if not self.ws: return None
        return self.ws_data[CH_WS_ORDER]

    # HTTP API
    def _request(self, method, path, payload):
        payload[PL_API_KEY] = self.api_key
        payload[PL_TS] = self._ts()
        payload[PL_RECEIVE_WINDOW] = 1000 * 500
        payload = dict(sorted(payload.items()))
        for k, v in list(payload.items()):
            if v is None:
                del payload[k]

        param_str = urllib.parse.urlencode(payload)
        sign = hmac.new(self.secret.encode(UTF8), param_str.encode(UTF8), hashlib.sha256).hexdigest()
        payload[PL_SIGN] = sign

        if method == RQ_GET:
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

    def place_active_order(self, side=None, symbol=None, order_type=None,qty=None, price=None,time_in_force=TIF_GTC, take_profit=None,stop_loss=None, order_link_id=None):
        payload = {PL_SIDE: side,PL_SYM: symbol if symbol else self.symbol,PL_TYPE: order_type,PL_QTY: qty,PL_PRICE: price,PL_TIF: time_in_force,PL_TP: take_profit,PL_SL: stop_loss,PL_OL_ID: order_link_id}
        return self._request(RQ_POST, D_OPEN_API + D_ORDER + D_CREATE, payload=payload)

    def get_active_order(self, order_id=None, order_link_id=None, symbol=None,sort=None, order=None, page=None, limit=None,order_status=None):
        payload = {PL_ID: order_id,PL_OL_ID: order_link_id,PL_SYM: symbol if symbol else self.symbol,PL_SORT: sort,PL_ORDER: order,PL_PAGE: page,PL_LIMIT: limit,PL_STAT: order_status}
        return self._request(RQ_GET, D_OPEN_API + D_ORDER + D_LIST, payload=payload)

    def cancel_active_order(self, order_id=None):
        return self._request(RQ_POST, D_OPEN_API + D_ORDER + D_CANCEL, payload={PL_ID: order_id})

    def place_conditional_order(self, side=None, symbol=None, order_type=None,qty=None, price=None, base_price=None,stop_px=None, time_in_force=TIF_GTC,close_on_trigger=None, reduce_only=None,order_link_id=None):
        payload = {PL_SIDE: side,PL_SYM: symbol if symbol else self.symbol,PL_TYPE: order_type,PL_QTY: qty,PL_PRICE: price,PL_BASE_PRICE: base_price,PL_STOP_PX: stop_px,PL_TIF: time_in_force,PL_COT: close_on_trigger,PL_REDUCE_ONLY: reduce_only,PL_OL_ID: order_link_id}
        return self._request(RQ_POST, D_OPEN_API + D_STOP_ORDER + D_CANCEL, payload=payload)

    def get_conditional_order(self, stop_order_id=None, order_link_id=None,symbol=None, sort=None, order=None, page=None,limit=None):
        payload = {PL_STOP_ID: stop_order_id,PL_OL_ID: order_link_id,PL_SYM: symbol if symbol else self.symbol,PL_SORT: sort,PL_ORDER: order,PL_PAGE: page,PL_LIMIT: limit}
        return self._request(RQ_GET, D_OPEN_API + D_STOP_ORDER + D_LIST, payload=payload)

    def cancel_conditional_order(self, order_id=None):
        return self._request(RQ_POST, D_OPEN_API + D_STOP_ORDER + D_CANCEL, payload={PL_ID: order_id})

    def get_leverage(self):
        return self._request(RQ_GET, D_USER + D_LEVERAGE, payload={})

    def change_leverage(self, symbol=None, leverage=None):
        payload = {PL_SYM: symbol if symbol else self.symbol,PL_LEVERAGE: leverage}
        return self._request(RQ_POST, D_USER + D_LEVERAGE + D_SAVE, payload=payload)

    def get_position_http(self):
        return self._request(RQ_GET, D_POSITION + D_LIST, payload={})

    def change_position_margin(self, symbol=None, margin=None):
        payload = {PL_SYM: symbol if symbol else self.symbol,PL_MARGIN: margin}
        return self._request(RQ_POST, D_POSITION + D_CHG_POS_MARGIN, payload=payload)

    def get_prev_funding_rate(self, symbol=None):
        payload = {PL_SYM: symbol if symbol else self.symbol,}
        return self._request(RQ_GET, D_OPEN_API + D_FUNDING + D_PFRATE, payload=payload)

    def get_prev_funding(self, symbol=None):
        payload = {PL_SYM: symbol if symbol else self.symbol,}
        return self._request(RQ_GET, D_OPEN_API + D_FUNDING + D_PFUNDING, payload=payload)

    def get_predicted_funding(self, symbol=None):
        payload = {PL_SYM: symbol if symbol else self.symbol,}
        return self._request(RQ_GET, D_OPEN_API + D_FUNDING + D_PREDF, payload=payload)

    def get_my_execution(self, order_id=None):
        payload = {PL_ID: order_id}
        return self._request(RQ_GET, D_V2 + D_PRV + D_EXEC + D_LIST, payload=payload)

    # New http API
    def symbols(self):
        return self._request(RQ_GET, D_V2 + D_PUB + D_SYM, payload={})

    def orderbookL2(self, symbol=None):
        return self._request(RQ_GET, D_V2 + D_PUB + D_ORDER_BOOK + D_L2, payload={PL_SYM: symbol if symbol else self.symbol})

    def kline(self, symbol=None, interval=None, _from=None, limit=None):
        payload = {PL_SYM: symbol if symbol else self.symbol,PL_INTERVAL: interval,PL_FROM: _from,PL_LIMIT: limit}
        return self._request(RQ_GET, D_V2 + D_PUB + D_KLINE + D_LIST, payload=payload)

    def place_active_order_v2(self, symbol=None, side=None, order_type=None,qty=None, price=None,time_in_force=TIF_GTC,order_link_id=None):
        payload = {PL_SYM: symbol if symbol else self.symbol,PL_SIDE: side,PL_TYPE: order_type,PL_QTY: qty,PL_PRICE: price,PL_TIF: time_in_force,PL_OL_ID: order_link_id}
        return self._request(RQ_POST, D_V2 + D_PRV + D_ORDER + D_CREATE, payload=payload)

    def cancel_active_order_v2(self, order_id=None):
        payload = {PL_ID: order_id}
        return self._request(RQ_POST, D_V2 + D_PRV + D_ORDER + D_CANCEL, payload=payload)
