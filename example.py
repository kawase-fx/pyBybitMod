import sys

import json
from time import time, sleep
import pandas as pd
from pprint import pprint
import traceback

from pybybitMod import Bybit

# MAIN
if __name__ == '__main__':
    with open( sys.argv[ 1 ] ) as conf_file:
        tokens = conf_file.readlines()
    bybit = Bybit(api_key=tokens[0].rstrip(),secret=tokens[1].rstrip(),symbol=tokens[3].rstrip(),test=tokens[2].rstrip()=='True',ws=True)

    # ポジションを取得
    position = bybit.get_position()
    print('Position ----------------------------------------------------------')
    print(json.dumps(position, indent=2))

    # 板情報を取得
    orderbook_buy = bybit.get_orderbook(side='Buy')
    print('Orderbook (Buy) ---------------------------------------------------')
    print(orderbook_buy.head(5))
    try:
        best_buy = float(orderbook_buy.iloc[0].name)

        # オーダーを送信
        print('Sending Order... --------------------------------------------------')
        order_resp = bybit.place_active_order(side='Buy', order_type='Limit', qty=1, price=best_buy - 100, time_in_force='PostOnly')
        print(json.dumps(order_resp, indent=2))
        order_id = order_resp['result']['order_id'] if order_resp['result'] else None

        sleep(5.0)

        # オーダーをキャンセル
        print('Cancel Order... ---------------------------------------------------')
        cancel_resp = bybit.cancel_active_order(order_id=order_id)
        print(json.dumps(cancel_resp, indent=2))

    except Exception:
        print(traceback.format_exc())
