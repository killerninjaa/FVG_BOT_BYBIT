from pybit.unified_trading import HTTP
import time
import threading
import logging
import math
from requests.exceptions import ConnectionError
from api_keys import API_KEY, API_SECRET

logging.basicConfig(
    level=logging.INFO,
    filename='pybit.log',
    format='%(asctime)s - %(levelname)s - %(message)s'
)

spot_client = HTTP(
    testnet=True,
    api_key=API_KEY,
    api_secret=API_SECRET,
)

KLINE_KEYS = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'turnover']
PAIR = "BTCUSDT"
TIME_FRAME = "15"
TIME_FRAME_MS = 900000
FVG_DICT = {'low': [], 'high': []}
COVER_NEIGHBORS_BULL = 3
COVER_NEIGHBORS_BEAR = 3
EXPAND_NEIGHBORS_BULL = 3
EXPAND_NEIGHBORS_BEAR = 3
START_TRADE = 0.2
STOP_LOSS_OFFSET = 0.1
RISK_REWARD_RATIO = 2
RISK = 0.03
LEVER = 1
MAX_TRADE_DURATION = 9000000
MAX_ORDER_DURATION = 9000000


def get_coin_balance(coin):
    '''
    Функция возвращает балансы
    Если передается coin, то баланс конретной монеты
    В противном случае все ненулевые балансы
    '''
    try:
        if coin:
            response = spot_client.get_wallet_balance(accountType="UNIFIED", coin=coin)
        else:
            response = spot_client.get_wallet_balance(accountType="UNIFIED")
        if response['retMsg'] == "OK":
            if coin:
                balance = float(response['result']['list'][0]['coin'][0]['walletBalance'])
            else:
                balance = response['result']['list'][0]['coin']
            return balance
        else:
            logging.info(f'Error getting balance: {response["retMsg"]}') 
            return False
    except ConnectionError as e:
        logging.info(f'Error getting klines: {e}')
        return False 


def get_klines(pair, time_frame, limit):
    '''
    Функция возвращает свечи в виде словаря
    Принимает торговую пару, тайм фрейм, количество свечей
    '''
    try:
        response = spot_client.get_kline(category="spot", symbol=pair, interval=time_frame, limit=limit)
        if response['retMsg'] == "OK":
            klines_list = response["result"]["list"]
            klines_list = list(map(lambda sublist: [float(x) if isinstance(x, str) else x for x in sublist[::-1]], zip(*klines_list)))
            klines_dict = dict(zip(KLINE_KEYS, klines_list))
            return klines_dict
        else:
            logging.info(f'Error getting klines: {response["retMsg"]}')   
            return False 
    except ConnectionError as e:
        logging.info(f'Error getting klines: {e}')
        return False


def check_if_bear_fvg(klines):
    '''
    Функция принимает на вход 4 свечи и 
    ищет медвежью FVG на первых 3 свечах
    '''
    if klines['low'][0] > klines['high'][2]:
        logging.info('Found bear FVG')
        return True
    return False


def check_if_bull_fvg(klines):
    '''
    Функция принимает на вход 4 свечи и 
    ищет бычью FVG на первых 3 свечах
    '''
    if klines['high'][0] < klines['low'][2]:
        logging.info('Found bull FVG')
        return True
    return False


def append_fvg(klines, bear_fvg_flag, bull_fvg_flag):
    '''
    Функция добавляет найденную FVG в словарь 
    '''
    if bull_fvg_flag:
        FVG_DICT['low'].append(klines['high'][0])
        FVG_DICT['high'].append(klines['low'][2])
    elif bear_fvg_flag:
        FVG_DICT['low'].append(klines['high'][2])
        FVG_DICT['high'].append(klines['low'][0])


def cover_fvg(klines, bear_fvg_flag, bull_fvg_flag):
    '''
    Функция проверяет, не перекрывается ли FVG противоположной свечой
    '''
    if bull_fvg_flag:
        if klines['low'][0] < FVG_DICT['low'][-1]:  # если нижняя граница FVG больше минимума свечи
            logging.info('Covered FVG')
            return True
    elif bear_fvg_flag:
        if klines['high'][0] > FVG_DICT['high'][-1]:  # если максимум свечи выше верхней границы FVG
            logging.info('Covered FVG')
            return True
    return False


def delete_fvg():
    '''
    Функция для удаления FVG из словаря
    '''
    FVG_DICT['low'].pop()
    FVG_DICT['high'].pop()


def expand_fvg(klines, bear_fvg_flag, bull_fvg_flag):
    '''
    Функция для расширения FVG
    '''
    if bull_fvg_flag:
        if FVG_DICT['high'][-1] < klines['low'][0]:  # если верхняя грань FVG меньше минимума следующей свечи
            FVG_DICT['high'].pop()
            FVG_DICT['high'].append(klines['low'][0])
            logging.info('Expanded FVG')
    elif bear_fvg_flag:
        if FVG_DICT['low'][-1] > klines['high'][0]:  # если нижняя граница FVG выше максимума следуюшей свечи
            FVG_DICT['low'].pop()
            FVG_DICT['low'].append(klines['high'][0])  
            logging.info('Expanded FVG')


def send_order(order_params):
    '''
    Функция для отправки ордера на биржу
    Принимает на вход словарь с параметрами
    '''
    try:
        response = spot_client.place_order(**order_params)
        if response['retMsg'] == "OK":
            return True
        else:
            logging.info(f'Error placing order: {response["retMsg"]}')   
            return False 
    except ConnectionError as e:
        logging.info(f'Error placing order: {e}')
        return False


def calc_order_params(bear_fvg_flag, bull_fvg_flag):
    '''
    Функция для расчета параметров ордера
    Возвращает словарь, который передается в функцию отправки ордера
    '''
    if bull_fvg_flag:
        open_price = FVG_DICT['high'][-1] - START_TRADE*(FVG_DICT['high'][-1] - FVG_DICT['low'][-1])  # считаем цену открытия сделки
        # для бычьего FVG: верхняя граница - % от FVG, который мы должны пересечь 
        sl = FVG_DICT['low'][-1]*(1 - STOP_LOSS_OFFSET)  # считаем стоп-лосс
        # для бычьего FVG: нижняя граница - % 
        tp = open_price + (open_price - sl) * RISK_REWARD_RATIO #считаем тейк-профит
        # для бычьего FVG: цена открытия + (расстояние от цены открытия до стоп-лосса)*соотношение риска к прибыли
        balance = get_coin_balance("USDT") # получаем баланс USDT
        if not balance:
            logging.info('Error getting balance, skip FVG')
            return False
        size = balance*RISK/(open_price-sl)*open_price*LEVER / open_price # считаем количество ордера
    elif bear_fvg_flag:
        open_price = FVG_DICT['low'][-1] + START_TRADE*(FVG_DICT['high'][-1] - FVG_DICT['low'][-1]) #считаем цену открытия сделки
        # для медвежьего FVG: нижняя граница + % от FVG, который мы должны пересечь
        sl = FVG_DICT['high'][-1]*(1 + STOP_LOSS_OFFSET)  # считаем стоп-лосс
        # для медвежьего FVG: верхняя граница + % 
        tp = open_price - (sl - open_price) * RISK_REWARD_RATIO #считаем тейк-профит
        # для медвежьего FVG: цена открытия - (расстояние от цены открытия до стоп-лосса)*соотношение риска к прибыли
        balance = get_coin_balance(PAIR[:-4]) # получаем баланс коина
        logging.info(f'Coin balance: {balance}')
        if not balance:
            logging.info('Error getting balance, skip FVG')
            return False
        size = balance*RISK/(sl-open_price)*sl*LEVER # считаем количество ордера
    order_params = {
            "category": "spot",
            "symbol": PAIR,
            "side": "Buy" if bull_fvg_flag else "Sell",
            "orderType": "LIMIT",
            "timeInForce": "GTC",
            "marketUnit": "quoteCoin" if bull_fvg_flag else "baseCoin",
            "qty": size,
            "price": open_price,
            "takeProfit": tp,
            "stopLoss": sl,
            "slOrderType": "Market",
            "tpOrderType": "Market"       
            } 
    logging.info(f'Order params. Size: {size}, Price: {open_price}, tp: {tp}, sl: {sl}')   
    return order_params


def get_order_filters():
    '''
    Функция получает фильтры для ордера
    '''
    try:
        response = spot_client.get_instruments_info(category="spot", symbol=PAIR, status='Trading')
        if response['retMsg'] == "OK":
            order_filters = {'base_prec': float(response['result']['list'][0]['lotSizeFilter']['basePrecision']),
                             'quote_prec': float(response['result']['list'][0]['lotSizeFilter']['quotePrecision']),
                             'min_quan': float(response['result']['list'][0]['lotSizeFilter']['minOrderQty']), 
                             'max_quan': float(response['result']['list'][0]['lotSizeFilter']['maxOrderQty']), 
                             'min_amount': float(response['result']['list'][0]['lotSizeFilter']['minOrderAmt']), 
                             'max_amount': float(response['result']['list'][0]['lotSizeFilter']['maxOrderAmt']),
                             'price_prec': float(response['result']['list'][0]['priceFilter']['tickSize'])}
            logging.info(f'Order filter params. Base_prec: {order_filters["base_prec"]}, quote_prec: {order_filters["quote_prec"]}')
            return order_filters
        else:
            logging.info(f'Error getting order filters: {response["retMsg"]}')   
            return False 
    except ConnectionError as e:
        logging.info(f'Error getting order filters: {e}')
        return False
    

def check_order_params(order_params, order_filters, bear_fvg_flag, bull_fvg_flag):
    '''
    Функция проверяет параметры ордера и приводит их к правильному виду
    '''
    if bear_fvg_flag:
        # приводим количество ордера к необходимой точности
        order_params['qty'] = str(round(int(order_params['qty'] / order_filters['base_prec']) * order_filters['base_prec'], 2))
    elif bull_fvg_flag:
        # приводим количество ордера к необходимой точности
        order_params['qty'] = str(round(int(order_params['qty'] / order_filters['quote_prec']) * order_filters['quote_prec'], 2))    
    # приводим цену ордера к необходимой точности    
    order_params['price'] = str(round(int(order_params['price'] / order_filters['price_prec']) * order_filters['price_prec'], 2)) 
    # приводим тейк-профит ордера к необходимой точности
    order_params['takeProfit'] = str(round(int(order_params['takeProfit'] / order_filters['price_prec']) * order_filters['price_prec'], 2))
    # приводим стоп-лосс ордера к необходимой точности
    order_params['stopLoss'] = str(round(int(order_params['stopLoss'] / order_filters['price_prec']) * order_filters['price_prec'], 2))
    logging.info(f'Order params after validation. Size: {order_params["qty"]}, Price: {order_params["price"]}, tp: {order_params["takeProfit"]}, sl: {order_params["stopLoss"]}')
    # проверка на границы по количеству ордера
    if float(order_params['qty']) > order_filters['max_quan'] or float(order_params['qty']) < order_filters['min_quan']:
        return False
    # проверка на границы по стоимости ордера
    if float(order_params['qty']) * float(order_params['price']) > order_filters['max_amount'] or float(order_params['qty']) * float(order_params['price']) < order_filters['min_amount']:
        return False
    return order_params


def get_orders():
    '''
    Функция возвращает открытые ордера
    '''
    try:
        response = spot_client.get_open_orders(category="spot", symbol=PAIR)
        if response['retMsg'] == "OK":
            orders = response['result']['list']
            logging.info("Got open orderds")
            return orders
        else:
            logging.info(f'Error getting orders: {response["retMsg"]}')   
            return False 
    except ConnectionError as e:
        logging.info(f'Error getting orders: {e}')
        return False
    

def delete_order(orderId):
    '''
    Функция отмены ордеров
    '''
    try:
        response = spot_client.cancel_order(category="spot", symbol=PAIR, orderId=orderId)
        if response['retMsg'] == "OK":
            logging.info("Order cancelled")
            return True
        else:
            logging.info(f'Error cancelling order: {response["retMsg"]}')   
            return False 
    except ConnectionError as e:
        logging.info(f'Error cancelling order: {e}')
        return False
    

def trade():
    '''
    Основная функция торговли
    '''
    logging.info('Getting order filters...')
    order_filters = get_order_filters() # получаем фильтры для ордера
    if not order_filters:
        logging.info('Stopping bot due to getting order filters errors')
        return
    logging.info('Got the orders filters!')
    while True:
        logging.info('Bot runing')
        klines = get_klines(PAIR, TIME_FRAME, limit=4) # получаем 4 последних свечи
        if not klines:
            logging.info('Stopping bot due to getting candles error')
            break
        logging.info(f'Got 4 candles: {klines}')
        bear_fvg_flag = check_if_bear_fvg(klines) # проверка на медвежий FVG
        bull_fvg_flag = check_if_bull_fvg(klines) # проверка на бычий FVG
        # если обнаружили FVG
        if bear_fvg_flag or bull_fvg_flag:
            append_fvg(klines, bear_fvg_flag, bull_fvg_flag) # добавляем FVG в словарь
            logging.info('FVG added to dict')
            cover_neighbors_counter = COVER_NEIGHBORS_BULL if bull_fvg_flag else COVER_NEIGHBORS_BEAR
            expand_neighbors_counter = EXPAND_NEIGHBORS_BULL if bull_fvg_flag else EXPAND_NEIGHBORS_BEAR
            # запускаем цикл проверки на перекрытие FVG и расширения FVG
            while cover_neighbors_counter > 0 or expand_neighbors_counter > 0:
                logging.info('Enter cycle of expanding and covering')
                # ждем закрытия текущей свечи
                sleeping_time = max(0, math.ceil((klines['open_time'][-1] + TIME_FRAME_MS - int(time.time() * 1000)) / 1000) + 2)
                logging.info(f'Sleep for {sleeping_time}')
                time.sleep(sleeping_time)
                logging.info('Waked up')
                # получаем 2 свечи
                klines = get_klines(PAIR, TIME_FRAME, limit=2)
                if not klines:
                    logging.info('Stopping bot due to candles error')
                    break
                logging.info('Got 2 candles')
                if expand_neighbors_counter > 0:
                    logging.info('Expanding FVG')
                    # проверка на расширение FVG
                    expand_fvg(klines, bear_fvg_flag, bull_fvg_flag)
                if cover_neighbors_counter > 0:
                    logging.info('Covering FVG')
                    # проверка на перекрытие FVG
                    cover_flag = cover_fvg(klines, bear_fvg_flag, bull_fvg_flag)
                    # если FVG перекрывается
                    if cover_flag:
                        delete_fvg() # удаляем FVG из словаря и опускаем флаги
                        logging.info('Deleted FVG')
                        bear_fvg_flag = False
                        bull_fvg_flag = False
                        logging.info('Leave cycle of expanding and covering')
                        break
                cover_neighbors_counter -= 1
                expand_neighbors_counter -= 1
            # если не перекрыли FVG
            if not cover_flag:
                logging.info('FVG doesnt cover')
                logging.info('Calc params of order')
                order_params = calc_order_params(bear_fvg_flag, bull_fvg_flag) # считаем параметры ордера
                logging.info('Params calculated')
                if order_params:
                    logging.info('Checking params for filters')
                    # проверяем параметры и форматируем их
                    legit_order_params = check_order_params(order_params, order_filters, bear_fvg_flag, bull_fvg_flag)
                    if not legit_order_params:
                        logging.info('Params dont pass filters')
                    # если параметры прошли фильтры, отправляем ордер    
                    else:   
                        logging.info('Params passed filters') 
                        if not send_order(order_params):
                            logging.info('Error placing order')
                        else:    
                            logging.info('Order placed')
            # опускаем флаги и спим до закрытия текущей свечи и еще двух следующих
            bear_fvg_flag = False
            bull_fvg_flag = False
            sleeping_time = max(0, math.ceil((klines['open_time'][-1] + TIME_FRAME_MS - int(time.time() * 1000)) / 1000) + 2 + 2*TIME_FRAME_MS // 1000)   
            logging.info(f'Sleep for 2 candles: {sleeping_time}') 
            time.sleep(sleeping_time)
            logging.info('Waked up')
        # если не обнаружили FVG    
        else:
            # ждем закрытия текущей свечи
            sleeping_time = max(0, math.ceil((klines['open_time'][-1] + TIME_FRAME_MS - int(time.time() * 1000)) / 1000) + 2)
            logging.info(f'Sleep for {sleeping_time}')
            time.sleep(sleeping_time)    
            logging.info('Waked up')


def order_canceller():
    '''
    Функция мониторинга ордеров
    '''
    while True:    
        logging.info('Trying to get orders')
        orders = get_orders() # получаем ордера
        if not orders:
            logging.info('Cant get orders or there is no orders')
        else:
            # проходим по ордерам
            for order in orders:
                # если это TP/SL ордер
                if order['stopOrderType'] == 'BidirectionalTpslOrder':
                    # если он висит больше разрешенного времени
                    if int(time.time() * 1000) - int(order['createdTime']) > MAX_TRADE_DURATION:
                        logging.info('Opened TP/SL order is too old, cancelling it')
                        # если ордер удачно отменен
                        if delete_order(order['orderId']):
                            order_params = {
                            "category": "spot",
                            "symbol": order['symbol'],
                            "side": order['side'],
                            "orderType": "MARKET",
                            "timeInForce": "GTC",
                            "marketUnit": "baseCoin",
                            "qty": order['qty']    
                            } 
                            # выставляем ордер, чтобы закрыть открытый до этого лимитный, от которого родился этот TP/SL 
                            if not send_order(order_params):
                                logging.info('Error closing position')
                            else:
                                logging.info('Position closed')  
                # если это лимитный ордер и он не исполнен даже частично                  
                elif order['orderType'] == 'Limit' and order['orderStatus'] == 'New':
                    # если он висит больше разрешенного времени
                    if int(time.time() * 1000) - int(order['createdTime']) > MAX_ORDER_DURATION:
                        logging.info('Not opened order is too old, cancelling it')
                        delete_order(order['orderId']) # отменяем его
                # если это лимитный ордер, исполненный частично        
                elif order['orderType'] == 'Limit' and order['orderStatus'] == 'PartiallyFilled':
                    # если он висит больше разрешенного времени
                    if int(time.time() * 1000) - int(order['updatedTime']) > MAX_TRADE_DURATION:
                        logging.info('Partially opened order is too old, cancelling it')
                        # отменяем его, если отмена успешна
                        if delete_order(order['orderId']):   
                            order_params = {
                            "category": "spot",
                            "symbol": order['symbol'],
                            "side": "Buy" if order['side'] == 'Sell' else "Sell",
                            "orderType": "MARKET",
                            "timeInForce": "GTC",
                            "marketUnit": "baseCoin",
                            "qty": order['cumExecQty']    
                            } 
                        # отправляем противоположный ордер на исполненное количество, чтобы закрыть позицию    
                        if not send_order(order_params):
                            logging.info('Error closing position')
                        else:
                            logging.info('Position closed') 
        # спим 100 секунд                     
        time.sleep(100)                


while True:
    inp = input('>>> ')
    if inp == 'start':
        print('Starting the bot ...')
        logging.info('Starting the bot ...') 
        main = threading.Thread(target=trade)
        order_dispatcher = threading.Thread(target=order_canceller)
        main.start()
        order_dispatcher.start()
        print('Bot succesfully started!')
        logging.info('Bot succesfully started!') 
    elif inp == 'balance':
        print("SPOT BALANCE")
        print(get_coin_balance(coin=False))
    elif inp == 'help':
        print('Print "start" after setting parameters to start the bot\n'
              'Print "balance" to get the balances\n')
    else:
        print('Unknown command')    
