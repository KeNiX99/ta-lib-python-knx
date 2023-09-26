#import redis, json5, time, os, logging, sys
import json5, time, os, logging, sys, numpy, talib ,pandas
import asyncio, time, random,logging, sqlalchemy
from binance.um_futures import UMFutures
from binance.lib.utils import config_logging
from binance.error import ClientError
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
from datetime import datetime
from sqlalchemy.sql import func
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker


#get argv
#logging.info("argv(s) = {}".format(len(sys.argv)))
#if len(sys.argv) == 1:
#    logging.info("Without config json file!!!")
#    exit()
#read config json
f = open("/config/default.json",'r')
text = f.read()
f.close()
params = json5.loads(text)
logger = logging.getLogger()
config_logging(logging, logging.INFO)
#formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', '%Y-%m-%d %H:%M:%S')
formatter = logging.Formatter('41X-4*4 %(asctime)s | %(message)s', '%Y-%m-%d %H:%M:%S')
file_handler = logging.FileHandler(params['logfile'])
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

engine = create_engine(params['engine'], echo=False)
Base = declarative_base()
Session = sessionmaker(bind=engine)
#session = Session()
trend_data = {}


class LastBaseline(Base):
    __tablename__ = 'lastbaseline'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(18))
    #timeframe = Column(String(9))
    baseline = Column(Float)
    #slp = Column(Float)
    #rsima = Column(Float)
    def __repr__(self):
        return "<LastBaseline(symbol='%s',baseline='%f')>" \
                    %(self.symbol, self.baseline)

class FAssets(Base):
    __tablename__ = 'fassets'
    id = Column(Integer, primary_key=True, autoincrement=True)
    asset = Column(String(18))
    walletbalance  = Column(Float)  #账户
    unrealizedprofit = Column(Float)
    marginbalance = Column(Float)
    maintmargin = Column(Float)  
    initialmargin = Column(Float)
    positioninitialmargin = Column(Float)
    availablebalance = Column(Float)
    def __repr__(self):
        return "<FAssets(asset='%s', walletbalance='%f', unrealizedprofit='%f',  marginbalance='%f', \
                    maintmargin='%f', initialmargin='%f', positioninitialmargin='%f', availablebalance='%f')>" \
                    %(  self.asset, self.walletbalance, self.unrealizedprofit, self.marginbalance,
                        self.maintmargin,self.initialmargin,self.positioninitialmargin,self.availablebalance)

class FPositions(Base):
    __tablename__ = 'fpositions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(18))
    positionside = Column(String(5))
    initialmargin = Column(Float)
    maintmargin = Column(Float)
    unrealizedprofit = Column(Float)
    positioninitialmargin = Column(Float)
    entryprice = Column(Float)
    positionamt = Column(Float)
    notional = Column(Float)
    markprice = Column(Float)
    liquidationprice = Column(Float)

    def __repr__(self):
        return "<FPositions(symbol='%s', positionside='%s', initialmargin='%f', maintmargin='%f', unrealizedprofit='%f', \
                     positioninitialmargin='%f', entryprice='%f', positionamt='%f', notional='%f', markprice='%f', liquidationprice='%f')>" \
                    %(  self.asset, self.positionside, self.initialmargin, self.maintmargin, self.unrealizedprofit,
                        self.positioninitialmargin,self.entryprice, self.positionamt, self.notional, self.markprice, self.liquidationprice)

class HPositions(Base):
    __tablename__ = 'hpositions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(18))
    positionside = Column(String(5))
    enterprice = Column(Float)
    positionamt = Column(Float)
    
    def __repr__(self):
        return "<FPositions(symbol='%s', positionside='%s', enterprice='%f', positionamt='%f')>" \
                    %(  self.asset, self.positionside, self.entryprice, self.positionamt)

class FOrders(Base):
    __tablename__ = 'forders'
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime)  
    symbol = Column(String(18))
    realizedprofit = Column(Float)
    fee = Column(Float)
    position = Column(String(9))
    side = Column(String(9))
    orderid = Column(String(36)) 
    def __repr__(self):
        return "<FOrders(timestamp='%s',symbol='%s', realizedprofit='%f', fee='%f',position='%f',side='%s', orderid='%s')>" %  \
                (time.strftime("%Y-%m-%d %H:%M:%S",self.timestamp),self.symbol,self.realizedprofit,self.fee,self.position,self.side,self.orderid)

class RProfit(Base):
    __tablename__ = 'rprofit'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(18))
    tempprofit = Column(Float)  #当前轮已实现利润
    def __repr__(self):
        return "<RProfit(symbol = '%s',  tempprofit='%f')>" \
                    %(  self.symbol, self.tempprofit)

HPositions.__table__
LastBaseline.__table__
FAssets.__table__
FPositions.__table__
FOrders.__table__
RProfit.__table__

Base.metadata.create_all(engine, checkfirst=True)
#Base.metadata.create_all(engine)

key = params['key']
secret = params['secret']

#pairlist = params['pairlist']

#winrate = params['winrate']

#appendrate = params['appendrate']

#stoprate = params['stoprate']
pairlist = []

handqty = params['handqty']

#strategy_str = params['strategy']

#releaselevel = params['releaselevel']

#redishost = params['redishost']

grid_rate = params['grid_rate']

back_rate = params['back_rate']

rate = grid_rate

#读取参数
for pair in handqty:
    pairlist.append(pair)
     
    #print("{} : {}".format(pair, handqty[pair]))

bn_client = UMFutures(key, secret, timeout=9)
#listen_key = bn_client.new_listen_key()
#_listenkey = listen_key["listenKey"]
#ws_client_mp = UMFuturesWebsocketClient()
ws_client = UMFuturesWebsocketClient()


#response = bn_client.new_listen_key()
#user_data_key = bn_client.new_listen_key()
#logging.info("\n\n\nCrazy!!! Futures Broker Keeper Start Timestamp@{}".format(bn_client.time()))
#logging.info("Receving listen key : {}".format(response["listenKey"]))


# connect to Redis and subscribe to tradingview messages
#lots = float(params['lots'])
allpositions = {}
trend_data = {}
Mark_Price = {}
ratios = {}
notional_limits = {}
pricePrecisions = {}
tickSize = {}
price_up = {}
price_down = {}
#check_position_lock (检查锁)
check_position_lock = False
#
update_listen_key = False
lstkey = ''

#指标数据
#ichimokumsg_5 = {}
#obvmacdmsg_5 = {}
#ichimokumsg_15 = {}
#obvmacdmsg_15 = {}
#ichimokumsg_60 = {}
#obvmacdmsg_60 = {}
#rsimsg_5 ={}

LONG_MAX = 3
SHORT_MAX = 5


#监听实时报价
def update_markprice_handler(_, message):
    global Mark_Price,allpositions,check_position_lock
    msg = json5.loads(message)
    #logger.info("message: {}".format(msg))
    if 'data' not in msg.keys():
        logger.info("message: {}".format(msg))
        
    
    elif 'markPriceUpdate' == msg['data']['e'] and not check_position_lock:
        
        symbol = msg['data']['s']
        Mark_Price[symbol] = float(msg['data']['p'])        
        
        logging.info("{}: {}".format(symbol,Mark_Price[symbol]))
        #logging.info(allpositions[message['data']['s']])
        #pull_positions(allpositions)
        
        rtmp = fmtprice(Mark_Price[symbol],symbol)
        if allpositions[symbol]['grid_rate'] == 0.0:
            allpositions[symbol]['grid_rate'] = fmtprice(float("{:.2}".format(rtmp/81)),symbol)
            allpositions[symbol]['back_rate'] = fmtprice(float("{:.1}".format(rtmp/81)) * 0.1 ,symbol)
        #allpositions[symbol]['back_rate'] = fmtprice(allpositions[symbol]['grid_rate']*0.1,symbol)
    
    
    '''
    else:
        logging.info('update_markprice_handler: {}'.format(message['data']))
    '''

def install_markprice_listener(pairlist,ws_client):
    symbols_list =[]
    for symbol in pairlist:
        symbolstr = "{}@markPrice@1s".format(symbol.lower())
        symbols_list.append(symbolstr)
    
    ws_client.subscribe(
        stream = symbols_list,
        #callback = update_markprice_handler,
    )
    
#监听用户数据
def userdata_handler(_, message):
    global bn_client, ws_client,pairlist,lstkey
    msg = json5.loads(message)
    logging.info("message:{}".format(message))
    logging.info("msg:{}".format(msg))
    
    if 'e' in msg.keys():
        if msg['e'] == 'ORDER_TRADE_UPDATE':
            check_position_lock = True
            
            if msg['o']['X'] == 'FILLED' and msg['o']['x'] == 'TRADE':
                logging.info("{} event:{}".format(msg['e'],msg))            
            #if message['o']['ot'] == 'MARKET' and message['o']['x'] == 'TRADE':
                pull_positions(allpositions)
                logging.info("===============================")
                logging.info("{} 有挂单单成交".format(msg['o']['s'])) 
                symbol = msg['o']['s']
                order = msg['o']
                #logging.debug("order 内容: {}".format(order))
                #for item in order:
                    #logging.info("{}:{}".format(item, order[item]))
                
                if (order['S'] == 'BUY' and order['ps'] == 'LONG') or (order['S'] == 'SELL' and order['ps'] == 'SHORT'): # and order['ot'] == 'LIMIT':
                    
                    trigger_open_position(order,allpositions[symbol],pricePrecisions[symbol],tickSize[symbol])
                    
                
                if (order['S'] == 'SELL' and order['ps'] == 'LONG') or (order['S'] == 'BUY' and order['ps'] == 'SHORT'): # and order['ot'] == 'LIMIT':
                    trigger_close_position(order,allpositions[symbol],pricePrecisions[symbol],tickSize[symbol])

                check_position_lock = False    
        if msg['e'] == 'listenKeyExpired': #and update_listen_key:

            logging.error("Err-type:{}\nError:{}".format(msg['e'],"Key 到期，启动更新"))
            #logging.error("userdata_handle Err-type:{}\nError:{}".format(type(err),err))
            #raise
            #logging.error("_main_finally quit!!!")
            os.kill(os.getpid(), 9)
        if msg['e'] == 'error':
            
            logging.error("message Err-type:{}\nError:{}".format(msg['e'],msg))
            #raise
            os.kill(os.getpid(), 9) 
            
        if msg['e'] == 'ACCOUNT_UPDATE':
            logging.info("{} event:{}".format(msg['e'],msg)) 
        
        #else:
            #logging.info("{} event:{}".format(message['e'],message))
            #pull_positions(allpositions)
        

def install_userdata_listener(listenkey,ws_client):
    ws_client.user_data(
        #_listen_key=listenkey["listenKey"],
        #listen_key = _listenkey,
        listen_key = listenkey,
        id=1,
        #callback=userdata_handler,
    )


#挂单处理检查
def close_calc_grid(symbol,long_open , short_open ,long_close, short_close):
    global check_position_lock,bn_client,Mark_Price, allpositions
    rtmp = fmtprice(Mark_Price[symbol],symbol)
    check_position_lock = True
    session = Session()
    #读取当前挂单
    long_orders = bn_client.get_orders(symbol = symbol, positionSide = "LONG",status = 'NEW', recvWindow = 3000)
    long_win_orders = []
    long_add_orders = []
    if len(long_orders) > 0 :
        for order in long_orders:
            if order['side'] == 'BUY' and order['positionSide'] == "LONG" :
                long_add_orders.append(order)
            if order['side'] == 'SELL' and order['positionSide'] == "LONG":
                long_win_orders.append(order)
            
        if not len(long_add_orders) == 0:
            if allpositions[symbol]['strend'] == "LONG" and not allpositions[symbol]['over'] == 'OverBuy':        
                logger.info("{} 多仓有: {} 张开仓挂单 ".format(symbol,len(long_add_orders)))
                for lborder in long_add_orders:
                    logger.info("{} 多仓有开仓挂单 {}@{}  ".format(symbol,lborder['origQty'],lborder['price']))
            else:
                clear_long_buy_orders(symbol)
        else:
            logger.info("{} 多仓没有开仓挂单 ".format(symbol))
        
        if not len(long_win_orders) == 0:        
            logger.info("{} 多仓有: {} 张平仓挂单 ".format(symbol,len(long_win_orders)))
            for lsorder in long_win_orders:
                logger.info("{} 多仓有平仓挂单 {}@{}  ".format(symbol,lsorder['origQty'],lsorder['price']))
        else:
            logger.info("{} 多仓没有平仓挂单 ".format(symbol))    
    short_orders = bn_client.get_orders(symbol = symbol,positionSide = "SHORT",status = 'NEW', recvWindow = 3000)
    short_win_orders = []
    short_add_orders = []
    if len(short_orders) > 0:
        for order in short_orders:
            if order['side'] == 'SELL' and order['positionSide'] == "SHORT":
                short_add_orders.append(order)
            
            if order['side'] == 'BUY' and order['positionSide'] == "SHORT":
                short_win_orders.append(order)
            
        if not len(short_add_orders) == 0:
            if allpositions[symbol]['strend'] == "SHORT" and not allpositions[symbol]['over'] == 'OverSell':        
                logger.info("{} 空仓有: {} 张开仓挂单 ".format(symbol,len(short_add_orders)))
                for ssorder in short_add_orders:
                    logger.info("{} 空仓有开仓挂单 {}@{}  ".format(symbol,ssorder['origQty'],ssorder['price']))
            else:
                clear_short_sell_orders(symbol)
        else:
            logger.info("{} 空仓没有张开仓挂单 ".format(symbol))
        
        if not len(short_win_orders) == 0:        
            logger.info("{} 空仓有: {} 张平仓挂单 ".format(symbol,len(short_win_orders)))
            for sborder in short_win_orders:
                logger.info("{} 空仓有平仓挂单 {}@{}  ".format(symbol,sborder['origQty'],sborder['price']))
        else:
            logger.info("{} 空仓没有平仓挂单 ".format(symbol))

    #多仓开仓单检查
    longopened = False
    if session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').count() > 0:
        longpositions = session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').all()
        for position in longpositions:
            if position.enterprice == long_open :
                longopened = True
    if not longopened and not allpositions[symbol]["long_pos"] < 4 * allpositions[symbol]['handqty'] :
        longopened = True
    if not longopened and allpositions[symbol]['strend'] == "LONG" and not allpositions[symbol]['over'] == 'OverBuy':
        #计算开仓仓位
        pull_positions(allpositions)
        pos = allpositions[symbol]
        longopensize = pos['handqty']
        if len(long_add_orders) == 1:
            #logging.info("long_add_orders[0] : {}".format(long_add_orders[0]))
            if (float(long_add_orders[0]['price']) > long_open and  longopensize > 0 )  \
            or (float(long_add_orders[0]['price']) == long_open and not float(long_add_orders[0]['origQty']) == longopensize) \
            and allpositions[symbol]['strend'] == "LONG" and not allpositions[symbol]['over'] == 'OverBuy':
                clear_long_buy_orders(symbol)
                new_long_buy(symbol, longopensize, long_open)    
            if (float(long_add_orders[0]['price']) < pos['last_pivot'] and pos['last_pivot'] > 0) \
            or not(allpositions[symbol]['strend'] == "LONG" and not allpositions[symbol]['over'] == 'OverBuy'):
                clear_long_buy_orders(symbol)
        elif not len(long_add_orders) == 1 :
            if len(long_add_orders) > 0 :
                clear_long_buy_orders(symbol)
            elif allpositions[symbol]['strend'] == "LONG" and not allpositions[symbol]['over'] == 'OverBuy':
                logging.info("{} 多仓在 {} 没有持仓,可以挂多仓开仓单,仓位头寸为 {} ".format(symbol, long_open, longopensize))
                new_long_buy(symbol, longopensize, long_open)    

    #空仓开仓单检查
    shortopened = False
    if session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').count() > 0:
        shortpositions = session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').all()
        for position in shortpositions:
            if position.enterprice == short_open :
                shortopened = True
    if not shortopened and not allpositions[symbol]["short_pos"] < 4 * allpositions[symbol]['handqty']:
        shortopened = True
    if not shortopened and allpositions[symbol]['strend'] == "SHORT" and not allpositions[symbol]['over'] == 'OverSell':
        #空仓开仓仓位检查
        pull_positions(allpositions)
        pos = allpositions[symbol]
        shortopensize = pos['handqty']
            
        if len(short_add_orders) == 1:

            if (float(short_add_orders[0]['price']) < short_open  and   shortopensize > 0) \
            or (float(short_add_orders[0]['price']) == short_open  and not float(short_add_orders[0]['origQty']) == shortopensize) \
            and allpositions[symbol]['strend'] == "SHORT" and not allpositions[symbol]['over'] == 'OverSell':
                clear_short_sell_orders(symbol)
                new_short_sell(symbol, shortopensize, short_open)
            if (float(short_add_orders[0]['price']) > pos['last_pivot'] and pos['last_pivot'] > 0) \
            or not(allpositions[symbol]['strend'] == "SHORT" and not allpositions[symbol]['over'] == 'OverSell'):
                clear_short_sell_orders(symbol)

        elif not len(short_add_orders) == 1 :
            if len(short_add_orders) > 0 :
                clear_short_sell_orders(symbol)
            elif allpositions[symbol]['strend'] == "SHORT" and not allpositions[symbol]['over'] == 'OverSell':
                logging.info("{} 空仓在 {} 没有持仓,可以挂空仓开仓单,仓位头寸为 {} ".format(symbol, short_open, shortopensize))
                new_short_sell(symbol, shortopensize, short_open)        

    #多仓平仓单检查
    if len(long_win_orders) == 1:
        closeprice = float(long_win_orders[0]['price'])
        closeqty = float(long_win_orders[0]['origQty'])
        longcloseable = 0
        if long_close > closeprice:
            #多仓一平检查
            if long_close > allpositions[symbol]['long_entprice'] + (allpositions[symbol]['grid_rate'] - allpositions[symbol]['back_rate'] ) \
            and allpositions[symbol]['long_pos'] >= allpositions[symbol]['handqty']:
                longcloseable = allpositions[symbol]['long_pos']
            #多仓当前档检查
            elif session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').count() > 0:
                longpositions = session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').all()
                for position in longpositions:
                    if  position.enterprice <= long_close : #+ allpositions[symbol]['back_rate'] :
                        longcloseable = longcloseable + position.positionamt
            #有单改单
            if  longcloseable >= allpositions[symbol]['handqty']:
                clear_long_sell_orders(symbol)
                new_long_sell(symbol, longcloseable, long_close)
        else:
            longcloseable = 0
            #多仓一平检查
            if closeprice > allpositions[symbol]['long_entprice'] + (allpositions[symbol]['grid_rate'] - allpositions[symbol]['back_rate'] ) \
            and allpositions[symbol]['long_pos'] >= allpositions[symbol]['handqty']:
                longcloseable = allpositions[symbol]['long_pos']
            #多仓当前档检查
            elif session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').count() > 0:
                longpositions = session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').all()
                for position in longpositions:
                    if  position.enterprice <= closeprice : #+ allpositions[symbol]['back_rate'] :
                        longcloseable = longcloseable + position.positionamt
            if not closeqty == longcloseable:  
                clear_long_sell_orders(symbol)
                new_long_sell(symbol, longcloseable, closeprice)

    elif not len(long_win_orders) == 1 :
        if len(long_win_orders) > 0:
            clear_long_sell_orders(symbol)
        else:
            longcloseable = 0
            #多仓一平检查
            if long_close > allpositions[symbol]['long_entprice'] + (allpositions[symbol]['grid_rate'] - allpositions[symbol]['back_rate'] ) \
            and allpositions[symbol]['long_pos'] >= allpositions[symbol]['handqty']:
                longcloseable = allpositions[symbol]['long_pos']
            #多仓当前档检查
            elif session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').count() > 0:
                longpositions = session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').all()
                for position in longpositions:
                    if  position.enterprice <= long_close: # + allpositions[symbol]['back_rate'] :
                        longcloseable = longcloseable + position.positionamt
            #无单挂单
            if  longcloseable >= allpositions[symbol]['handqty']:
                new_long_sell(symbol, longcloseable, long_close)
    
    #空仓平仓单检查
    if len(short_win_orders) == 1:
        closeprice = float(short_win_orders[0]['price'])
        closeqty = float(short_win_orders[0]['origQty'])
        shortcloseable = 0
        if short_close < closeprice:
            #空仓一平检查
            if short_close < allpositions[symbol]['short_entprice'] - (allpositions[symbol]['grid_rate'] - allpositions[symbol]['back_rate'] ) \
            and allpositions[symbol]['short_pos'] > allpositions[symbol]['handqty']:
                shortcloseable = allpositions[symbol]['short_pos']
            #空仓当前档检查    
            elif session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').count() > 0:
                shortpositions = session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').all()
                for position in shortpositions:
                    if position.enterprice >= short_close: # - allpositions[symbol]['back_rate']:
                        shortcloseable = shortcloseable + position.positionamt
            
            #有单改单
            if  shortcloseable >= allpositions[symbol]['handqty']:
                clear_short_buy_orders(symbol)
                new_short_buy(symbol,shortcloseable, short_close)
        else:
            shortcloseable = 0
            #空仓一平检查
            if closeprice < allpositions[symbol]['short_entprice'] - (allpositions[symbol]['grid_rate'] - allpositions[symbol]['back_rate'] ) \
            and allpositions[symbol]['short_pos'] >= allpositions[symbol]['handqty']:
                shortcloseable = allpositions[symbol]['short_pos']
            #空仓当前档检查    
            elif session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').count() > 0:
                shortpositions = session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').all()
                for position in shortpositions:
                    if position.enterprice >= closeprice: # - allpositions[symbol]['back_rate']:
                        shortcloseable = shortcloseable + position.positionamt
            
            if not closeqty == shortcloseable:
                clear_short_buy_orders(symbol)
                new_short_buy(symbol,shortcloseable, closeprice)

    elif not len(short_win_orders) == 1 :
        if len(short_win_orders) > 0:
            clear_short_buy_orders(symbol)
        else:
            shortcloseable = 0
            #空仓一平检查
            if short_close < allpositions[symbol]['short_entprice'] - (allpositions[symbol]['grid_rate'] - allpositions[symbol]['back_rate'] ) \
            and allpositions[symbol]['short_pos'] >= allpositions[symbol]['handqty']:
                shortcloseable = allpositions[symbol]['short_pos']
            #空仓当前档检查    
            elif session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').count() > 0:
                shortpositions = session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').all()
                for position in shortpositions:
                    if position.enterprice >= short_close: #- allpositions[symbol]['back_rate']:
                        shortcloseable = shortcloseable + position.positionamt
            
            #无单挂单
            if  shortcloseable >= allpositions[symbol]['handqty']:
                new_short_buy(symbol,shortcloseable, short_close)    
    
    session.close()
    check_position_lock = False
    return

#拉取账户数据
def init_pull_positions(positions):
    #bninfo = bn_client.account()
    #for p in bninfo['positions']:
    global pairlist, handqty, grid_rate, back_rate
    session = Session()
    for symbol in pairlist:
        #if p['symbol'] in pairlist:
        positions[symbol]={}
        positions[symbol]['long_notional'] = 0.0
        positions[symbol]['long_pos'] = 0.0
        positions[symbol]['long_profit'] = 0.0
        positions[symbol]['long_entprice'] = 0.0
        positions[symbol]['long_positioninitialmargin'] = 0.0
        positions[symbol]['short_notional'] = 0.0
        positions[symbol]['short_pos'] = 0.0 
        positions[symbol]['short_profit'] = 0.0
        positions[symbol]['short_entprice'] = 0.0
        positions[symbol]['short_positioninitialmargin'] = 0.0
        positions[symbol]['grid_rate'] = 0.0
        positions[symbol]['back_rate'] = 0.0
        positions[symbol]['last_base_price'] = 0.0
        positions[symbol]['last_pivot'] = 0.0
        positions[symbol]['last_high_price'] = 0.0
        positions[symbol]['last_low_price'] = 0.0
        positions[symbol]['avg_price'] = 0.0
        positions[symbol]['grid_move'] = ""
        positions[symbol]['strend'] = "NOP"
        positions[symbol]['over'] = "Normal"
        if session.query(LastBaseline).filter_by(symbol = symbol).count() == 0:
            lastbaselinedata = LastBaseline(    symbol = symbol,
                                                baseline= 0.0
                                    
                                )
            session.add(lastbaselinedata)
            session.commit()
        elif session.query(LastBaseline).filter_by(symbol = symbol).count() > 0:
            lastbaselinedata = session.query(LastBaseline).filter_by(symbol = symbol).first()   
            allpositions[symbol]['last_base_price'] = lastbaselinedata.baseline
            session.commit()
        

          
    for pair in handqty:
        positions[pair]['handqty'] = handqty[pair]
        Mark_Price[pair] = 0.0
    '''
    for pair in grid_rate:
        positions[pair]['grid_rate'] = grid_rate[pair]
        logger.info("{} 网格区间为 {} USDT".format(pair, positions[pair]['grid_rate']))
    
    for pair in back_rate:
        positions[pair]['back_rate'] = back_rate[pair]
        logger.info("{} 回调阈值为 {} USDT".format(pair, positions[pair]['back_rate']))
    '''
    session.close()

def pull_positions(positions):
    logging.debug("pull_positions")
    session = Session()
    try:
        account_info = bn_client.account()
    except Exception as err:
        logging.error("account_info(pull_positions()) :{}\nError:{}".format(type(err),err))
        os.kill(os.getpid(), 9)
    
    #bninfo = bn_client.account()
    for p in account_info['positions']:
        if p['symbol'] in pairlist:
            if ( p['positionSide'] == 'LONG') :
                positions[p['symbol']]['long_notional'] = abs(float(p['notional']))
                positions[p['symbol']]['long_pos'] = abs(float(p['positionAmt']))
                positions[p['symbol']]['long_profit'] = float(p['unrealizedProfit'])
                positions[p['symbol']]['long_entprice'] = float(p['entryPrice'])
                positions[p['symbol']]['long_positioninitialmargin'] =float(p['positionInitialMargin'])
                        
            if ( p['positionSide'] == 'SHORT') :
                positions[p['symbol']]['short_notional'] = abs(float(p['notional']))
                positions[p['symbol']]['short_pos'] = abs(float(p['positionAmt'])) 
                positions[p['symbol']]['short_profit'] = float(p['unrealizedProfit'])
                positions[p['symbol']]['short_entprice'] = float(p['entryPrice'])
                positions[p['symbol']]['short_positioninitialmargin'] =float(p['positionInitialMargin'])
                    
            #logging.info("temp_calc {}: \n{}".format(p['symbol'],positions[p['symbol']]))
            if positions[p['symbol']]['long_pos'] + positions[p['symbol']]['short_pos'] > 0:
                 positions[p['symbol']]['avg_price'] = (positions[p['symbol']]['long_notional'] + positions[p['symbol']]['short_notional'])/(positions[p['symbol']]['long_pos'] + positions[p['symbol']]['short_pos'])   
            elif not  Mark_Price[p['symbol']] == 0.0 :
                positions[p['symbol']]['avg_price'] = Mark_Price[p['symbol']]
            else:
                 positions[p['symbol']]['avg_price'] = 0.0
                
    #账户更新 
    #logging.info("更新asset数据库")
    for item in account_info['assets']:
        if item['asset'] == 'USDT':
            if not session.query(FAssets).filter_by(asset = 'USDT').count() == 0:
                assetdata = session.query(FAssets).filter_by(asset = item["asset"]).first()
                assetdata.walletbalance = item['walletBalance']
                assetdata.unrealizedprofit = item['unrealizedProfit']
                assetdata.marginbalance = item['marginBalance']
                assetdata.maintmargin = item['maintMargin']
                assetdata.initialmargin = item['initialMargin']
                assetdata.positioninitialmargin = item['positionInitialMargin']
                assetdata.availablebalance = item['availableBalance']
                session.commit()
            if session.query(FAssets).filter_by(asset = 'USDT').count() == 0:
                assetdata = FAssets(asset = item['asset'],
                                    walletbalance = item['walletBalance'],
                                    unrealizedprofit = item['unrealizedProfit'],
                                    marginbalance = item['marginBalance'],
                                    maintmargin = item['maintMargin'],
                                    initialmargin = item['initialMargin'],
                                    positioninitialmargin = item['positionInitialMargin'],
                                    availablebalance = item['availableBalance']
                                    ) 
                session.add(assetdata)
                session.commit() 
    #仓位更新
    risks = bn_client.get_position_risk(recvWindow=3000)
    
    for item in account_info['positions']:
        if not item['initialMargin'] == '0':
            for ii in risks:
                if ii['symbol'] == item['symbol'] and ii['positionSide'] == item['positionSide'] :
                    riskdata = ii 
                    
            if session.query(FPositions).filter_by(symbol = item['symbol'], positionside = item['positionSide']).count() == 0:
                positiondata = FPositions( symbol = item['symbol'],
                                           positionside = item['positionSide'],
                                           initialmargin = item['initialMargin'],
                                           maintmargin = item['maintMargin'],
                                           unrealizedprofit = item['unrealizedProfit'],
                                           positioninitialmargin = item['positionInitialMargin'], 
                                           entryprice = item['entryPrice'],
                                           positionamt = item['positionAmt'],
                                           notional = item['notional'],
                                           markprice = riskdata['markPrice'],
                                           liquidationprice = riskdata['liquidationPrice']
                                        )
                session.add(positiondata)
                session.commit()

            elif session.query(FPositions).filter_by(symbol = item['symbol'], positionside = item['positionSide']).count() > 1:
                del_positions = session.query(FPositions).filter_by(symbol = item['symbol'], positionside = item['positionSide'] )
                for position in del_positions:
                    session.delete(position)
                positiondata = FPositions( symbol = item['symbol'],
                                           positionside = item['positionSide'],
                                           initialmargin = item['initialMargin'],
                                           maintmargin = item['maintMargin'],
                                           unrealizedprofit = item['unrealizedProfit'],
                                           positioninitialmargin = item['positionInitialMargin'], 
                                           entryprice = item['entryPrice'],
                                           positionamt = item['positionAmt'],
                                           notional = item['notional'],
                                           markprice = riskdata['markPrice'],
                                           liquidationprice = riskdata['liquidationPrice']
                                        )
                session.add(positiondata)
                session.commit()
            
            elif session.query(FPositions).filter_by(symbol = item['symbol'], positionside = item['positionSide']).count() == 1:
                position = session.query(FPositions).filter_by(symbol = item['symbol'], positionside = item['positionSide']).first()
                position.symbol = item['symbol']
                position.positionside = item['positionSide']
                position.initialmargin = item['initialMargin']
                position.maintmargin = item['maintMargin']
                position.unrealizedprofit = item['unrealizedProfit']
                position.positioninitialmargin = item['positionInitialMargin'] 
                position.entryprice = item['entryPrice']
                position.positionamt = item['positionAmt']
                position.notional = item['notional']
                position.markprice = riskdata['markPrice']
                position.liquidationprice = riskdata['liquidationPrice']
                session.commit()
        else:
            if not session.query(FPositions).filter_by(symbol = item['symbol'],positionside = item['positionSide']).count() == 0:
                del_positions = session.query(FPositions).filter_by(symbol = item['symbol'])
                for position in del_positions:
                    session.delete(position)
                    session.commit()

    #更新当前平仓数据
    for symbol in pairlist:
        if session.query(RProfit).filter_by(symbol = symbol).count() == 0:
            rprofitdata = RProfit(
                                    symbol = symbol,
                                    tempprofit = 0.0
                                    )
            session.add(rprofitdata)
            session.commit()
        if  positions[symbol]['long_pos'] == 0.0 and positions[symbol]['short_pos'] == 0.0 :
            rprofit = session.query(RProfit).filter_by(symbol = symbol).first()
            rprofit.tempprofit = 0.0
            session.commit()
            
    session.close()

def get_balance():
    session = Session()
    balance = session.query(FAssets).filter_by(asset = 'USDT').first()
    session.close()
    return balance

def fmtprice(price,symbol):
    global tickSize,pricePrecision
    #pprice = float(int((price)/tickSize[symbol])*tickSize[symbol])
    xxd = len(str(int(1/tickSize[symbol])))
    str_price = ('%.'+str(xxd)+'f')% price
    pprice = float(str_price)
    return pprice
    #return  float(int(price/tickSize[symbol])*tickSize[symbol]) - (float(int(price/tickSize[symbol])*tickSize[symbol]) % tickSize[symbol])
    #return  float(round(float(int(price/tickSize[symbol]))*tickSize[symbol],pricePrecisions[symbol]))

#挂单操作模块
def clear_long_buy_orders(symbol):
    logging.info("清理{}开多仓挂单".format(symbol))
    global bn_client
    delorders = bn_client.get_orders(symbol = symbol,status = 'NEW', recvWindow = 3000)
    if len(delorders) > 0:
        for delorder in delorders:
            if delorder['side'] == 'BUY' and delorder['positionSide'] == "LONG" :
                response = bn_client.cancel_order(symbol=symbol, orderId=delorder['orderId'], recvWindow=3000)
                #logging.info("删除订单执行结果: {}".format(response))        
def clear_long_sell_orders(symbol):
    logging.info("清理{}平多仓挂单".format(symbol))
    global bn_client
    delorders = bn_client.get_orders(symbol = symbol,status = 'NEW', recvWindow = 3000)
    if len(delorders) > 0:
        for delorder in delorders:
            if delorder['side'] == 'SELL' and delorder['positionSide'] == "LONG" :
                response = bn_client.cancel_order(symbol=symbol, orderId=delorder['orderId'], recvWindow=3000)
                #logging.info("删除订单执行结果: {}".format(response))        
def clear_short_sell_orders(symbol):
    logging.info("清理{}开空仓挂单".format(symbol))
    global bn_client
    delorders = bn_client.get_orders(symbol = symbol,status = 'NEW', recvWindow = 3000)
    if len(delorders) > 0:
        for delorder in delorders:
            if delorder['side'] == 'SELL' and delorder['positionSide'] == "SHORT" :
                response = bn_client.cancel_order(symbol=symbol, orderId=delorder['orderId'], recvWindow=3000)
                #logging.info("删除订单执行结果: {}".format(response))        
def clear_short_buy_orders(symbol):
    logging.info("清理{}平空仓挂单".format(symbol))
    global bn_client
    delorders = bn_client.get_orders(symbol = symbol,status = 'NEW', recvWindow = 3000)
    if len(delorders) > 0:
        for delorder in delorders:
            if delorder['side'] == 'BUY' and delorder['positionSide'] == "SHORT" :
                response = bn_client.cancel_order(symbol=symbol, orderId=delorder['orderId'], recvWindow=3000)
                #logging.info("删除订单执行结果: {}".format(response))        
def new_long_buy(symbol, qty, price):
    global bn_client
    logging.info("新建{}开多仓挂单 {}@{} ".format(symbol,qty,price))
    try:
        response = bn_client.new_order(
                        symbol=symbol,
                        side='BUY',
                        positionSide='LONG',
                        type='STOP',
                        quantity=  qty,
                        timeInForce="GTC",
                        price= price,
                        stopPrice = price,
                        
                    )
    except Exception as err:
        logging.debug("挂开多仓单失败，错误：{} 改成跳过".format(err))
def new_long_sell(symbol, qty, price):
    global bn_client
    logging.info("新建{}平多仓挂单 {}@{} ".format(symbol,qty,price))
    try:
        response = bn_client.new_order(
                        symbol=symbol,
                        side='SELL',
                        positionSide='LONG',
                        type='STOP',
                        quantity=  qty,
                        timeInForce="GTC",
                        price= price,
                        stopPrice = price,
                        
                    )
    except Exception as err:
        logging.debug("挂平多仓单失败，错误：{} 改成跳过".format(err))
def new_short_sell(symbol, qty, price):
    global bn_client
    logging.info("新建{}开空仓挂单 {}@{} ".format(symbol,qty,price))
    try:
        response = bn_client.new_order(
                        symbol=symbol,
                        side='SELL',
                        positionSide='SHORT',
                        type='STOP',
                        quantity=  qty,
                        timeInForce="GTC",
                        price= price,
                        stopPrice = price,
                        
                    )
    except Exception as err:
        logging.debug("挂开空仓单失败，错误：{} 改成跳过".format(err))
def new_short_buy(symbol, qty, price):
    global bn_client
    logging.info("新建{}平空仓挂单 {}@{} ".format(symbol,qty,price))
    try:
        response = bn_client.new_order(
                        symbol=symbol,
                        side='BUY',
                        positionSide='SHORT',
                        type='STOP',
                        quantity=  qty,
                        timeInForce="GTC",
                        price= price,
                        stopPrice = price,
                        
                    )
    except Exception as err:
        logging.debug("挂平空仓单失败，错误：{} 改成跳过".format(err))

#开仓触发处理
def trigger_open_position(order,position,priceprecision,ticksize):
    global check_position_lock
    #锁查仓
    check_position_lock = True
    symbol = order['s']
    #position[symbol]['last_high_price'] = 0.0
    #position[symbol]['last_low_price'] = 0.0
    session = Session()

    logging.info("{} 开仓 {}".format(order['s'], order))
    #rprofit 记账
    rprofit = session.query(RProfit).filter_by(symbol = symbol).first()
    rprofit.tempprofit = rprofit.tempprofit + float(order['rp']) - float(order['n'])
    tprofit = rprofit.tempprofit
    session.commit()
    logging.info("{} 开仓记账手续费 {}".format(order['s'], tprofit))
    
    #if session.query(HPositions).filter_by(symbol = order['s'], enterprice = float(order['p']), positionside = order['ps']).count() == 0:
    logging.info("{} 记录持仓表 {}x{}@{}".format(order['s'], order['p'],float(order['q']),order['ps']))
    if not session.query(HPositions).filter_by(symbol = order['s'], enterprice = float(order['p']), positionside = order['ps']).count() == 0:
        hpositionsdata = session.query(HPositions).filter_by(symbol = order['s'], enterprice = float(order['p']), positionside = order['ps']).first()    
        hpositionsdata.positionamt += float(order['q'])
        session.commit()

    elif session.query(HPositions).filter_by(symbol = order['s'], enterprice = float(order['p']), positionside = order['ps']).count() == 0:
        hpositionsdata = HPositions(symbol = order['s'], enterprice = float(order['p']), positionside = order['ps'], positionamt = float(order['q']))
        session.add(hpositionsdata)
        session.commit()
           
    #重新检查挂单
    if order['ps'] == 'LONG':
        
        #base_line = fmtprice(float(order['p']),symbol)
        
        if allpositions[symbol]['last_pivot'] == 0.0:
            allpositions[symbol]['last_pivot'] = fmtprice(base_line,symbol)
        
        allpositions[symbol]['last_pivot'] = fmtprice(float(order['p']),symbol)
        allpositions[symbol]['grid_rate'] = fmtprice(float("{:.2}".format(allpositions[symbol]['last_pivot']/81)),symbol)
        allpositions[symbol]['back_rate'] = fmtprice(float("{:.1}".format(allpositions[symbol]['last_pivot']/81)) * 0.1 ,symbol)
        long_open = fmtprice(allpositions[symbol]['last_pivot'] + allpositions[symbol]['grid_rate'],symbol)
        long_close = fmtprice(allpositions[symbol]['last_pivot'] - allpositions[symbol]['back_rate'],symbol)

        short_open = fmtprice(allpositions[symbol]['last_pivot'] - allpositions[symbol]['grid_rate'],symbol)
        short_close = fmtprice(allpositions[symbol]['last_pivot'] + allpositions[symbol]['back_rate'],symbol)
        
        '''
        #清理同价开仓单
        long_orders = bn_client.get_orders(symbol = symbol, positionSide = "LONG",status = 'NEW', recvWindow = 3000)
        long_add_orders = []
        if len(long_orders) > 0 :
            for order in long_orders:
                if order['side'] == 'BUY' and order['positionSide'] == "LONG" :
                    long_add_orders.append(order)
        if not len(long_add_orders) == 0:        
            for lborder in long_add_orders:
                if float(lborder['price']) <= float(order['p']):
                    response = bn_client.cancel_order(symbol=symbol, orderId=lborder['orderId'], recvWindow=3000)
        '''
  
    if order['ps'] == 'SHORT':
        base_line = fmtprice(float(order['p']) ,symbol)
        if allpositions[symbol]['last_pivot'] == 0.0:
            allpositions[symbol]['last_pivot'] = fmtprice(base_line,symbol)
        allpositions[symbol]['last_pivot'] = fmtprice(float(order['p']),symbol)
        allpositions[symbol]['grid_rate'] = fmtprice(float("{:.2}".format(allpositions[symbol]['last_pivot']/81)),symbol)
        allpositions[symbol]['back_rate'] = fmtprice(float("{:.1}".format(allpositions[symbol]['last_pivot']/81)) * 0.1 ,symbol)
        long_open = fmtprice(allpositions[symbol]['last_pivot'] + allpositions[symbol]['grid_rate'],symbol)
        long_close = fmtprice(allpositions[symbol]['last_pivot'] - allpositions[symbol]['back_rate'],symbol)

        short_open = fmtprice(allpositions[symbol]['last_pivot'] - allpositions[symbol]['grid_rate'],symbol)
        short_close = fmtprice(allpositions[symbol]['last_pivot'] + allpositions[symbol]['back_rate'],symbol)
               
        '''
        #清理同价开仓单
        short_orders = bn_client.get_orders(symbol = symbol,positionSide = "SHORT",status = 'NEW', recvWindow = 3000)
        short_add_orders = []
        if len(short_orders) > 0:
            for order in short_orders:
                if order['side'] == 'SELL' and order['positionSide'] == "SHORT":
                    short_add_orders.append(order)
        if not len(short_add_orders) == 0:        
            for ssorder in short_add_orders:
                if float(ssorder['price']) >= float(order['p']):
                    response = bn_client.cancel_order(symbol=symbol, orderId=ssorder['orderId'], recvWindow=3000)    
        '''
        
    base_line = fmtprice(float(order['p']),symbol)
    #写入数据表
    if session.query(LastBaseline).filter_by(symbol = symbol).count() == 0:
        lastbaselinedata = LastBaseline(    symbol = symbol,
                                            baseline= 0.0
                            )
        session.add(lastbaselinedata)
        session.commit()
    elif session.query(LastBaseline).filter_by(symbol = symbol).count() > 0:
        lastbaselinedata = session.query(LastBaseline).filter_by(symbol = symbol).first()   
        allpositions[symbol]['last_base_price'] = lastbaselinedata.baseline
        session.commit()
    
    
    if allpositions[symbol]['last_base_price'] == 0.0:
        lastbaselinedata = session.query(LastBaseline).filter_by(symbol = symbol).first()   
        lastbaselinedata.baseline = base_line
        session.commit()
        allpositions[symbol]['last_base_price'] = base_line
    
    logger.info("============开仓设置开平仓价格=================================")
    
    logger.info("收到 {} 开仓成交报价 {}".format(symbol, float(order['p'])))
    logger.info("{} grid_rate: {}  back_rate: {} ".format(symbol, allpositions[symbol]['grid_rate'], allpositions[symbol]['back_rate']))
    #logger.info("{} 多仓锚点(long_arch)为 {}".format(symbol, long_arch))
    logger.info("{} 多仓开仓挂单价格(long_open)为 {}".format(symbol, long_open))
    logger.info("{} 多仓平仓挂单价格(long_close)为 {}".format(symbol, long_close))
    #logger.info("{} 空仓锚点(short_arch)为 {}".format(symbol, short_arch))
    logger.info("{} 空仓开仓挂单价格(short_open)为 {}".format(symbol, short_open))
    logger.info("{} 空仓平仓挂单价格(short_close)为 {}".format(symbol, short_close))
    logger.info("============开仓设置开平仓价格结束=================================")
    
    #close_calc_grid(symbol, long_open, short_open,long_close, short_close)
    
    #解锁查仓
    session.close()   
    check_position_lock = False

#平仓触发处理
def trigger_close_position(order,position,priceprecision,ticksize):
    global allpositions,Mark_Price, check_position_lock
    #锁查仓
    check_position_lock = True
    logging.info("{} 平仓 {}".format(order['s'], order))
    symbol = order['s']
    
    #billing record
    orderid = str(order['i'])
    session = Session()
    '''
    rtmp = fmtprice(Mark_Price[symbol],symbol)
    #kenix-414 formula
    base_line = fmtprice((rtmp - rtmp % allpositions[symbol]['grid_rate']),symbol)
    long_arch = fmtprice(base_line,symbol)
    short_arch = fmtprice(long_arch + allpositions[symbol]['grid_rate'],symbol)
    long_open = fmtprice(long_arch + allpositions[symbol]['grid_rate'],symbol)
    long_close = fmtprice(long_arch - allpositions[symbol]['back_rate'],symbol)
    short_open = fmtprice(short_arch - allpositions[symbol]['grid_rate'],symbol)
    short_close = fmtprice(short_arch + allpositions[symbol]['back_rate'],symbol)
    '''

    if session.query(FOrders).filter_by(orderid = orderid).count() == 0:
        logging.info("{} 记账平仓单 {}".format(order['s'], order))
        forderdata = FOrders(   timestamp = time.localtime(), 
                                symbol = symbol,
                                realizedprofit = float(order['rp']),
                                fee = float(order['n']),
                                position = order['ps'],
                                orderid = orderid,
                                side = order['S']
                            )
        session.add(forderdata)
        session.commit()
        
        #rprofit 记账
        rprofit = session.query(RProfit).filter_by(symbol = symbol).first()
        rprofit.tempprofit = rprofit.tempprofit + float(order['rp']) - float(order['n'])
        tprofit = rprofit.tempprofit
        session.commit()
        logging.info("{} 平仓流水记账 {}".format(order['s'], tprofit))
        pull_positions(allpositions)
        position = allpositions[symbol]
        long_hands = abs(position['long_pos'])
        short_hands = abs(position['short_pos'])
        rtmp = fmtprice(Mark_Price[symbol],symbol)
        allpositions[symbol]['grid_rate'] = fmtprice(float("{:.2}".format(rtmp/81)),symbol)
        allpositions[symbol]['back_rate'] = fmtprice(float("{:.1}".format(rtmp/81)) * 0.1 ,symbol)    
            
        if order['ps'] == "LONG":
            #多仓一平后处理
            if long_hands == 0:
                hpositionsdata = session.query(HPositions).filter_by(symbol = order['s'],  positionside = order['ps']).all()
                for rec in hpositionsdata:
                    session.delete(rec)
                    session.commit()
            #多仓非一平处理
            else:
                hpositionsdata = session.query(HPositions).filter_by(symbol = order['s'],  positionside = order['ps']).all()
                for rec in hpositionsdata:
                    if not rec.enterprice > float(order['p']): # + allpositions[symbol]['back_rate'] : 
                        session.delete(rec)
                        session.commit()
            
        if order['ps'] == "SHORT":
            #空仓一平处理
            if short_hands == 0:
                hpositionsdata = session.query(HPositions).filter_by(symbol = order['s'],  positionside = order['ps']).all()
                for rec in hpositionsdata:
                    session.delete(rec)
                    session.commit()
            #空仓非一平处理
            else:
                hpositionsdata = session.query(HPositions).filter_by(symbol = order['s'],  positionside = order['ps']).all()
                for rec in hpositionsdata:
                    if not rec.enterprice <  float(order['p']) : #- allpositions[symbol]['back_rate'] : 
                        session.delete(rec)
                        session.commit()
    
    
    session.close()
    
    check_position_lock = False

#初始化
#获取交易所资料
def get_exchange_info(ratios,notional_limits,pricePrecision,tickSize):
    ex_info = bn_client.exchange_info()
    for symbol in pairlist:
        for item in ex_info['symbols']:
            if item['symbol'] == symbol:
                pricePrecisions[symbol] = int(item['pricePrecision'])  
                
                for i in item['filters']:
                    if i['filterType'] == 'MARKET_LOT_SIZE':
                        ratio = float(i['minQty'])
                        ratios[symbol]= ratio
                    if i['filterType'] == 'MIN_NOTIONAL':
                        notional_limit = float(i['notional'])
                        notional_limits[symbol]= notional_limit
                    if i['filterType'] == 'PRICE_FILTER':
                        ticksize = float(i['tickSize'])
                        tickSize[symbol]= ticksize
        
    logging.info(ratios)
    logging.info(notional_limits)
    logging.info(pricePrecisions)
    logging.info(tickSize)
    
#监听趋势数据
async def check_messages():
    global Mark_Price,allpositions,check_position_lock,lstkey,bn_client
    session = Session()
    if not lstkey == '':
        #logging.info("Renew_listen_key: {}".format(bn_client.renew_listen_key(lstkey)))
        try:
            bn_client.renew_listen_key(lstkey)    
        except Exception as err:
            logging.error("Try renew listenkey Err-type:{}\nError:{}".format(type(err),err))
            os.kill(os.getpid(), 9) 
    for symbol in pairlist:
        #logger.info("check_position_lock = {} ".format(check_position_lock))
        logging.info("读取 {} K线数据: ".format(symbol))
        logging.info( "计算 {} 趋势策略".format(symbol))
        klines_org = bn_client.mark_price_klines(symbol, "4h", **{"limit": 108})
        kl_close = []   
        for kline in klines_org:
            #logging.info( "kline[0]: {}".format(kline[0]))
            #logging.info( "kline[1]: {}".format(kline[1]))
            #logging.info( "kline[2]: {}".format(kline[2]))
            #logging.info( "kline[3]: {}".format(kline[3]))
            #logging.info( "kline[4]: {}".format(kline[4]))
            #logging.info( "kline[5]: {}".format(kline[5]))
            kl_close.append(fmtprice(float(kline[4]),symbol))
        #logging.info( "{} close[] = {}".format(symbol , kl_close ))

        a_sma6 = talib.SMA(numpy.real(kl_close),6)
        a_sma36 = talib.SMA(numpy.real(kl_close),36)
        a_rsi18 = talib.RSI(numpy.real(kl_close),18)
        sma6 = fmtprice(a_sma6[-1], symbol)
        sma36 = fmtprice(a_sma36[-1], symbol)
        rsi18 = fmtprice(a_rsi18[-1], symbol)
        #logging.info( "{} SMA(6)_Array = {}".format(symbol , a_sma6))
        logging.info( "{} SMA(6) = {}".format(symbol , sma6 ))
        #logging.info( "{} SMA(36)_Array = {}".format(symbol , a_sma36))
        logging.info( "{} SMA(36) = {}".format(symbol , sma36))
        #logging.info( "{} RSI(18)_Array = {}".format(symbol , a_rsi18))
        logging.info( "{} RSI(18) = {}".format(symbol , rsi18))
        if not check_position_lock:    
            
            rtmp = fmtprice(Mark_Price[symbol],symbol)
            strend = "NOP"
            if sma6 > sma36 and rtmp > sma36:
                strend = "LONG"
            elif sma6 < sma36 and rtmp < sma36 :
                strend = "SHORT"

            over = "Normal"
            if rsi18 > 75:
                over = "OverBuy"
            elif rsi18 < 15:
                over = "OverSell" 

            logging.info( "{} EA: {} stats: {}".format(symbol , strend, over))       
            allpositions[symbol]['strend'] = strend
            allpositions[symbol]['over'] = over            
            #kenix-414 formula
            
            base_line = fmtprice((rtmp - rtmp % allpositions[symbol]['grid_rate']),symbol)
            
            if not allpositions[symbol]['last_pivot'] > 0:
                allpositions[symbol]['last_pivot'] = fmtprice(base_line,symbol)
            '''
            logger.info("{} check_message  baseline :{}  last_pivot:{} ".format(symbol, base_line, allpositions[symbol]['last_pivot']))
            
            long_open = fmtprice(base_line + allpositions[symbol]['grid_rate'],symbol)
            long_close = fmtprice(base_line - allpositions[symbol]['back_rate'],symbol)
            short_open = fmtprice(base_line - allpositions[symbol]['grid_rate'],symbol)
            short_close = fmtprice(base_line + allpositions[symbol]['grid_rate'] + allpositions[symbol]['back_rate'],symbol)
            '''    

            
            if not allpositions[symbol]['last_pivot'] > 0:
                allpositions[symbol]['last_pivot'] = fmtprice(base_line,symbol)
            
            long_open = fmtprice(allpositions[symbol]['last_pivot'] + allpositions[symbol]['grid_rate'],symbol)
            long_close = fmtprice(allpositions[symbol]['last_pivot'] - allpositions[symbol]['back_rate'],symbol)
            short_open = fmtprice(allpositions[symbol]['last_pivot'] - allpositions[symbol]['grid_rate'],symbol)
            short_close = fmtprice(allpositions[symbol]['last_pivot'] + allpositions[symbol]['back_rate'],symbol)
                    

            #判断区间上下移动
            if session.query(LastBaseline).filter_by(symbol = symbol).count() == 0:
                lastbaselinedata = LastBaseline(    symbol = symbol,
                                                    baseline= 0.0
                                        
                                    )
                session.add(lastbaselinedata)
                session.commit()
            elif session.query(LastBaseline).filter_by(symbol = symbol).count() > 0:
                lastbaselinedata = session.query(LastBaseline).filter_by(symbol = symbol).first()   
                allpositions[symbol]['last_base_price'] = lastbaselinedata.baseline
                session.commit()
            if allpositions[symbol]['last_base_price'] == 0.0:
                lastbaselinedata = session.query(LastBaseline).filter_by(symbol = symbol).first()   
                lastbaselinedata.baseline = base_line
                session.commit()
                allpositions[symbol]['last_base_price'] = base_line
                
            else:
                if base_line > allpositions[symbol]['last_base_price']:
                    
                    allpositions[symbol]['last_base_price'] = base_line
                    lastbaselinedata = session.query(LastBaseline).filter_by(symbol = symbol).first()   
                    lastbaselinedata.baseline = base_line
                    session.commit()
                    if base_line > allpositions[symbol]['last_pivot']:
                        allpositions[symbol]['grid_move'] = 'UP'
                        allpositions[symbol]['last_pivot'] = fmtprice(allpositions[symbol]['last_pivot'] + allpositions[symbol]['grid_rate'],symbol)
                        long_open = fmtprice(allpositions[symbol]['last_pivot'] + allpositions[symbol]['grid_rate'],symbol)
                        long_close = fmtprice(allpositions[symbol]['last_pivot'] - allpositions[symbol]['back_rate'],symbol)
                        short_open = fmtprice(allpositions[symbol]['last_pivot'] - allpositions[symbol]['grid_rate'],symbol)
                        short_close = fmtprice(allpositions[symbol]['last_pivot'] + allpositions[symbol]['back_rate'],symbol)

                    '''
                    long_open = fmtprice(base_line + allpositions[symbol]['grid_rate'],symbol)
                    long_close = fmtprice(base_line - allpositions[symbol]['back_rate'],symbol)
                    short_open = fmtprice(base_line - allpositions[symbol]['grid_rate'],symbol)
                    short_close = fmtprice(base_line + allpositions[symbol]['grid_rate'] + allpositions[symbol]['back_rate'],symbol)
                    '''
                elif base_line < allpositions[symbol]['last_base_price']:
                    
                    
                    allpositions[symbol]['last_base_price'] = base_line
                    lastbaselinedata = session.query(LastBaseline).filter_by(symbol = symbol).first()   
                    lastbaselinedata.baseline = base_line
                    session.commit()
                    if base_line < allpositions[symbol]['last_pivot'] - allpositions[symbol]['grid_rate']:
                        allpositions[symbol]['grid_move'] = 'DOWN'
                        allpositions[symbol]['last_pivot'] = fmtprice(allpositions[symbol]['last_pivot'] - allpositions[symbol]['grid_rate'], symbol)
                        long_open = fmtprice(allpositions[symbol]['last_pivot'] + allpositions[symbol]['grid_rate'],symbol)
                        long_close = fmtprice(allpositions[symbol]['last_pivot'] - allpositions[symbol]['back_rate'],symbol)
                        short_open = fmtprice(allpositions[symbol]['last_pivot'] - allpositions[symbol]['grid_rate'],symbol)
                        short_close = fmtprice(allpositions[symbol]['last_pivot'] + allpositions[symbol]['back_rate'],symbol)

                    '''
                    long_open = fmtprice(base_line + 2* allpositions[symbol]['grid_rate'],symbol)
                    long_close = fmtprice(base_line - allpositions[symbol]['back_rate'],symbol)
                    short_open = fmtprice(base_line ,symbol)
                    short_close = fmtprice(base_line + allpositions[symbol]['grid_rate'] + allpositions[symbol]['back_rate'],symbol)
                    '''
                elif base_line == allpositions[symbol]['last_base_price']:
                    allpositions[symbol]['grid_move'] = ''
                    allpositions[symbol]['last_base_price'] = base_line
                    lastbaselinedata = session.query(LastBaseline).filter_by(symbol = symbol).first()   
                    lastbaselinedata.baseline = base_line
                    session.commit()
                    if not allpositions[symbol]['last_pivot'] > 0:
                        allpositions[symbol]['last_pivot'] = fmtprice(base_line,symbol)
                    long_open = fmtprice(allpositions[symbol]['last_pivot'] + allpositions[symbol]['grid_rate'],symbol)
                    long_close = fmtprice(allpositions[symbol]['last_pivot'] - allpositions[symbol]['back_rate'],symbol)
                    short_open = fmtprice(allpositions[symbol]['last_pivot'] - allpositions[symbol]['grid_rate'],symbol)
                    short_close = fmtprice(allpositions[symbol]['last_pivot'] + allpositions[symbol]['back_rate'],symbol)

                    
            logger.info("=============================================")
            logger.info("41X 收到 {} 当前报价 {}".format(symbol, rtmp))
            logger.info("{} grid_rate: {}  back_rate: {} ".format(symbol, allpositions[symbol]['grid_rate'], allpositions[symbol]['back_rate']))
            #logger.info("{} 多仓锚点(long_arch)为 {}".format(symbol, long_arch))
            logger.info("{} 锚点(base_line)为 {}".format(symbol, base_line))
            logger.info("{} 中轴(lastpivot)为 {}".format(symbol, allpositions[symbol]['last_pivot']))
            logger.info("{} 多仓开仓挂单价格(long_open)为 {}".format(symbol, long_open))
            logger.info("{} 多仓平仓挂单价格(long_close)为 {}".format(symbol, long_close))
            #logger.info("{} 空仓锚点(short_arch)为 {}".format(symbol, short_arch))
            logger.info("{} 空仓开仓挂单价格(short_open)为 {}".format(symbol, short_open))
            logger.info("{} 空仓平仓挂单价格(short_close)为 {}".format(symbol, short_close))
            
            #更新数据库
            pull_positions(allpositions)
            apiposition = allpositions[symbol]
            
            #持仓标签唯一检查
            hpositions = session.query(HPositions).filter_by(symbol = symbol).all()
            longtable = {}
            shorttable = {}
            for positions in hpositions:
                if positions.positionside == 'LONG':
                    if not str(positions.enterprice) in longtable: 
                        longtable[str(positions.enterprice)] = positions.positionamt
                    else:
                        longtable[str(positions.enterprice)] += positions.positionamt
                if positions.positionside == 'SHORT':
                    if not str(positions.enterprice) in shorttable:
                        shorttable[str(positions.enterprice)] = positions.positionamt
                    else:
                        shorttable[str(positions.enterprice)] += positions.positionamt

            longitems = 0
            shortitems = 0
            for position in hpositions:
                if position.positionside == 'LONG':
                    longitems += 1
                if position.positionside == 'SHORT':
                    shortitems += 1
            logging.info("本地持仓表中 {} 多仓持仓字典 {} ".format(symbol, longtable))
            logging.info("{} - 多仓 字典中 {} 项 数据表中 {} 项".format(symbol, len(longtable), longitems))                    
            logging.info("本地持仓表中 {} 空仓持仓字典 {} ".format(symbol, shorttable))
            logging.info("{} - 空仓 字典中 {} 项 数据表中 {} 项".format(symbol, len(shorttable), shortitems))
            
            for k,v in longtable.items():
                logging.info("Long Price: {} amount {}".format(k,v))
            for k,v in shorttable.items():
                logging.info("Short Price: {} amount {}".format(k,v))
             
            #持仓表项处理
            if not len(longtable) == longitems:
                for position in hpositions:
                    if position.positionside == 'LONG':
                        session.delete(position)
                        session.commit()
                for k,v in longtable.items():
                    position = HPositions( symbol = symbol, positionside = 'LONG', enterprice = float(k),positionamt = v)
                    session.add(position)
                    session.commit()

            if not len(shorttable) == shortitems:
                for position in hpositions:
                    if position.positionside == 'SHORT':
                        session.delete(position)
                        session.commit()
                for k,v in shorttable.items():
                    position = HPositions( symbol = symbol, positionside = 'SHORT', enterprice = float(k),positionamt = v)
                    session.add(position)
                    session.commit()
        
            hpositions = session.query(HPositions).filter_by(symbol = symbol).all()
            tlongpostions = 0
            tshortpostions = 0
            for position in hpositions:
                if position.positionside == 'LONG':
                    tlongpostions = tlongpostions + position.positionamt
                if position.positionside == 'SHORT':
                    tshortpostions = tshortpostions + position.positionamt
            logging.info("本地持仓表中 {} 多仓持仓 {} 空仓持仓 {}".format(symbol, tlongpostions, tshortpostions))
            logging.info("API表中 {} 多仓持仓 {} 空仓持仓 {}".format(symbol, apiposition['long_pos'], apiposition['short_pos']))            
            if tlongpostions == apiposition['long_pos'] and tshortpostions == apiposition['short_pos']:
                logging.info("持仓检查结果一致，执行挂单价格检查")
                #标签策略检查
                if not allpositions[symbol]['long_pos'] == 0 :
                    #long_one_sync(symbol)
                    long_position_lable_trace(symbol, allpositions[symbol]['last_pivot'])
                if not allpositions[symbol]['short_pos'] == 0:
                    #short_one_sync(symbol)
                    short_position_lable_trace(symbol, allpositions[symbol]['last_pivot'])
                #锚点偏移检查
                if allpositions[symbol]['grid_move'] == 'UP':
                    logger.info("{} 41X执行上移处理流程".format(symbol))
                    #up_calc_grid(symbol,long_open,short_open)
                    close_calc_grid(symbol, long_open, short_open,long_close, short_close)
                if allpositions[symbol]['grid_move'] == 'DOWN':
                    logger.info("{} 41X执行下移处理流程".format(symbol))
                    #down_calc_grid(symbol,short_open,long_open)
                    close_calc_grid(symbol, long_open, short_open,long_close, short_close)
                if allpositions[symbol]['grid_move'] == '':
                    logger.info("{} 41X执行例行挂单检查".format(symbol))
                    close_calc_grid(symbol, long_open, short_open,long_close, short_close)                             
                logger.info("=============================================")
            else:
                if not apiposition['long_pos'] == tlongpostions:
                    logging.info("{} 多仓检查结果差异，重置标签".format(symbol))
                    long_label_sync(symbol, apiposition['long_pos'], tlongpostions, long_open, long_close)
                if not tshortpostions == apiposition['short_pos']:
                    logging.info("{} 空仓检查结果差异，重置标签".format(symbol))
                    short_label_sync(symbol, apiposition['short_pos'], tshortpostions, short_open, short_close)
                logger.info("=============================================")
    
    session.close()

def long_label_sync(symbol,api_position, tab_longposition, long_open ,long_close):
    logging.info("{} 多仓检查结果差异  实际持仓 {}  标签持仓 {}".format(symbol, api_position, tab_longposition))
    global allpositions
    session = Session()
    #清理多仓同价开仓单
    long_orders = bn_client.get_orders(symbol = symbol, positionSide = "LONG",status = 'NEW', recvWindow = 3000)
    long_add_orders = []
    if len(long_orders) > 0 :
        for order in long_orders:
            if order['side'] == 'BUY' and order['positionSide'] == "LONG" :
                long_add_orders.append(order)
    if not len(long_add_orders) == 0:        
        for lborder in long_add_orders:
            if float(lborder['price']) <= allpositions[symbol]['last_pivot']:
                response = bn_client.cancel_order(symbol=symbol, orderId=lborder['orderId'], recvWindow=3000)

    #多持仓表同步处理#
    if api_position == 0: #无持仓，清空持仓表
        logging.info("{} 多仓无持仓，清空持仓标签表".format(symbol))
        if not session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').count() == 0:
            hpositionsdata = session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').all()    
            for pos in hpositionsdata:
                session.delete(pos)    
            session.commit()
    if api_position > 0 and tab_longposition == 0: #多持仓表初始化
        pull_positions(allpositions)
        p_pos = allpositions[symbol]['long_pos']
        fmtgridrate = fmtprice(allpositions[symbol]['grid_rate'],symbol)
        fmtlongent = fmtprice(allpositions[symbol]['long_entprice'],symbol)
        long_cmb_price_u = (fmtlongent - fmtlongent % fmtgridrate) + fmtgridrate
        long_cmb_price = fmtprice(float(int(long_cmb_price_u/fmtgridrate) * fmtgridrate), symbol)
        #清空标签表
        if not session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').count() == 0:
            hpositionsdata = session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').all()    
            for pos in hpositionsdata:
                session.delete(pos)    
            session.commit()
        #插入标签表记录
        position = HPositions(symbol = symbol, positionside = 'LONG', enterprice = long_cmb_price, positionamt = p_pos )
        session.add(position)
        session.commit()

    else:
        if api_position > tab_longposition: #多仓实际持仓大于持仓标签总数，以当前中轴价格标签补上持仓标签差缺
            logging.info("{} 多仓实际持仓大于持仓标签总数，以当前中轴价格标签补上持仓标签差缺".format(symbol))
            lable_price = fmtprice(allpositions[symbol]['last_pivot'],symbol)
            add_qty = api_position - tab_longposition
            if not session.query(HPositions).filter_by(symbol = symbol, enterprice = lable_price, positionside = 'LONG').count() == 0:
                hpositionsdata = session.query(HPositions).filter_by(symbol = symbol, enterprice = lable_price, positionside = 'LONG').first()    
                hpositionsdata.positionamt += add_qty
                session.commit()

            elif session.query(HPositions).filter_by(symbol = symbol, enterprice = lable_price, positionside = 'LONG').count() == 0:
                hpositionsdata = HPositions(symbol = symbol, enterprice = lable_price, positionside = 'LONG', positionamt = add_qty)
                session.add(hpositionsdata)
                session.commit()
            
        elif api_position < tab_longposition: #多仓实际持仓小于持仓标签总数，从当前持仓标签低价标签减去多出差缺        
            logging.info("{} 多仓实际持仓小于持仓标签总数，从当前持仓标签低价标签减去多出差缺".format(symbol))
            dec_pos = tab_longposition - api_position
            long_pos_table =session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').order_by(HPositions.enterprice.asc()).all()
            for longpos in long_pos_table:
                if not dec_pos == 0:
                    if longpos.positionamt > dec_pos:
                        longpos.positionamt = longpos.positionamt - dec_pos
                        dec_pos = 0
                        session.commit()
                    else:
                        dec_pos = dec_pos - longpos.positionamt
                        session.delete(longpos)
                        session.commit()                        
    

    session.close()

def short_label_sync(symbol, api_position, tab_shortposition,short_open, short_close):
    global allpositions
    session = Session()
    
    logging.info("{} 空仓检查结果差异，实际持仓 {} 标签持仓  {}".format(symbol, api_position, tab_shortposition))
    
    #清理空仓同价开仓单
    short_orders = bn_client.get_orders(symbol = symbol,positionSide = "SHORT",status = 'NEW', recvWindow = 3000)
    short_add_orders = []
    if len(short_orders) > 0:
        for order in short_orders:
            if order['side'] == 'SELL' and order['positionSide'] == "SHORT":
                short_add_orders.append(order)
    if not len(short_add_orders) == 0:        
        for ssorder in short_add_orders:
            if float(ssorder['price']) >= allpositions[symbol]['last_pivot']:
                response = bn_client.cancel_order(symbol=symbol, orderId=ssorder['orderId'], recvWindow=3000)    

    #空持仓表同步处理#
    if api_position == 0: #无持仓，清空持仓表
        logging.info("{} 空仓无持仓，清空持仓标签表".format(symbol))
        if not session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').count() == 0:
            hpositionsdata = session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').all()    
            for pos in hpositionsdata:
                session.delete(pos)    
            session.commit()
    if api_position > 0 and tab_shortposition == 0: #空持仓表初始化
        pull_positions(allpositions)
        p_pos = allpositions[symbol]['short_pos']
        fmtgridrate = fmtprice(allpositions[symbol]['grid_rate'],symbol)
        fmtshortent = fmtprice(allpositions[symbol]['short_entprice'],symbol)
        short_cmb_price_u = fmtshortent - fmtshortent % fmtgridrate
        short_cmb_price = fmtprice(float(int(short_cmb_price_u/fmtgridrate) * fmtgridrate),symbol)
        #清空标签表
        if not session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').count() == 0:
            hpositionsdata = session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').all()    
            for pos in hpositionsdata:
                session.delete(pos)    
            session.commit()
        #插入标签表记录
        position = HPositions(symbol = symbol, positionside = 'SHORT', enterprice = short_cmb_price, positionamt = p_pos )
        session.add(position)
        session.commit()

        
    else:
        if api_position > tab_shortposition: #空仓实际持仓大于持仓标签总数，在当前中轴价格补上持仓标签差缺
            logging.info("{} 空仓实际持仓大于持仓标签总数，以中轴价格为标签补上持仓标签差缺".format(symbol))
            lable_price = fmtprice(allpositions[symbol]['last_pivot'],symbol)
            add_qty = api_position - tab_shortposition
            if not session.query(HPositions).filter_by(symbol = symbol, enterprice = lable_price, positionside = 'SHORT').count() == 0:
                hpositionsdata = session.query(HPositions).filter_by(symbol = symbol, enterprice = lable_price, positionside = 'SHORT').first()    
                hpositionsdata.positionamt += add_qty
                session.commit()

            elif session.query(HPositions).filter_by(symbol = symbol, enterprice = lable_price, positionside = 'SHORT').count() == 0:
                hpositionsdata = HPositions(symbol = symbol, enterprice = lable_price, positionside = 'SHORT', positionamt = add_qty)
                session.add(hpositionsdata)
                session.commit()
            

        elif api_position < tab_shortposition: #空仓实际持仓小于持仓标签总数，从当前持仓标签高价标签减去多出差缺        
            logging.info("{} 空仓实际持仓小于持仓标签总数，从当前持仓标签高价标签减去多出差缺".format(symbol))
            dec_pos = tab_shortposition - api_position
            short_pos_table =session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').order_by(HPositions.enterprice.asc()).all()
            for shortpos in short_pos_table:
                if not dec_pos == 0:
                    if shortpos.positionamt > dec_pos:
                        shortpos.positionamt = shortpos.positionamt - dec_pos
                        dec_pos = 0
                        session.commit()
                    else:
                        dec_pos = dec_pos - shortpos.positionamt
                        session.delete(shortpos)
                        session.commit()                        
            
    
    session.close()

def long_position_lable_trace(symbol,mid_pivot):
    global allpositions
    session = Session()
    logging.info("--------------------------------------------")        
    logging.info("{} 多仓标签跟随".format(symbol))
    pull_positions(allpositions)
    p_pos = allpositions[symbol]['long_pos'] #读取API多仓持仓
    #计算多仓持仓手数
    if not p_pos == 0:
        l_hands = int(p_pos/allpositions[symbol]['handqty'])
    else:
        l_hands = 0      

    if not l_hands == 0:
        fmtlongent = fmtprice(allpositions[symbol]['long_entprice'],symbol) #计算持仓单价
        fmtgridrate = fmtprice(allpositions[symbol]['grid_rate'],symbol) #计算网格
        #计算多仓持仓单价标签
        if fmtprice(fmtlongent % fmtgridrate, symbol) == 0:
            long_cmb_price = fmtlongent
        else:    
            long_cmb_price_u = (fmtlongent - fmtlongent % fmtgridrate) + fmtgridrate
            long_cmb_price = fmtprice(float(int(long_cmb_price_u/fmtgridrate) * fmtgridrate), symbol)
        logging.info("--------------------------------------------")
        logging.info("{} 多仓标签跟随 共持仓{}手 持仓单价 {}".format(symbol, l_hands, fmtlongent))
        logging.info("{} 持仓单价标签 {} 市价中轴 {} ".format(symbol, long_cmb_price, mid_pivot))        
        logging.info("--------------------------------------------")
        if l_hands == 1: #多仓一手持仓策略检查
            label_check_pass = False #ce
            ##标签策略：持仓标签=持仓单价标签
            sgy_long_dataset = {}
            sgy_long_enterprice = []
            sgy_long_positionamt = []
            sgy_long_enterprice = [long_cmb_price ]
            sgy_long_positionamt = [ allpositions[symbol]['handqty'] ] 
            sgy_long_dataset['enterprice'] = sgy_long_enterprice
            sgy_long_dataset['positionamt'] = sgy_long_positionamt
            

            #读取多仓持仓表，多仓标签从小到大
            labledb_long_dataset = {}
            long_enterprice = []
            long_positionamt = []
            long_lables = session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').order_by(HPositions.enterprice.asc()).all() 
            
            for long_lable in long_lables:
                long_enterprice.append(long_lable.enterprice)
                long_positionamt.append(long_lable.positionamt)
            labledb_long_dataset['enterprice'] = long_enterprice
            labledb_long_dataset['positionamt'] = long_positionamt

            if sgy_long_dataset == labledb_long_dataset:
                label_check_pass = True

            if label_check_pass:
                logging.info("{} 策略标签检查通过".format(symbol))
            else:
                logging.info("{} 策略标签检查不通过，执行技术校准".format(symbol))
            
                clear_long_buy_orders(symbol)
                clear_long_sell_orders(symbol)
                logging.info("{} 清理多仓挂单".format(symbol))
                #清空标签表
                if not session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').count() == 0:
                    hpositionsdata = session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').all()    
                    for pos in hpositionsdata:
                        session.delete(pos)    
                    session.commit()
                #插入标签表记录
                for lp in sgy_long_dataset['enterprice']:
                    position = HPositions(symbol = symbol, positionside = 'LONG', enterprice = lp, positionamt = allpositions[symbol]['handqty'] )
                    session.add(position)
                    session.commit()
        elif l_hands == 2: #多仓二手持仓策略检查
            label_check_pass = False #ce
            ##标签策略：持仓标签=持仓单价标签
            sgy_long_dataset = {}
            sgy_long_enterprice = []
            sgy_long_positionamt = []
            price1 = fmtprice(fmtlongent - (fmtlongent % fmtgridrate),symbol)
            price2 = fmtprice(price1 + fmtgridrate, symbol)
            sgy_long_enterprice = [price1, price2]
            sgy_long_positionamt = [ allpositions[symbol]['handqty'], allpositions[symbol]['handqty'] ] 
            sgy_long_dataset['enterprice'] = sgy_long_enterprice
            sgy_long_dataset['positionamt'] = sgy_long_positionamt
            
            #读取多仓持仓表，多仓标签从小到大
            labledb_long_dataset = {}
            long_enterprice = []
            long_positionamt = []
            long_lables = session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').order_by(HPositions.enterprice.asc()).all() 
            
            for long_lable in long_lables:
                long_enterprice.append(long_lable.enterprice)
                long_positionamt.append(long_lable.positionamt)
            labledb_long_dataset['enterprice'] = long_enterprice
            labledb_long_dataset['positionamt'] = long_positionamt

            if sgy_long_dataset == labledb_long_dataset:
                label_check_pass = True

            if label_check_pass:
                logging.info("{} 策略标签检查通过".format(symbol))
            else:
                logging.info("{} 策略标签检查不通过，执行技术校准".format(symbol))

                clear_long_buy_orders(symbol)
                clear_long_sell_orders(symbol)
                logging.info("{} 清理多仓挂单".format(symbol))
                
                #清空标签表
                if not session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').count() == 0:
                    hpositionsdata = session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').all()    
                    for pos in hpositionsdata:
                        session.delete(pos)    
                    session.commit()
                #插入标签表记录
                for lp in sgy_long_dataset['enterprice']:
                    position = HPositions(symbol = symbol, positionside = 'LONG', enterprice = lp, positionamt = allpositions[symbol]['handqty'] )
                    session.add(position)
                    session.commit()
        elif l_hands == 3: #多仓三手持仓策略检查
            label_check_pass = False #ce
            ##标签策略：持仓标签=持仓单价标签
            sgy_long_dataset = {}
            sgy_long_enterprice = []
            sgy_long_positionamt = []
            #持仓策略计算
            if mid_pivot > fmtlongent:
                price1 = fmtprice(fmtlongent - (fmtlongent % fmtgridrate),symbol)
                price2 = fmtprice(price1 + fmtgridrate, symbol)
                price3 = fmtprice(price1 + fmtgridrate + fmtgridrate , symbol)
            else:
                price2 = fmtprice(fmtlongent - (fmtlongent % fmtgridrate),symbol)
                price3 = fmtprice(price2 + fmtgridrate, symbol)
                if mid_pivot > price2 - fmtgridrate: 
                    price1 = fmtprice(price2 - fmtgridrate , symbol) 
                else:
                    price1 = fmtprice(mid_pivot, symbol)

            sgy_long_enterprice = [price1, price2 , price3]            
            sgy_long_positionamt = [ allpositions[symbol]['handqty'], allpositions[symbol]['handqty'], allpositions[symbol]['handqty'] ] 
            sgy_long_dataset['enterprice'] = sgy_long_enterprice
            sgy_long_dataset['positionamt'] = sgy_long_positionamt
            
            #读取多仓持仓表，多仓标签从小到大
            labledb_long_dataset = {}
            long_enterprice = []
            long_positionamt = []
            long_lables = session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').order_by(HPositions.enterprice.asc()).all() 
            
            for long_lable in long_lables:
                long_enterprice.append(long_lable.enterprice)
                long_positionamt.append(long_lable.positionamt)
            labledb_long_dataset['enterprice'] = long_enterprice
            labledb_long_dataset['positionamt'] = long_positionamt

            
            if sgy_long_dataset == labledb_long_dataset:
                label_check_pass = True

            if label_check_pass:
                logging.info("{} 策略标签检查通过".format(symbol))
            else:
                logging.info("{} 策略标签检查不通过，执行技术校准".format(symbol))

                clear_long_buy_orders(symbol)
                clear_long_sell_orders(symbol)
                logging.info("{} 清理多仓挂单".format(symbol))
                
                #清空标签表
                if not session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').count() == 0:
                    hpositionsdata = session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').all()    
                    for pos in hpositionsdata:
                        session.delete(pos)    
                    session.commit()
                #插入标签表记录
                for lp in sgy_long_dataset['enterprice']:
                    position = HPositions(symbol = symbol, positionside = 'LONG', enterprice = lp, positionamt = allpositions[symbol]['handqty'] )
                    session.add(position)
                    session.commit()
        elif l_hands == 4: #多仓四手持仓策略检查
            label_check_pass = False #ce
            ##标签策略：持仓标签=持仓单价标签
            sgy_long_dataset = {}
            sgy_long_enterprice = []
            sgy_long_positionamt = []
            #持仓策略计算
            if mid_pivot > fmtlongent:
                price1 = fmtprice(fmtlongent - (fmtlongent % fmtgridrate),symbol)
                price2 = fmtprice(price1 + fmtgridrate, symbol)
                price3 = fmtprice(price1 + fmtgridrate + fmtgridrate , symbol)
                price4 = fmtprice(price1 + fmtgridrate + fmtgridrate + fmtgridrate , symbol)
            else:
                price3 = fmtprice(fmtlongent - (fmtlongent % fmtgridrate),symbol)
                price4 = fmtprice(price3 + fmtgridrate, symbol)
                if mid_pivot > price3 - fmtgridrate: 
                    price2 = fmtprice(price3 - fmtgridrate , symbol)
                    price1 = fmtprice(price2 - fmtgridrate , symbol)  
                else:
                    price2 = fmtprice(mid_pivot, symbol)
                    price1 = fmtprice(price2 - fmtgridrate , symbol) 

            sgy_long_enterprice = [price1, price2 , price3, price4]            
            sgy_long_positionamt = [ allpositions[symbol]['handqty'], allpositions[symbol]['handqty'], allpositions[symbol]['handqty'], allpositions[symbol]['handqty'] ] 
            sgy_long_dataset['enterprice'] = sgy_long_enterprice
            sgy_long_dataset['positionamt'] = sgy_long_positionamt
            
            #读取多仓持仓表，多仓标签从小到大
            labledb_long_dataset = {}
            long_enterprice = []
            long_positionamt = []
            long_lables = session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').order_by(HPositions.enterprice.asc()).all() 
            
            for long_lable in long_lables:
                long_enterprice.append(long_lable.enterprice)
                long_positionamt.append(long_lable.positionamt)
            labledb_long_dataset['enterprice'] = long_enterprice
            labledb_long_dataset['positionamt'] = long_positionamt

            
            if sgy_long_dataset == labledb_long_dataset:
                label_check_pass = True

            if label_check_pass:
                logging.info("{} 策略标签检查通过".format(symbol))
            else:
                logging.info("{} 策略标签检查不通过，执行技术校准".format(symbol))

                clear_long_buy_orders(symbol)
                clear_long_sell_orders(symbol)
                logging.info("{} 清理多仓挂单".format(symbol))
                
                #清空标签表
                if not session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').count() == 0:
                    hpositionsdata = session.query(HPositions).filter_by(symbol = symbol, positionside = 'LONG').all()    
                    for pos in hpositionsdata:
                        session.delete(pos)    
                    session.commit()
                #插入标签表记录
                for lp in sgy_long_dataset['enterprice']:
                    position = HPositions(symbol = symbol, positionside = 'LONG', enterprice = lp, positionamt = allpositions[symbol]['handqty'] )
                    session.add(position)
                    session.commit()

    session.close() 

def short_position_lable_trace(symbol, mid_pivot):
    global allpositions
    session = Session()
    logging.info("--------------------------------------------")        
    logging.info("{} 空仓标签跟随".format(symbol))
    pull_positions(allpositions)
    p_pos = allpositions[symbol]['short_pos'] #读取API空仓持仓
    #计算空仓持仓手数
    if not p_pos == 0:
        s_hands = int(p_pos/allpositions[symbol]['handqty'])
    else:
        s_hands = 0      

    if not s_hands == 0:
        fmtshortent = fmtprice(allpositions[symbol]['short_entprice'],symbol)
        fmtgridrate = fmtprice(allpositions[symbol]['grid_rate'],symbol)
        #计算持仓单价标签    
        if fmtprice(fmtshortent % fmtgridrate, symbol) == 0:
            short_cmb_price = fmtshortent
        else:
            short_cmb_price_u = fmtshortent - fmtshortent % fmtgridrate
            short_cmb_price = fmtprice(float(int(short_cmb_price_u/fmtgridrate) * fmtgridrate),symbol)
        logging.info("--------------------------------------------")
        logging.info("{} 空仓标签跟随 共持仓{}手 持仓单价 {}".format(symbol, s_hands, fmtshortent))
        logging.info("{} 持仓单价标签 {} 市价中轴 {} ".format(symbol, short_cmb_price, mid_pivot))        
        logging.info("--------------------------------------------")
        if s_hands == 1: #空仓一手持仓策略检查
            label_check_pass = False #ce
            ##标签策略：持仓标签=持仓单价标签
            sgy_short_dataset = {}
            sgy_short_enterprice = []
            sgy_short_positionamt = []
            sgy_short_enterprice = [short_cmb_price ]
            sgy_short_positionamt = [ allpositions[symbol]['handqty'] ] 
            sgy_short_dataset['enterprice'] = sgy_short_enterprice
            sgy_short_dataset['positionamt'] = sgy_short_positionamt
            

            #读取空仓持仓表，多仓标签从大到小
            labledb_short_dataset = {}
            short_enterprice = []
            short_positionamt = []
            short_lables = session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').order_by(HPositions.enterprice.desc()).all() 
            
            for short_lable in short_lables:
                short_enterprice.append(short_lable.enterprice)
                short_positionamt.append(short_lable.positionamt)
            labledb_short_dataset['enterprice'] = short_enterprice
            labledb_short_dataset['positionamt'] = short_positionamt

            if sgy_short_dataset == labledb_short_dataset:
                label_check_pass = True

            if label_check_pass:
                logging.info("{} 策略标签检查通过".format(symbol))
            else:
                logging.info("{} 策略标签检查不通过，执行技术校准".format(symbol))
                clear_short_buy_orders(symbol)
                clear_short_sell_orders(symbol)
                logging.info("{} 清理空仓挂单".format(symbol))
                
                #清空标签表
                if not session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').count() == 0:
                    hpositionsdata = session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').all()    
                    for pos in hpositionsdata:
                        session.delete(pos)    
                    session.commit()
                #插入标签表记录
                for sp in sgy_short_dataset['enterprice']:
                    position = HPositions(symbol = symbol, positionside = 'SHORT', enterprice = sp, positionamt = allpositions[symbol]['handqty'] )
                    session.add(position)
                    session.commit()
        elif s_hands == 2: #空仓二手持仓策略检查
            label_check_pass = False #ce
            ##标签策略：持仓标签=持仓单价标签
            sgy_short_dataset = {}
            sgy_short_enterprice = []
            sgy_short_positionamt = []
            price1 = fmtprice(fmtshortent - (fmtshortent % fmtgridrate),symbol)
            price2 = fmtprice(price1 - fmtgridrate, symbol)
            sgy_short_enterprice = [price1, price2 ]
            sgy_short_positionamt = [ allpositions[symbol]['handqty'], allpositions[symbol]['handqty']  ] 
            sgy_short_dataset['enterprice'] = sgy_short_enterprice
            sgy_short_dataset['positionamt'] = sgy_short_positionamt
            

            #读取空仓持仓表，多仓标签从大到小
            labledb_short_dataset = {}
            short_enterprice = []
            short_positionamt = []
            short_lables = session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').order_by(HPositions.enterprice.desc()).all() 
            
            for short_lable in short_lables:
                short_enterprice.append(short_lable.enterprice)
                short_positionamt.append(short_lable.positionamt)
            labledb_short_dataset['enterprice'] = short_enterprice
            labledb_short_dataset['positionamt'] = short_positionamt

            if sgy_short_dataset == labledb_short_dataset:
                label_check_pass = True

            if label_check_pass:
                logging.info("{} 策略标签检查通过".format(symbol))
            else:
                logging.info("{} 策略标签检查不通过，执行技术校准".format(symbol))
                clear_short_buy_orders(symbol)
                clear_short_sell_orders(symbol)
                logging.info("{} 清理空仓挂单".format(symbol))
                
                #清空标签表
                if not session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').count() == 0:
                    hpositionsdata = session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').all()    
                    for pos in hpositionsdata:
                        session.delete(pos)    
                    session.commit()
                #插入标签表记录
                for sp in sgy_short_dataset['enterprice']:
                    position = HPositions(symbol = symbol, positionside = 'SHORT', enterprice = sp, positionamt = allpositions[symbol]['handqty'] )
                    session.add(position)
                    session.commit()
        elif s_hands == 3: #空仓三手持仓策略检查
            label_check_pass = False #ce
            ##标签策略：持仓标签=持仓单价标签
            sgy_short_dataset = {}
            sgy_short_enterprice = []
            sgy_short_positionamt = []
            #持仓策略计算
            if mid_pivot < fmtshortent:
                price1 = fmtprice(fmtshortent - (fmtshortent % fmtgridrate),symbol)
                price2 = fmtprice(price1 - fmtgridrate, symbol)
                price3 = fmtprice(price1 - fmtgridrate - fmtgridrate, symbol)
            else:
                price2 = fmtprice(fmtshortent - (fmtshortent % fmtgridrate),symbol)
                price3 = fmtprice(price2 - fmtgridrate, symbol)
                if mid_pivot < price2 + fmtgridrate:
                    price1 = fmtprice(price2 + fmtgridrate, symbol )
                else:
                    price1 = fmtprice(mid_pivot, symbol)

            sgy_short_enterprice = [price1, price2, price3 ]
            sgy_short_positionamt = [ allpositions[symbol]['handqty'], allpositions[symbol]['handqty'], allpositions[symbol]['handqty'] ] 
            sgy_short_dataset['enterprice'] = sgy_short_enterprice
            sgy_short_dataset['positionamt'] = sgy_short_positionamt            

            #读取空仓持仓表，多仓标签从大到小
            labledb_short_dataset = {}
            short_enterprice = []
            short_positionamt = []
            short_lables = session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').order_by(HPositions.enterprice.desc()).all() 
            
            for short_lable in short_lables:
                short_enterprice.append(short_lable.enterprice)
                short_positionamt.append(short_lable.positionamt)
            labledb_short_dataset['enterprice'] = short_enterprice
            labledb_short_dataset['positionamt'] = short_positionamt

            if sgy_short_dataset == labledb_short_dataset:
                label_check_pass = True

            if label_check_pass:
                logging.info("{} 策略标签检查通过".format(symbol))
            else:
                logging.info("{} 策略标签检查不通过，执行技术校准".format(symbol))
                clear_short_buy_orders(symbol)
                clear_short_sell_orders(symbol)
                logging.info("{} 清理空仓挂单".format(symbol))
                
                #清空标签表
                if not session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').count() == 0:
                    hpositionsdata = session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').all()    
                    for pos in hpositionsdata:
                        session.delete(pos)    
                    session.commit()
                #插入标签表记录
                for sp in sgy_short_dataset['enterprice']:
                    position = HPositions(symbol = symbol, positionside = 'SHORT', enterprice = sp, positionamt = allpositions[symbol]['handqty'] )
                    session.add(position)
                    session.commit()
        elif s_hands == 4: #空仓四手持仓策略检查
            label_check_pass = False #ce
            ##标签策略：持仓标签=持仓单价标签
            sgy_short_dataset = {}
            sgy_short_enterprice = []
            sgy_short_positionamt = []
            #持仓策略计算
            if mid_pivot < fmtshortent:
                price1 = fmtprice(fmtshortent - (fmtshortent % fmtgridrate),symbol)
                price2 = fmtprice(price1 - fmtgridrate, symbol)
                price3 = fmtprice(price1 - fmtgridrate - fmtgridrate, symbol)
                price4 = fmtprice(price1 - fmtgridrate - fmtgridrate - fmtgridrate , symbol)
                
            else:
                price3 = fmtprice(fmtshortent - (fmtshortent % fmtgridrate),symbol)
                price4 = fmtprice(price3 - fmtgridrate, symbol)
                if mid_pivot < price3 + fmtgridrate:
                    price2 = fmtprice(price3 + fmtgridrate, symbol )
                    price1 = fmtprice(price2 + fmtgridrate, symbol )
                else:
                    price2 = fmtprice(mid_pivot, symbol)
                    price1 = fmtprice(mid_pivot + fmtgridrate, symbol)

            sgy_short_enterprice = [price1, price2, price3, price4 ]
            sgy_short_positionamt = [ allpositions[symbol]['handqty'], allpositions[symbol]['handqty'], allpositions[symbol]['handqty'], allpositions[symbol]['handqty'] ] 
            sgy_short_dataset['enterprice'] = sgy_short_enterprice
            sgy_short_dataset['positionamt'] = sgy_short_positionamt            

            #读取空仓持仓表，多仓标签从大到小
            labledb_short_dataset = {}
            short_enterprice = []
            short_positionamt = []
            short_lables = session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').order_by(HPositions.enterprice.desc()).all() 
            
            for short_lable in short_lables:
                short_enterprice.append(short_lable.enterprice)
                short_positionamt.append(short_lable.positionamt)
            labledb_short_dataset['enterprice'] = short_enterprice
            labledb_short_dataset['positionamt'] = short_positionamt

            if sgy_short_dataset == labledb_short_dataset:
                label_check_pass = True

            if label_check_pass:
                logging.info("{} 策略标签检查通过".format(symbol))
            else:
                logging.info("{} 策略标签检查不通过，执行技术校准".format(symbol))
                clear_short_buy_orders(symbol)
                clear_short_sell_orders(symbol)
                logging.info("{} 清理空仓挂单".format(symbol))
                #清空标签表
                if not session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').count() == 0:
                    hpositionsdata = session.query(HPositions).filter_by(symbol = symbol, positionside = 'SHORT').all()    
                    for pos in hpositionsdata:
                        session.delete(pos)    
                    session.commit()
                #插入标签表记录
                for sp in sgy_short_dataset['enterprice']:
                    position = HPositions(symbol = symbol, positionside = 'SHORT', enterprice = sp, positionamt = allpositions[symbol]['handqty'] )
                    session.add(position)
                    session.commit()
        
    session.close()
   
#执行轮询
async def run_loop():
    SLEEP_DURATION = 3.6 
    while True:
        await asyncio.gather(asyncio.sleep(SLEEP_DURATION), check_messages())
            
if __name__ == "__main__":
    logging.info("God bless SmartGrid™ Snipper AI_Trade_Bot® !!!")    
    logging.info("SmartGrid™ Snipper AI_Trade_Bot® V{} Start@{} ".format("41X",datetime.fromtimestamp(bn_client.time()['serverTime']/1000)))        
    init_pull_positions(allpositions)
    pull_positions(allpositions)
    get_exchange_info(ratios,notional_limits,pricePrecisions,tickSize)
    
    try:
        listen_key = bn_client.new_listen_key()
        lstkey = listen_key["listenKey"]
        #ws_client.start()
        mp_client = UMFuturesWebsocketClient(on_message=update_markprice_handler, is_combined=True)
        install_markprice_listener(pairlist,mp_client)
        user_client = UMFuturesWebsocketClient(on_message=userdata_handler)
        install_userdata_listener(lstkey,user_client)
        
    except Exception as err:
        raise
        logging.error("_main_ Err-type:{}\nError:{}".format(type(err),err))
        os.kill(os.getpid(), 9)
    time.sleep(3)
    asyncio.run(run_loop())

