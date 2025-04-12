import websocket
import threading
import json
import time
import traceback
import requests
import queue
import pandas as pd
import telebot
from telebot import types

class SocketConn_Binance(websocket.WebSocketApp):
    def __init__(self, url,):#q):
        super().__init__(url=url, on_open=self.on_open)

        self.on_message = lambda ws, msg: self.message(msg)
        self.on_error = lambda ws, e: self.on_errors(f'{traceback.format_exc()}')
        self.on_close = lambda ws: self.on_closes('CLOSING')

        #self.q = q

        self.df = None

        self.ist_5m = []

        self.run_forever()

    def on_closes(self,txt):
        print(txt)

    def on_errors(self, error):
        print(error)
        self.reconnect()

    def reconnect(self):
        print('Reconnecting...')
        time.sleep(5)
        self.__init__(self.url)

    def on_open(self, ws):
        print("Websocket was opened")

    def message(self, msg):
        msg = json.loads(msg)

        def time(df):
            last_value = df['T'].iloc[-1]-15*60000
            df = df[df['T'] >= last_value]
            return df

        def sum_Liqvidation(df, s, S):
            licvidations = 0

            for i in range(len(df)):
                if df['s'].iloc[i] == s and df['S'].iloc[i] == S:
                    licvidations += df['p'].iloc[i]

            return round(licvidations,2)

        if msg.get('e') != None and msg.get('e') == 'forceOrder' and msg.get('o') != None:
            msg = msg.get('o')
            #print(msg)
            msg = {'s':msg.get('s'),'S':msg.get('S'),'ap':float(msg.get('ap')),'p':round(float(msg.get('q'))*float(msg.get('p')), 2),'T':int(msg.get('T'))}

            if self.df is not None and isinstance(self.df, pd.DataFrame):
                self.df.loc[len(self.df)] = msg
            else:
                self.df = pd.DataFrame(msg, index=[0])

            self.df = time(self.df)
            licvidations = sum_Liqvidation(self.df, msg.get('s'), msg.get('S'))

            #print(self.df)
            print(licvidations)

            if msg.get('ap') != None and licvidations > 30000 and licvidations > msg.get('ap')*6: #ЭТА СТРОКА!!!!!!!!
                go = True
                for i in self.ist_5m:
                    if i.get('s') == msg.get('s') and i.get('S') == msg.get('S'):
                        if i.get('T') + 5*60000 > msg.get('T'):
                            go = False
                        else:
                            self.ist_5m.remove(i)
                        break
                if go:
                    self.ist_5m.append(msg)
                    msg['licvidations'] = licvidations
                    worker(msg)
                    #self.q.put(msg)

def worker(task):#q):
    API = '7322937059:AAHWbYHdmMXhZxNuBK8ujPC8agYuFScxTkw'
    bot = telebot.TeleBot(API)

    #while True:
    z = True
    if z:
        #task = q.get()  # Получаем задачу из очереди
        #if task is None:
            #print(task)

        print(f"Поток обработал задачу: s: {task['s']}, l: {task['licvidations']}, ap: {task['ap']}, np: {task['S']}")

        if task.get('S') == 'BUY':
            np = '🟩'
        else:
            np = '🟥'

        txt = (f"#{task.get('s')} <code>{task.get('s')}</code>\n"
               f"{np}{task.get('S')} | sum: {task.get('licvidations')}$\n"
               f"ap: {task.get('ap')}$")

        markup = types.InlineKeyboardMarkup()

        b1 = types.InlineKeyboardButton(text='TV',
                                        url=f"https://ru.tradingview.com/chart/{task.get('s')}.P")
        b2 = types.InlineKeyboardButton(text='CG',
                                        url=f"https://www.coinglass.com/tv/ru/Binance_{task.get('s')}")
        markup.add(b1, b2)
        try:
            bot.send_message('@REPATEST', txt, parse_mode='HTML', reply_markup=markup )
        except Exception as e:
            print('\nОшибка:\n', traceback.format_exc())
        time.sleep(1)

def main():
    #q = queue.Queue()
    #t = threading.Thread(target=worker, args=(q,))
    #t.start()
    while True:
        zx = threading.Thread(target=SocketConn_Binance, args=(f'wss://fstream.binance.com/ws/!forceOrder@arr',))#q,))

        zx.start()
        zx.join()

        time.sleep(5)


if __name__ == '__main__':
    main()
