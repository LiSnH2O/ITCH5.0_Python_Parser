# -*- coding: utf-8 -*-
"""
Created on Fri Sep 24 23:58:12 2019

@author: Xiyu Li
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 06:38:21 2019

@author: Xiyu Li
"""
# import modules
import gzip
import struct
from datetime import timedelta
import pandas as pd
import os

# Use Object oriented programming
# Create operations on the ITCH files as member function
class ITCH():
    # defalut constructor
    def __init__(self):
        # member data
        self.temp = []
        self.flag = None
        if not os.path.exists(os.path.join('.', 'output')):
            os.makedirs(os.path.join('.', 'output'))
    # member function
    def get_binary(self, size):
        read = bin_data.read(size)
        return read
    
    # interpert the information in binary code
    def trade_message(self, msg):
        # >HH6sQsI8sIQ
        # big-endian; follow the format of ITCH Documentation
        temp = struct.unpack('>HH6sQsI8sIQ', message)
        t = int.from_bytes(temp[2], byteorder='big')
        x='{0}'.format(timedelta(seconds=t * 1e-9))
        return [x, bytes.decode(temp[6]).strip(), temp[7]/10000, temp[5]], x.split(':')[0]
    
    # calculate the weighted average price for each stock
    def cal_vwap(self, df):
        df['amount'] = df['price'] * df['volume']
        df['time'] = pd.to_datetime(df['time'])
        df = df.groupby([df['time'].dt.hour, df['symbol']])['amount', 'volume'].sum()
        df['vwap'] = df['amount'] / df['volume']
        # if need to keep 2 digits after decimal, un-comment the code below
        # df['vwap'] = df['vwap'].round(2)
        df = df.reset_index()
        df['time'] = df.apply(lambda x: str(x['time']) + ':00:00', axis=1)
        df = df[['time', 'symbol', 'vwap']]
        return df
    
    # group information accroding to hours and compute hourly VWAP
    def get_vwap(self, message):
        parsed_data, hour = self.trade_message(message)
        if self.flag is None:
            self.flag = hour
        if self.flag != hour:
            df = pd.DataFrame(self.temp, columns=['time', 'symbol', 'price', 'volume'])
            result = self.cal_vwap(df)
            result.to_csv(os.path.join(str(self.flag) + '.csv'), index=False)
            #print(result)
            self.temp = []
            self.flag = hour
        self.temp.append(parsed_data)
        
        
if __name__ == '__main__':
    # load file
    bin_data = gzip.open('E:\\Trexquant Coding Test\\01302019.NASDAQ_ITCH50.gz', 'rb')
    # create instance of the class
    itch= ITCH()
    # load first batch of bytes
    msg_header = bin_data.read(1)

    # iterate throught the binary file
    while msg_header:
        if msg_header == str.encode("S"):
            # tranverse the index to 11 places later
            message = itch.get_binary(11)

        elif msg_header == str.encode("R"):
            message = itch.get_binary(38)

        elif msg_header == str.encode("H"):
            message = itch.get_binary(24)

        elif msg_header == str.encode("Y"):
            message = itch.get_binary(19)

        elif msg_header == str.encode("L"):
            message = itch.get_binary(25)

        elif msg_header == str.encode("V"):
            message = itch.get_binary(34)

        elif msg_header == str.encode("W"):
            message = itch.get_binary(11)

        elif msg_header == str.encode("K"):
            message = itch.get_binary(27)

        elif msg_header ==str.encode("A"):
            message = itch.get_binary(35)

        elif msg_header == str.encode("F"):
            message = itch.get_binary(39)

        elif msg_header == str.encode("E"):
            message = itch.get_binary(30)

        elif msg_header == str.encode("C"):
            message = itch.get_binary(35)

        elif msg_header == str.encode("X"):
            message = itch.get_binary(22)

        elif msg_header == str.encode("D"):
            message = itch.get_binary(18)

        elif msg_header == str.encode("U"):
            message = itch.get_binary(34)

        elif msg_header == str.encode("P"):
            message = itch.get_binary(43)
            itch.get_vwap(message)

        elif msg_header == str.encode("Q"):
            message = itch.get_binary(39)

        elif msg_header == str.encode("B"):
            message = itch.get_binary(18)

        elif msg_header == str.encode("I"):
            message = itch.get_binary(49)

        elif msg_header == str.encode("N"):
            message = itch.get_binary(19)
            
        msg_header = bin_data.read(1)

    bin_data.close()
    #print(itch.temp)
