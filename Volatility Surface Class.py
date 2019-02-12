"""
Author: Peng Jin
Date: 01/30/2019
"""

import json
import websocket
import traceback
import helper
import ssl
import time as time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.interpolate import griddata
from mpl_toolkits.mplot3d import Axes3D


class vol_surface(object):

    """Derbit volatiolity analytics tool for decision making"""

    def __init__(self, url='', on_message=None, traceback=2, save_local=False, plot_type=None):
        """
        Program constructor
        :param url: Requested websocket address
        :param on_message: event message
        :param traceback: number of hours to look back from
        :param save_local: True if data is stored to local
        :param plot_type: Plot type （currently support scatter plot 2D, scatter plot 3D, and surface plot 3D
        """
        self.url = url
        self.traceback = traceback
        self.save_local = save_local
        self.plot_type = plot_type
        try:
            self.vol_data = pd.read_csv("volatility.csv")
        except FileNotFoundError:
            self.vol_data = pd.DataFrame()
        self.ws = None
        self.active = False
        self.on_message = on_message
        self.action = "/api/v1/public/getlasttrades"

    def on_message(self, message):
        """
        Websocket response message
        :param message: response message in dict format.
        """
        if self.on_message:
            self.on_message()
        else:
            print(message)

    def start(self):
        """
        Start websocket
        """
        self.ws = websocket.create_connection(self.url, sslopt={'cert_reqs': ssl.CERT_NONE})
        self.active = True
        self.on_connect()
        self.run()


    def on_connect(self):
        """
        Call when websocket is connected.
        """
        print('connected')

    def reconnect(self):
        """
        Reconnect to websocket server.
        """
        self.ws = websocket.create_connection(self.url, sslopt={'cert_reqs': ssl.CERT_NONE})
        self.on_connect()

    def on_error(self, err):
        """
        Print message when error occur
        """
        print(err)

    def send_req(self, req):
        """
        Send request to websocket server
        """
        self.ws.send(json.dumps(req))
        print(req)

    @staticmethod
    def concurrent_data_handler(message):
        """
        using pandas to transform the message into format we intended
        :param message: message received from websocket
        :return: revised data-stream
        """
        temp_df = pd.DataFrame(message['result'])
        temp_df = temp_df[['instrument', 'direction', 'indexPrice', 'price', 'quantity', 'iv', 'timeStamp', 'tradeId']]
        temp_df['timeStamp'] = temp_df['timeStamp'] / 1000
        temp_df['C-P'] = temp_df['instrument'].str.split('-', expand=True)[3]
        temp_df['strike'] = temp_df['instrument'].str.split('-', expand=True)[2].astype(float)
        temp_df['end_ts'] = pd.DataFrame(
            pd.to_datetime(temp_df['instrument'].str.split('-', expand=True)[1]).values.astype(np.int64) / 1000000000)
        temp_df['expiration_t'] = (temp_df['end_ts'] - temp_df['timeStamp']) / (365 * 24 * 3600)
        temp_df['option_price'] = temp_df['price'] * temp_df['indexPrice']
        return temp_df

    @staticmethod
    def vis_tool(df, exp_ts, plot_type="scatter_3D"):
        """
        Help to visualize the volatility skew/smile of past trades
        :param df: A dictionary object passed from the previous function
        :param exp_ts: expiration time
        :param plot_type: Plot type （currently support scatter plot 2D, scatter plot 3D, and surface plot 3D）
        :return: A PyPlot object
        """
        x = df['strike']
        y = df['expiration_t']
        z = df['iv']
        area = df['quantity'] * 3  # this is a scalar used for drawing

        def make_surf(x, y, z):
            x_grids, y_grids = np.meshgrid(np.linspace(min(x), max(x), 100), np.linspace(min(y), max(y), 100))
            z_grids = griddata(np.array([x, y]).T, np.array(z), (x_grids, y_grids), method='linear')
            return x_grids, y_grids, z_grids

        x_grids, y_grids, z_grids = make_surf(x, y, z)


        if plot_type == "scatter_2D":
            # Plot axes
            fig = plt.figure()
            ax = plt.axes()
            scat = plt.scatter(x=x, y=z, s=area, c=z)
            plt.set_cmap('viridis_r')
            fig.colorbar(scat, shrink=0.5, aspect=5)
            # Add fitted line for the scatter plot
            fitted_data = np.polyfit(x, z, 3)
            p = np.poly1d(fitted_data)
            xp = np.linspace(x.min(), x.max(), 100)
            plt.plot(xp, p(xp), '-', alpha=0.3, color='red')
            # Set x axis label
            plt.xlabel('Strike')
            # Set y axis label
            plt.ylabel('Implied Volatility')
            # Set size legend
            for area in [area.min(), area.mean(), area.max()]:
                plt.scatter([], [], alpha=0.3, s=area, color='grey', label=str(round(area / 3, 2)))
                handles, labels = ax.get_legend_handles_labels()
                plt.legend(handles[-3:], labels[-3:], scatterpoints=1, labelspacing=1, title='Order Size')

        if plot_type == "surface_3D":
            fig = plt.figure()
            ax = plt.axes(projection='3d')
            surf = ax.plot_surface(x_grids, y_grids, z_grids, cmap='viridis',
                                   vmax=z.max(), vmin=z.min(), cstride=5, rstride=5,
                                   antialiased=True)
            fig.colorbar(surf, shrink=0.5, aspect=5)
            ax.set_xlabel('Strike Price')
            ax.set_ylabel('Time Remain to Expiration')
            ax.set_zlabel('Implied Volatility')

        time_object = time.gmtime(exp_ts)
        plt.title("Options expiring on %s/%s/%s %s:00:00 (GMT Time)" % (time_object.tm_mon, time_object.tm_mday,
                                                                      time_object.tm_year, time_object.tm_hour))
        plt.show()

    def save_data(self, data, path='volatility.csv'):
        """
        Save streaming data to local
        :param data: Websocket data stream
        :param path: Name of the file
        :return: None
        """
        self.vol_data = pd.concat([self.vol_data, data], axis=0)
        self.vol_data = self.vol_data.drop_duplicates(subset='tradeId', keep='last')
        # self.vol_data = self.vol_data.reset_index(inplace=True)
        self.vol_data.to_csv(path, index=False)

    def run(self):
        """
        listen to ws messages
        :return: volatility analytic plots
        """
        while self.active:
            arguments = {
                "instrument": "options",
                "startTimestamp": (time.time() - self.traceback * 60 * 60) * 1000,  # Get trades in the last **self.traceback** hours
                "count": 1000
            }
            try:
                self.send_req(req={
                    "action": self.action,
                    "id": 666,
                    "arguments": arguments,
                    "sig": helper.get_signature(action=self.action, arguments=arguments),
                    "message": "heartbeat"
                })
                stream = self.ws.recv()
                message = json.loads(stream)
                data = self.concurrent_data_handler(message)
                if self.save_local:
                    self.save_data(data=data)
                dfs = dict(tuple(data.groupby('end_ts')))  # Here we break down the dataframe by end_ts
                for i in dfs:
                    self.vis_tool(df=dfs[i], exp_ts=i, plot_type=self.plot_type)
            except ConnectionError:
                msg = traceback.print_exc()
                self.on_error(msg)
                self.reconnect()


if __name__ == '__main__':

        url = "wss://www.deribit.com/ws/api/v1"

        test = vol_surface(url, traceback=12, plot_type="scatter_2D", save_local=True)

        test.start()
