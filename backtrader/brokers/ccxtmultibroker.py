#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015, 2016, 2017 Daniel Rodriguez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from backtrader import BrokerBase, Order
from backtrader.utils.py3 import queue
from backtrader.stores.ccxtstore import CCXTStore, CCXTOrder


class CCXTOrderForMultiBroker(CCXTOrder):
    def __init__(self, owner, data, ccxt_order, exchange):
        super(CCXTOrderForMultiBroker, self).__init__(owner, data, ccxt_order)
        self.exchange = exchange


class CCXTMultiBroker(BrokerBase):
    '''Multi Exchange Broker implementation for CCXT cryptocurrency trading library.

    This class maps the orders/positions from CCXT to the
    internal API of ``backtrader``.
    '''

    order_types = {Order.Market: 'market',
                   Order.Limit: 'limit',
                   Order.Stop: 'stop',
                   Order.StopLimit: 'stop limit'}

    def __init__(self, main_currency, retries=5):
        super(CCXTMultiBroker, self).__init__()
        self.stores = dict()
        self.active_store = None
        self.main_currency = main_currency
        self.notifs = queue.Queue()  # holds orders which are notified (from all exchanges)
        self.retries = retries

    def add_exchange(self, exchange, config, currency):
        store = CCXTStore(exchange, config, self.retries)
        self.stores[exchange] = {'store': store, 'currency': currency}
        if not self.active_store:
            self.active_store = store

    def switch_exchange(self, exchange):
        self.active_store = self.store(exchange)

    def store(self, exchange):
        return self.stores[exchange]['store']

    def getcash(self):
        return self.active_store.getcash(self.currency)

    def getvalue(self, datas=None):
        return self.active_store.getvalue(self.currency)

    def get_notification(self):
        try:
            return self.notifs.get(False)
        except queue.Empty:
            return None

    def notify(self, order):
        self.notifs.put(order)

    def getposition(self, data):
        currency = data.symbol.split('/')[0]
        return self.active_store.getposition(currency)

    def _submit(self, owner, data, exectype, side, amount, price, params):
        # 'exchange' is a special kwargs (not part of ccxt create_orders params) that can be paste in buy/sell actions
        # to allow backtrader to switch between ccxt exchanges and save action history without breaking functionality.
        # this is made possible cause a reference to exchange can be saved as part of the order records
        # for later reference, for example in notification or cancel order.
        exchange = params.pop('exchange', self.active_store.exchange.name)

        order_type = self.order_types.get(exectype)
        ccxt_order = self.store(exchange).create_order(symbol=data.symbol, order_type=order_type, side=side,
                                        amount=amount, price=price, params=params)
        order = CCXTOrderForMultiBroker(owner, data, ccxt_order, exchange)
        self.notify(order)
        return order

    def buy(self, owner, data, size, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0, oco=None,
            trailamount=None, trailpercent=None,
            **kwargs):
        return self._submit(owner, data, exectype, 'buy', size, price, kwargs)

    def sell(self, owner, data, size, price=None, plimit=None,
             exectype=None, valid=None, tradeid=0, oco=None,
             trailamount=None, trailpercent=None,
             **kwargs):
        return self._submit(owner, data, exectype, 'sell', size, price, kwargs)

    def cancel(self, order):
        return self.store(order.exchange).cancel_order(order)

    def get_orders_open(self, exchange):
        return self.store(exchange).fetch_open_orders()
