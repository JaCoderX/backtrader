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
from backtrader.feeds.ccxt import CCXT


class CCXTMultiBroker(BrokerBase):
    '''Multi Exchange Broker implementation for CCXT cryptocurrency trading library.

    This class maps the orders/positions from CCXT to the
    internal API of ``backtrader``.
    '''

    class BrokerAccount:
        def __init__(self, store, currency, rate):
            self.store = store
            self.currency = currency
            self.cash = 0
            self.value = 0
            self.currency_conversion_rate = rate  # for the case that exchange currency isn't the same as the main currency

    order_types = {Order.Market: 'market',
                   Order.Limit: 'limit',
                   Order.Stop: 'stop',
                   Order.StopLimit: 'stop limit'}

    def __init__(self, main_currency, retries=5):
        super(CCXTMultiBroker, self).__init__()
        self.brokers = dict()
        self.total_cash = 0.0
        self.total_value = 0.0
        self.main_currency = main_currency
        self.notifs = queue.Queue()  # holds orders which are notified (from all exchanges)
        self.retries = retries

    def add_exchange(self, exchange: str, config, currency, rate = 1.0):
        store = CCXTStore(exchange, config, self.retries)
        exchange = exchange.lower()
        self.brokers[exchange] = self.BrokerAccount(store, currency, rate)

    def next(self):
        # update account balance info for the next cycle
        self.get_cash(cached = False)
        self.get_value(cached = False)

    def broker(self, exchange: str):
        return self.brokers[exchange.lower()]

    def store(self, exchange):
        return self.broker(exchange).store

    def getcash(self, exchange = None):
        return self.total_cash if exchange is None else self.broker(exchange).cash

    def get_cash(self, exchange = None, cached = True):
        if cached:
            return self.getcash(exchange)

        if exchange is not None:
            self.broker(exchange).cash = self.store(exchange).getcash(self.store(exchange).currency)
            return self.broker(exchange).cash

        # default is to aggregate all the cash and update the cache
        # note: the system is calling the default on every loop prior to the strategy.
        #       so it is safe to assume that a cache update will be performed at least once per cycle
        self.total_cash = 0.0
        for exchange, Account in self.brokers.items():
            # update the account cash info
            Account.cash = Account.store.getcash(Account.currency)

            # aggregate all the cash from across all exchanges under one main currency.
            self.total_cash += Account.cash * Account.currency_conversion_rate

        return self.total_cash

    def getvalue(self, datas = None, exchange = None):
        return self.total_value if exchange is None else self.broker(exchange).value

    def get_value(self, datas = None, exchange = None, cached = True):
        if cached:
            return self.getvalue(exchange)

        if exchange is not None:
            self.broker(exchange).value = self.store(exchange).getvalue(self.store(exchange).currency)
            return self.broker(exchange).value

        # default is to aggregate all the value and update the cache
        # note: the system is calling the default on every loop prior to the strategy.
        #       so it is safe to assume that a cache update will be performed at least once per cycle
        self.total_value = 0.0
        for exchange, Account in self.brokers.items():
            # update the account value info
            Account.value = Account.store.getvalue(Account.currency)

            # aggregate all the value from across all exchanges under one main currency.
            self.total_value += Account.value * Account.currency_conversion_rate

        return self.total_value

    def get_notification(self):
        try:
            return self.notifs.get(False)
        except queue.Empty:
            return None

    def notify(self, order):
        self.notifs.put(order)

    def getposition(self, data):
        if isinstance(data, CCXT):
            exchange = data.store.exchange.name
            currency = data.symbol.split('/')[0]
            return self.store(exchange).getposition(currency)

    def _submit(self, owner, data, exectype, side, amount, price, params):
        # This is where the magic happens.
        # All data that are of type CCXT data feed will have the access to the exchange that belong to them
        if isinstance(data, CCXT):
            exchange = data.store.exchange.name

            order_type = self.order_types.get(exectype)
            ccxt_order = self.store(exchange).create_order(symbol=data.symbol, order_type=order_type, side=side,
                                                           amount=amount, price=price, params=params)
            order = CCXTOrder(owner, data, ccxt_order)
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
        exchange = order.data.store.exchange.name
        return self.store(exchange).cancel_order(order)

    def get_orders_open(self, exchange):
        return self.store(exchange).fetch_open_orders()
