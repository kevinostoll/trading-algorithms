# https://quantpedia.com/strategies/realized-skewness-predicts-equity-returns/
#
# Investment universe consists of stocks from NYSE, AMEX and Nasdaq with a price higher than 5$. Only stocks from the highest size quintile are then used in trading.
# Intraday, 5-minute quotes, are used to calculate 5-minutes log-returns for each stock. Investor calculates skewness for each stock from those intraday data, and 
# resultant daily measure is aggregated into weekly realized skewness. Negative values indicate that the stock’s return distribution has a left tail that is fatter
# than the right tail, and positive values indicate the opposite.
# Each week, investor sorts stocks into deciles based on the current-week realized moment and buys the portfolio of stocks with the highest realized skewness and
# sells the portfolio of stocks with the lowest realized skewness. The portfolio is value-weighted and rebalanced on a weekly basis.

#region imports
from AlgorithmImports import *
from collections import deque
from scipy.stats import skew
import numpy as np
#endregion

class RealizedSkewnessPredictsEquityReturns(QCAlgorithm):
    
    def Initialize(self):
        self.SetStartDate(2019, 1, 1)
        self.SetCash(100000)

        self.symbol = self.AddEquity('SPY', Resolution.Minute).Symbol

        self.weight = {}
        
        # 5Minute price data.
        self.data = {}
        self.period = 5 * 78
        
        self.coarse_count = 100

        # Yearly selected universe with symbol and market cap data.
        self.selected_universe = []
        
        self.month = 12
        self.days = 5
        self.selection_flag = False
        self.UniverseSettings.Resolution = Resolution.Minute
        self.AddUniverse(self.CoarseSelectionFunction, self.FineSelectionFunction)
        self.Schedule.On(self.DateRules.MonthStart(self.symbol), self.TimeRules.AfterMarketOpen(self.symbol), self.Selection)
        
    def OnSecuritiesChanged(self, changes):
        for security in changes.AddedSecurities:
            symbol = security.Symbol
            
            security.SetFeeModel(CustomFeeModel())
            security.SetLeverage(5)
            
            if symbol not in self.data:
                history = self.History(symbol, self.period * 5, Resolution.Minute)
                closes_5M = []
                if len(history) == self.period and 'close' in history:
                    closes_1M = [x for x in history['close']]
                    closes_5M = closes_1M[::5]
                self.data[symbol] = deque(closes_5M, maxlen = self.period)
        
        # Remove old stocks from selected universe data.
        for security in changes.RemovedSecurities:
            if symbol in self.data:
                del self.data[symbol]
            
    def CoarseSelectionFunction(self, coarse):
        if not self.selection_flag: 
            return Universe.Unchanged
            
        selected = sorted([x for x in coarse if x.HasFundamentalData and x.Price > 5 and x.Market == 'usa'],
            key=lambda x: x.DollarVolume, reverse=True)
        
        self.selection_flag = False
        
        return [x.Symbol for x in selected[:self.coarse_count]]

    def FineSelectionFunction(self, fine):
        sorted_by_market_cap = sorted(fine, key = lambda x:x.MarketCap, reverse = True)
        self.selected_universe = [(x.Symbol, x.MarketCap) for x in sorted_by_market_cap[:self.coarse_count]]

        selected_symbols = [x[0] for x in self.selected_universe]
        
        # newly_added = [x[0] for x in top_by_market_cap if x[0] not in self.data]
        # traded_symbols = [x[0] for x in self.weight.items()]
        # return list(set(newly_added) | set(traded_symbols))
        return selected_symbols
        
    def OnData(self, data):
        if self.Time.minute % 5 == 0:
            # Store 5 minute data.
            for symbol in self.data:
                if symbol in data and data[symbol]:
                    price = data[symbol].Value
                    self.data[symbol].append(price)

        if not (self.Time.hour == 16 and self.Time.minute == 0):
            return

        if self.days == 5:
            aggregate_skewness_market_cap = {}
            for symbol, market_cap in self.selected_universe:
                # 5 Minute data is ready.
                if symbol in self.data and len(self.data[symbol]) == self.data[symbol].maxlen:
                    closes_5M = np.array(self.data[symbol])
                    returns_5M = (closes_5M[1:] - closes_5M[:-1]) / closes_5M[:-1]
                    skewness = skew(returns_5M)
                    aggregate_skewness_market_cap[symbol] = (skewness, market_cap)
                            
            if len(aggregate_skewness_market_cap) != 0:
                # Aggregate skewness sorting.
                sorted_by_aggregate_skewness = sorted(aggregate_skewness_market_cap.items(), key = lambda x: x[1][0], reverse = True)
                decile = int(len(sorted_by_aggregate_skewness) / 10)
                long = [x for x in sorted_by_aggregate_skewness[-decile:]]
                short = [x for x in sorted_by_aggregate_skewness[:decile]]
            
                weight = {}
                
                # Market cap weighting.
                total_market_cap_long = sum([x[1][1] for x in long])
                for symbol, skewness_market_cap_data in long:
                    market_cap = skewness_market_cap_data[1]
                    weight[symbol] = market_cap / total_market_cap_long
                
                total_market_cap_short = sum([x[1][1] for x in short])
                for symbol, skewness_market_cap_data in short:
                    market_cap = skewness_market_cap_data[1]
                    weight[symbol] = -market_cap / total_market_cap_short            

                # Trade execution.
                stocks_invested = [x.Key for x in self.Portfolio if x.Value.Invested]
                for symbol in stocks_invested:
                    if symbol not in weight:
                        self.Liquidate(symbol)
            
                for symbol, w in weight.items():
                    if symbol in data and data[symbol]:
                        self.SetHoldings(symbol, w)
        
        self.days += 1
        if self.days > 5:
            self.days = 1      
    
    def Selection(self):
        if self.month == 12:
            self.selection_flag = True
        
        self.month += 1
        if self.month > 12:
            self.month = 1

# Custom fee model
class CustomFeeModel(FeeModel):
    def GetOrderFee(self, parameters):
        fee = parameters.Security.Price * parameters.Order.AbsoluteQuantity * 0.00005
        return OrderFee(CashAmount(fee, "USD"))
