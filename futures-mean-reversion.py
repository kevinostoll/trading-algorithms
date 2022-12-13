
# QuantBook Analysis Tool 
# For more information see [https://www.quantconnect.com/docs/v2/our-platform/research/getting-started]
#qb = QuantBook()
#spy = qb.AddEquity("SPY")
#history = qb.History(qb.Securities.Keys, 360, Resolution.Daily)

# Indicator Analysis
#bbdf = qb.Indicator(BollingerBands(30, 2), spy.Symbol, 360, Resolution.Daily)
#bbdf.drop('standarddeviation', axis=1).plot()



# region imports
from AlgorithmImports import *
# endregion

class SwimmingBlackTermite(QCAlgorithm):

    def Initialize(self):
        self.SetStartDate(2019, 1, 1)
        self.SetEndDate(2022, 12, 1)
        self.SetCash(100000)
        
        self.max_loss = 3000 # Per trade position (used to place the stop loss)
        self.stop_loss_std_multiple = 1 # std from current price
        self.profit_target_multiple = 0.5 # % of max_loss (0.5 == 50% the as far as the stop loss)
        self.max_holding_time = timedelta(days=8)
        self.long_bb_threshold = 0.2
        self.short_bb_threshold = 0.8

        self.symbol_data_by_future = {}
        tickers = [
            #Futures.Indices.SP500EMini,
            Futures.Indices.Dow30EMini#,
            #Futures.Indices.NASDAQ100EMini,
            #Futures.Indices.Russell2000EMini
        ]
        for ticker in tickers:
            future = self.AddFuture(ticker, Resolution.Minute, 
                                    dataMappingMode=DataMappingMode.OpenInterest, 
                                    dataNormalizationMode=DataNormalizationMode.BackwardsRatio, 
                                    contractDepthOffset=0)
            future.SetFilter(0, 180)
            self.symbol_data_by_future[future] = SymbolData(self, future, self.max_loss, self.profit_target_multiple, 
                                                            self.max_holding_time, self.stop_loss_std_multiple, 
                                                            self.long_bb_threshold, self.short_bb_threshold)

    def OnData(self, data: Slice):
        # Get current positions
        current_holds = [self.Securities[security_holding.Symbol.Canonical] for security_holding in self.Portfolio.Values if security_holding.Invested]

        # Find new entries
        z_score_by_symbol_data = {future: symbol_data.z_score for future, symbol_data in self.symbol_data_by_future.items() if symbol_data.allowed_entry}
        new_longs = sorted([(future, z_score) for future, z_score in z_score_by_symbol_data.items() if z_score <= self.long_bb_threshold and future not in current_holds], key=lambda kvp: kvp[1], reverse=True)
        new_shorts = sorted([(future, z_score) for future, z_score in z_score_by_symbol_data.items() if z_score >= self.short_bb_threshold and future not in current_holds], key=lambda kvp: kvp[1], reverse=False)        

        # Enter new positions
        for future, _ in new_longs + new_shorts:
            symbol_data = self.symbol_data_by_future[future]
            if self.Portfolio.MarginRemaining > symbol_data.required_buying_power:
                symbol_data.trade()

        for symbol_data in self.symbol_data_by_future.values():
            symbol_data.scan(data)

    def OnOrderEvent(self, orderEvent: OrderEvent) -> None:
        future = self.Securities[orderEvent.Symbol.Canonical]
        self.symbol_data_by_future[future].on_order_event(orderEvent)

        

class SymbolData:
    def __init__(self, algorithm, future, max_loss, profit_target_multiple, max_holding_time, stop_loss_std_multiple, long_bb_threshold, short_bb_threshold):
        self.algorithm = algorithm
        self.future = future
        self.max_loss = max_loss
        self.profit_target_multiple = profit_target_multiple
        self.max_holding_time = max_holding_time
        self.stop_loss_std_multiple = stop_loss_std_multiple
        self.long_bb_threshold = long_bb_threshold
        self.short_bb_threshold = short_bb_threshold

        # Create indicators
        algorithm.EnableAutomaticIndicatorWarmUp = True
        self.bb = algorithm.BB(future.Symbol, 24, 2, resolution=Resolution.Hour)
        self.std = algorithm.STD(future.Symbol, 22, Resolution.Daily)
        self.price = algorithm.Identity(future.Symbol)

        self.profit_target_ticket = None
        self.stop_loss_ticket = None

        self.last_trade_entry_time = None
        self.rollover = None
        self.stop_loss_hit_time = None
        self.stop_loss_hit_delay = timedelta(days=7)

    
    def trade(self):
        # Set stop loss n-std away (the max loss controls how many contracts we buy)
        z_score = self.z_score 

        def round_price(price):
            # Round the tp/sl price level so we don't get errors from not following the MinimumPriceVariation
            minimum_price_variation = self.future.SymbolProperties.MinimumPriceVariation
            precision = len(str(minimum_price_variation).split('.')[1])
            return round(int(price / minimum_price_variation) * minimum_price_variation, precision)

        pct_portfolio_available = self.algorithm.Portfolio.MarginRemaining / (self.algorithm.Portfolio.TotalMarginUsed + self.algorithm.Portfolio.MarginRemaining)
        if z_score <= self.long_bb_threshold:
            quantity = int(self.max_loss / (self.stop_loss_std_multiple*self.std.Current.Value * self.future.SymbolProperties.ContractMultiplier))
            quantity = min(quantity, self.algorithm.CalculateOrderQuantity(self.future.Mapped, pct_portfolio_available))
            if quantity == 0:
                return
            
            # Entry order
            entry_price = self.algorithm.MarketOrder(self.future.Mapped, quantity).AverageFillPrice

            # Stop loss order
            stop_loss_price_level = round_price(entry_price - self.stop_loss_std_multiple*self.std.Current.Value)
            self.stop_loss_ticket = self.algorithm.StopMarketOrder(self.future.Mapped, -quantity, stop_loss_price_level) 

            # Profit target order
            profit_target_price_level = round_price(entry_price + self.stop_loss_std_multiple*self.std.Current.Value*self.profit_target_multiple)
            self.profit_target_ticket = self.algorithm.LimitOrder(self.future.Mapped, -quantity, profit_target_price_level)

            # Record entry time
            self.last_trade_entry_time = self.algorithm.Time
        elif z_score >= self.short_bb_threshold:
            quantity = -int(self.max_loss / (self.stop_loss_std_multiple*self.std.Current.Value * self.future.SymbolProperties.ContractMultiplier))
            quantity = max(quantity, self.algorithm.CalculateOrderQuantity(self.future.Mapped, -pct_portfolio_available))
            if quantity == 0:
                return

            # Entry order
            entry_price = self.algorithm.MarketOrder(self.future.Mapped, quantity).AverageFillPrice

            # Stop loss order
            stop_loss_price_level = round_price(entry_price + self.stop_loss_std_multiple*self.std.Current.Value)
            self.stop_loss_ticket = self.algorithm.StopMarketOrder(self.future.Mapped, -quantity, stop_loss_price_level) 

            # Profit target order
            profit_target_price_level = round_price(entry_price - self.stop_loss_std_multiple*self.std.Current.Value*self.profit_target_multiple)
            self.profit_target_ticket = self.algorithm.LimitOrder(self.future.Mapped, -quantity, profit_target_price_level)

            # Record entry time
            self.last_trade_entry_time = self.algorithm.Time
        elif self.profit_target_ticket is not None or self.stop_loss_ticket is not None:
            self.algorithm.Debug(f"{self.algorithm.Time} - Closing {self.future.Symbol} because not in the BB bounds")
            self.close()

    @property
    def required_buying_power(self):
        margin_required_per_contract = self.algorithm.Securities[self.future.Mapped].BuyingPowerModel.InitialIntradayMarginRequirement
        max_quantity = int(self.max_loss / (self.stop_loss_std_multiple * self.std.Current.Value * self.future.SymbolProperties.ContractMultiplier))
        margin_required_for_max_quantity = max_quantity * margin_required_per_contract
        return margin_required_for_max_quantity

    @property
    def z_score(self):
        # Calculate BB factor scores
        bb_width = (self.bb.UpperBand.Current.Value - self.bb.LowerBand.Current.Value)
        return (self.price.Current.Value - self.bb.LowerBand.Current.Value) / bb_width

    @property
    def allowed_entry(self):
        if self.stop_loss_hit_time is None:
            return True
        elif self.algorithm.Time > self.stop_loss_hit_time + self.stop_loss_hit_delay:
            self.stop_loss_hit_time = None
            return True
        return False
    
    def scan(self, data: Slice):
        # Catch rollover signals
        for symbol_changed_event in  data.SymbolChangedEvents.Values:
            if symbol_changed_event.Symbol == self.future.Symbol:
                quantity = self.algorithm.Portfolio[symbol_changed_event.OldSymbol].Quantity
                if quantity != 0:
                    self.rollover = {'old_symbol' : symbol_changed_event.OldSymbol, 'new_symbol': symbol_changed_event.NewSymbol, 'quantity': quantity}
                    self.algorithm.Debug(f"{self.algorithm.Time} - Contract rollover DETECTED {symbol_changed_event.OldSymbol} => {symbol_changed_event.NewSymbol}")
        
        # Rollover contracts when their data is available in the Slice
        if self.rollover is not None and self.rollover['old_symbol'] in data.Bars and self.rollover['new_symbol'] in data.Bars:
            # Close current contract
            self.close()

            # Buy new contract and set tp/sl orders
            self.trade()
            self.algorithm.Debug(f"{self.algorithm.Time} - Contract rollover TRADED {self.rollover['old_symbol']} => {self.rollover['new_symbol']}")
            self.rollover = None

        # Only hold positions up to a max of n days
        if self.last_trade_entry_time is not None and data.Time > self.last_trade_entry_time + self.max_holding_time:
            self.algorithm.Debug(f"{data.Time} - Closing {self.future.Symbol} because held for more than 7 days")
            self.close()

    def close(self):
        # Close position, clean up tickets
        if self.stop_loss_ticket is not None:
            symbol = self.stop_loss_ticket.Symbol
        elif self.profit_target_ticket is not None:
            symbol = self.profit_target_ticket.Symbol
        else:
            self.algorithm.Debug(f"{self.algorithm.Time} - Close doesn't have a symbol")
            return
        self.algorithm.Liquidate(symbol)
        self.reset()

    def on_order_event(self, orderEvent: OrderEvent) -> None:
        # When stop loss or profit target is hit, cancel the other order
        if orderEvent.Status != OrderStatus.Filled:
            return
        if self.stop_loss_ticket is not None and orderEvent.OrderId == self.stop_loss_ticket.OrderId:
            self.profit_target_ticket.Cancel("Stop loss hit")
            self.stop_loss_hit_time = self.algorithm.Time
        elif self.profit_target_ticket is not None and orderEvent.OrderId == self.profit_target_ticket.OrderId:
            self.stop_loss_ticket.Cancel("Profit target hit")
        else:
            return # The entry order was filled
        self.reset()

    def reset(self):
        self.profit_target_ticket = None
        self.stop_loss_ticket = None
        self.last_trade_entry_time = None
