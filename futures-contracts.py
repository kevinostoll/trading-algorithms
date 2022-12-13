# region imports
from AlgorithmImports import *
# endregion

class SwimmingBlackTermite(QCAlgorithm):

    def Initialize(self):
        self.SetStartDate(2020, 1, 1)
        #self.SetEndDate(2022, 12, 1)
        self.SetCash(100000)

        bar_size = timedelta(minutes=60)
        trailing_stop_pct = 0.05

        future = self.AddFuture(Futures.Indices.Dow30EMini, Resolution.Minute, 
                                dataMappingMode=DataMappingMode.OpenInterest, 
                                dataNormalizationMode=DataNormalizationMode.BackwardsRatio, 
                                contractDepthOffset=0)
        future.SetFilter(0, 180)
        self.symbol_data_by_future = {}
        self.symbol_data_by_future[future] = SymbolData(self, future, bar_size, trailing_stop_pct)

    def OnData(self, data: Slice):    
        for symbol_data in self.symbol_data_by_future.values():
            ids_to_remove = []
            for i, trade in enumerate(symbol_data.trade_collection):
                trade.scan(data)
                if trade.completed:
                    ids_to_remove.append(i)

            # Remove completed trades
            for id_ in ids_to_remove[::-1]:
                del symbol_data.trade_collection[id_]
        
    def OnOrderEvent(self, orderEvent: OrderEvent) -> None:
        future = self.Securities[orderEvent.Symbol.Canonical]
        self.symbol_data_by_future[future].on_order_event(orderEvent)
    
    def OnEndOfDay(self, symbol):
        for symbol_data in self.symbol_data_by_future.values():
            self.Plot("Open Trades", "Count", len(symbol_data.trade_collection))

        

class SymbolData:
    def __init__(self, algorithm, future, bar_size, trailing_stop_pct):
        self.algorithm = algorithm
        self.future = future
        self.trailing_stop_pct = trailing_stop_pct

        # Create indicators
        self.ema = algorithm.EMA(future.Symbol, 20, Resolution.Daily)

        # Add consolidator to create n-minute bars
        self.consolidator = TradeBarConsolidator(bar_size)
        self.consolidator.DataConsolidated += self.consolidation_handler
        algorithm.SubscriptionManager.AddConsolidator(future.Symbol, self.consolidator)

        # Create RollingWindow objects for EMA and consolidated price history
        self.trailing_ema = RollingWindow[float](3)
        self.trailing_closes = RollingWindow[float](3)

        # Warm up RollingWindow objects
        self.is_warming_up = True
        daily_trade_bars = algorithm.History[TradeBar](future.Symbol, self.ema.WarmUpPeriod + self.trailing_ema.Size + 100, Resolution.Daily) # 100 extra days so EMA warms up consistently (see https://www.quantconnect.com/docs/v2/writing-algorithms/indicators/supported-indicators/exponential-moving-average#01-Introduction)
        minute_trade_bars = algorithm.History[TradeBar](future.Symbol, 150, Resolution.Minute)
        for daily_trade_bar in daily_trade_bars:
            for minute_trade_bar in minute_trade_bars:
                if daily_trade_bar.Time < minute_trade_bar.EndTime < daily_trade_bar.EndTime:
                    self.consolidator.Update(minute_trade_bar)
            self.ema.Update(daily_trade_bar.EndTime, daily_trade_bar.Close)
        #  Iterate through the minute-bars again just in case some minute bars are from a day that's passed the last bar in `daily_trade_bars`
        for minute_trade_bar in minute_trade_bars:
            if daily_trade_bar.EndTime < minute_trade_bar.EndTime:
                self.consolidator.Update(minute_trade_bar)
        self.is_warming_up = False

        # Define a collection to manage the independent trades
        self.trade_collection = []

    def consolidation_handler(self, sender: object, consolidated_bar: TradeBar) -> None:
        # Update trialing history
        self.trailing_ema.Add(self.ema.Current.Value)
        self.trailing_closes.Add(consolidated_bar.Close)
        
        # Check if we have sufficient history
        if self.is_warming_up or not (self.trailing_ema.IsReady and self.trailing_closes.IsReady):
            return
        
        if self.algorithm.LiveMode:
            self.algorithm.Log(f"Trailing EMAs: {list(self.trailing_ema)}; Trailing closes: {list(self.trailing_closes)}")

        if not self.should_trade:
            return

        # Check for LONG entry condition (1 close below EMA and then 2 closes above EMA)
        if self.trailing_closes[2] < self.trailing_ema[2] \
            and self.trailing_closes[1] > self.trailing_ema[1] \
            and self.trailing_closes[0] > self.trailing_ema[0]:
            # Enter LONG position
            self.trade_collection.append(Trade(self.algorithm, self.future, OrderDirection.Buy, self.trailing_stop_pct))
        
        # Check for SHORT entry condition (1 close above EMA and then 2 closes below EMA)
        elif self.trailing_closes[2] > self.trailing_ema[2] \
            and self.trailing_closes[1] < self.trailing_ema[1] \
            and self.trailing_closes[0] < self.trailing_ema[0]:
            # Enter SHORT position
            self.trade_collection.append(Trade(self.algorithm, self.future, OrderDirection.Sell, self.trailing_stop_pct))
        

    @property
    def should_trade(self):
        return len(self.trade_collection) < int(1 / self.trailing_stop_pct)

    def on_order_event(self, orderEvent: OrderEvent) -> None:
        for trade in self.trade_collection:
            trade.on_order_event(orderEvent)


class Trade:
    def __init__(self, algorithm, future, order_direction, trailing_stop_pct):
        self.algorithm = algorithm
        self.future = future
        self.order_direction = order_direction
        self.trailing_stop_pct = trailing_stop_pct

        self.completed = False
        self.rollover = None

        self.place_orders(future.Mapped, order_direction)

    def place_orders(self, contract_symbol, order_direction):
        self.contract_symbol = contract_symbol
        self.quantity = 0
        self.high_water_mark = 0
        self.stop_loss_ticket = None

        # Calculate order quantity -- 0, 1, or -1 contract
        pct_portfolio_available = self.algorithm.Portfolio.MarginRemaining / (self.algorithm.Portfolio.TotalMarginUsed + self.algorithm.Portfolio.MarginRemaining)
        if order_direction == OrderDirection.Buy:
            self.quantity = min(1, self.algorithm.CalculateOrderQuantity(contract_symbol, pct_portfolio_available))
        elif order_direction == OrderDirection.Sell:
            self.quantity = max(-1, self.algorithm.CalculateOrderQuantity(contract_symbol, -pct_portfolio_available))
        
        if self.quantity == 0:
            self.completed = True
            return

        # Submit entry order
        self.high_water_mark = self.algorithm.MarketOrder(contract_symbol, self.quantity).AverageFillPrice

        # Submit stop loss order
        self.stop_loss_ticket = self.algorithm.StopMarketOrder(contract_symbol, -self.quantity, self.get_stop_loss_price(order_direction)) 

    def get_stop_loss_price(self, order_direction):
        def round_price(price):
            # Round the stop loss price level so we don't get errors from not following the MinimumPriceVariation
            minimum_price_variation = self.future.SymbolProperties.MinimumPriceVariation
            precision = len(str(minimum_price_variation).split('.')[1])
            return round(int(price / minimum_price_variation) * minimum_price_variation, precision)
        
        # Longs
        if order_direction == OrderDirection.Buy:
            return round_price(self.high_water_mark * (1 - self.trailing_stop_pct / self.future.SymbolProperties.ContractMultiplier))
        
        # Shorts
        return round_price(self.high_water_mark * (1 + self.trailing_stop_pct / self.future.SymbolProperties.ContractMultiplier))


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
            # Close trade on current contract
            self.algorithm.MarketOrder(self.contract_symbol, -self.quantity)
            self.stop_loss_ticket.Cancel()

            # Buy new contract and set stop loss order
            self.place_orders(self.rollover['new_symbol'], self.order_direction)
            self.algorithm.Debug(f"{self.algorithm.Time} - Contract rollover TRADED {self.rollover['old_symbol']} => {self.rollover['new_symbol']}")
            self.rollover = None

        # Update trailing stop loss
        if self.contract_symbol in data.QuoteBars:
            # Longs
            if self.order_direction == OrderDirection.Buy: 
                current_price = data.QuoteBars[self.contract_symbol].Bid.Close
                if current_price > self.high_water_mark:
                    self.high_water_mark = current_price
                    self.stop_loss_ticket.UpdateStopPrice(self.get_stop_loss_price(self.order_direction))
            # Shorts
            elif self.order_direction == OrderDirection.Sell:
                current_price = data.QuoteBars[self.contract_symbol].Ask.Close
                if current_price < self.high_water_mark:
                    self.high_water_mark = current_price
                    self.stop_loss_ticket.UpdateStopPrice(self.get_stop_loss_price(self.order_direction))

    def on_order_event(self, orderEvent: OrderEvent) -> None:
        # When the stop loss is hit, mark the trade as completed
        if orderEvent.Status == OrderStatus.Filled \
            and self.stop_loss_ticket is not None \
            and orderEvent.OrderId == self.stop_loss_ticket.OrderId:
                self.completed = True
                
