# region imports
from AlgorithmImports import *
# endregion

class SwimmingBlackTermite(QCAlgorithm):

    def Initialize(self):
        self.SetStartDate(2020, 1, 1)
        self.SetEndDate(2022, 12, 1)
        self.SetCash(100000)

        bar_size = timedelta(hours=1)
        trailing_stop_pct = 0.1

        self.SetSecurityInitializer(BrokerageModelSecurityInitializer(self.BrokerageModel, FuncSecuritySeeder(self.GetLastKnownPrices)))

        self.symbol_data_by_asset = {}
        tickers = ['SPY', 'QQQ']
        for ticker in tickers:
            equity = self.AddEquity(ticker, Resolution.Minute, dataNormalizationMode=DataNormalizationMode.Raw)
            self.symbol_data_by_asset[equity] = SymbolData(self, equity, bar_size, trailing_stop_pct)

        self.SetWarmUp(timedelta(days=100))

    def OnData(self, data: Slice):
        for symbol_data in self.symbol_data_by_asset.values():
            ids_to_remove = []
            for i, trade in enumerate(symbol_data.trade_collection):
                trade.scan(data)
                if trade.completed:
                    ids_to_remove.append(i)

            # Remove completed trades
            for id_ in ids_to_remove[::-1]:
                del symbol_data.trade_collection[id_]
        
    def OnOrderEvent(self, orderEvent: OrderEvent) -> None:
        security = self.Securities[orderEvent.Symbol.Underlying]
        self.symbol_data_by_asset[security].on_order_event(orderEvent)
    
    def OnEndOfDay(self, symbol):
        for symbol_data in self.symbol_data_by_asset.values():
            self.Plot("Open Trades", "Count", len(symbol_data.trade_collection))

        

class SymbolData:
    def __init__(self, algorithm, security, bar_size, trailing_stop_pct):
        self.algorithm = algorithm
        self.security = security
        self.trailing_stop_pct = trailing_stop_pct
        
        self.CALL_BB_THRESHOLD = 0.1
        self.PUT_BB_THRESHOLD = 0.9
        self.ENTRY_DAYS = [0, 1, 2] # Monday - Wednesday
        self.TRADE_WEIGHT = 0.03 # Percentage of portfolio

        # Create indicators
        self.ema = algorithm.EMA(security.Symbol, 20, Resolution.Daily)
        self.bb = algorithm.BB(security.Symbol, 20, 2, Resolution.Daily)

        # Add consolidator to create n-minute bars
        self.consolidator = TradeBarConsolidator(bar_size)
        self.consolidator.DataConsolidated += self.consolidation_handler
        algorithm.SubscriptionManager.AddConsolidator(security.Symbol, self.consolidator)

        # Create RollingWindow objects for EMA and consolidated price history
        self.trailing_ema = RollingWindow[float](3)
        self.trailing_closes = RollingWindow[float](3)

        # Define a collection to manage the independent trades
        self.trade_collection = []

    def consolidation_handler(self, sender: object, consolidated_bar: TradeBar) -> None:
        # Update trialing history
        self.trailing_ema.Add(self.ema.Current.Value)
        self.trailing_closes.Add(consolidated_bar.Close)
        
        # Check if we have sufficient history
        if self.algorithm.IsWarmingUp or not (self.trailing_ema.IsReady and self.trailing_closes.IsReady):
            return
        
        if self.algorithm.LiveMode:
            self.algorithm.Log(f"Trailing EMAs: {list(self.trailing_ema)}; Trailing closes: {list(self.trailing_closes)}")

        if not self.should_trade:
            return

        if self.bb.UpperBand.Current.Value == self.bb.LowerBand.Current.Value:
            return
        bb_location = (consolidated_bar.Close - self.bb.LowerBand.Current.Value) \
                    / (self.bb.UpperBand.Current.Value - self.bb.LowerBand.Current.Value)

        # Only buy on entry days
        if consolidated_bar.EndTime.weekday() not in self.ENTRY_DAYS:
            return

        # Check for LONG entry condition; EMA signal: 1 close below EMA and then 2 closes above EMA; BB signal: within bottom 10% of BB
        if self.trailing_closes[2] < self.trailing_ema[2] \
            and self.trailing_closes[1] > self.trailing_ema[1] \
            and self.trailing_closes[0] > self.trailing_ema[0] \
            and bb_location <= self.CALL_BB_THRESHOLD:
            # Enter LONG position
            self.trade_collection.append(Trade(self.algorithm, self.security, OrderDirection.Buy, self.trailing_stop_pct, self.TRADE_WEIGHT))
        
        # Check for SHORT entry condition; EMA signal: 1 close above EMA and then 2 closes below EMA; BB signal: within top 10% of BB
        elif self.trailing_closes[2] > self.trailing_ema[2] \
            and self.trailing_closes[1] < self.trailing_ema[1] \
            and self.trailing_closes[0] < self.trailing_ema[0] \
            and bb_location >= self.PUT_BB_THRESHOLD:
            # Enter SHORT position
            self.trade_collection.append(Trade(self.algorithm, self.security, OrderDirection.Sell, self.trailing_stop_pct, self.TRADE_WEIGHT))

        

    @property
    def should_trade(self):
        return len(self.trade_collection) < int(1 / self.TRADE_WEIGHT)

    def on_order_event(self, orderEvent: OrderEvent) -> None:
        for trade in self.trade_collection:
            trade.on_order_event(orderEvent)


class Trade:
    def __init__(self, algorithm, security, order_direction, trailing_stop_pct, trade_weight):
        self.algorithm = algorithm
        self.security = security
        self.order_direction = order_direction
        self.trailing_stop_pct = trailing_stop_pct
        self.trade_weight = trade_weight

        self.EXIT_DAY = 4 # Friday
        self.EXIT_TIME = time(10)

        self.completed = False

        self.contract_symbol = self.get_contract(security)
        if self.contract_symbol:
            self.place_orders(self.contract_symbol, order_direction)
        else:
            self.completed = True

    def get_contract(self, security): 
        # Use OptionChainProvider to select the ATM contract that expires this week
        contract_symbols = self.algorithm.OptionChainProvider.GetOptionContractList(security.Symbol, self.algorithm.Time)
        original_symbols = contract_symbols
        if len(contract_symbols) == 0:
            self.algorithm.Debug(f"{self.algorithm.Time}: GetOptionContractList returned no contracts")
            self.completed = True
            return

        # Filter for exipry dates and Option right
        current_date = self.algorithm.Time.date()
        latest_expiry = datetime.combine(current_date + timedelta(days=5 - self.algorithm.Time.date().weekday()), time(0, 0)) # Saturday at 12 AM
        option_right = OptionRight.Call if self.order_direction == OrderDirection.Buy else OptionRight.Put
        contract_symbols = [symbol for symbol in contract_symbols if symbol.ID.Date < latest_expiry and symbol.ID.Date.weekday() == self.EXIT_DAY and symbol.ID.OptionRight == option_right]
        if len(contract_symbols) == 0:
            self.algorithm.Debug(f"{self.algorithm.Time}: No contracts match the option right and expiry requirements")
            self.completed = True
            return

        # Select ATM contract
        contract_symbol = sorted(contract_symbols, key=lambda contract_symbol: abs(security.Price - contract_symbol.ID.StrikePrice))[0]

        # Subscribe to Option contract
        self.algorithm.AddOptionContract(contract_symbol)

        return contract_symbol

    def place_orders(self, contract_symbol, order_direction):
        self.quantity = 0
        self.high_water_mark = 0
        self.stop_loss_ticket = None

        # Calculate order quantity (x% of portfolio value)
        self.quantity = self.algorithm.CalculateOrderQuantity(contract_symbol, self.trade_weight)
        if self.quantity == 0:
            self.algorithm.Debug(f"{self.algorithm.Time}: Can't afford a single contract")
            self.completed = True
            return

        # Submit entry order
        self.high_water_mark = self.algorithm.MarketOrder(contract_symbol, self.quantity).AverageFillPrice

        # Submit stop loss order
        self.stop_loss_ticket = self.algorithm.StopMarketOrder(contract_symbol, -self.quantity, self.get_stop_loss_price(order_direction)) 

    def get_stop_loss_price(self, order_direction):
        price = self.high_water_mark * (1 - self.trailing_stop_pct)
        
        # Round the stop loss price level so we don't get errors from not following the MinimumPriceVariation  
        minimum_price_variation = self.algorithm.Securities[self.contract_symbol].SymbolProperties.MinimumPriceVariation
        precision = len(str(minimum_price_variation).split('.')[1])
        return round(int(price / minimum_price_variation) * minimum_price_variation, precision)

    def scan(self, data: Slice):
        if self.contract_symbol is None:
            return

        # If it's Friday 10 AM EST and we have an open position, close it
        if data.Time.weekday() == self.EXIT_DAY and data.Time.time() >= self.EXIT_TIME:
            self.stop_loss_ticket.Cancel()
            self.algorithm.MarketOrder(self.contract_symbol, -self.quantity)
            self.completed = True
            self.algorithm.Debug(f"{data.Time}: {self.contract_symbol} position closed because it's the exit day")
            return

        # Update trailing stop loss
        if self.contract_symbol in data.QuoteBars:
            current_price = data.QuoteBars[self.contract_symbol].Bid.Close
            if current_price > self.high_water_mark:
                self.high_water_mark = current_price
                self.stop_loss_ticket.UpdateStopPrice(self.get_stop_loss_price(self.order_direction))

    def on_order_event(self, orderEvent: OrderEvent) -> None:
        # When the stop loss is hit, mark the trade as completed
        if orderEvent.Status == OrderStatus.Filled \
            and self.stop_loss_ticket is not None \
            and orderEvent.OrderId == self.stop_loss_ticket.OrderId:
                self.completed = True
                
