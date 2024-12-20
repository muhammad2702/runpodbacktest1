import pandas as pd
import requests
from io import StringIO
from backtesting import Backtest, Strategy
import runpod

# Define Strategies
class Strategy1(Strategy):
    take_profit_ratio = 0.05
    stop_loss_ratio = 0.025
    def init(self):
        pass
    def next(self):
        if self.data.Close[-1] == self.data.Close.max():
            self.buy(
                tp=self.data.Close[-1] * (1 + self.take_profit_ratio),
                sl=self.data.Close[-1] * (1 - self.stop_loss_ratio)
            )
        elif self.position:
            self.position.close()

class Strategy2(Strategy):
    def init(self):
        pass
    def next(self):
        if self.data.Close[-1] == self.data.Close.max():
            self.buy(size=1)
        elif self.position and self.data.Close[-1] != self.data.Close.max():
            self.position.close()

class Strategy3(Strategy):
    take_profit_ratio = 0.1
    stop_loss_ratio = 0.05
    def init(self):
        pass
    def next(self):
        if self.data.Close[-1] == self.data.Close.max():
            self.buy(
                tp=self.data.Close[-1] * (1 + self.take_profit_ratio),
                sl=self.data.Close[-1] * (1 - self.stop_loss_ratio)
            )

class Strategy4(Strategy):
    def init(self):
        pass
    def next(self):
        if self.data.Close[-1] == self.data.Close.max():
            self.buy(size=1)
        elif self.position and self.data.Close[-1] < self.data.Close.max():
            self.position.close()

class Strategy5(Strategy):
    take_profit_ratio = 0.07
    stop_loss_ratio = 0.03
    def init(self):
        pass
    def next(self):
        if self.data.Close[-1] == self.data.Close.max():
            self.buy(
                tp=self.data.Close[-1] * (1 + self.take_profit_ratio),
                sl=self.data.Close[-1] * (1 - self.stop_loss_ratio)
            )

def download_csv(url):
    response = requests.get(url)
    response.raise_for_status()  # Ensure the request was successful
    return pd.read_csv(StringIO(response.text))

def preprocess_data(data):
    data['Close'] = data['predicted_close_price']
    data['Open'] = data['last_actual_close']
    data['High'] = data['Close']
    data['Low'] = data['Close']
    data['Date'] = pd.to_datetime(data['t'])
    data.set_index('Date', inplace=True)
    data = data[~data.index.duplicated(keep='first')]
    data = data.sort_index()
    assert data.index.is_monotonic_increasing, "DataFrame is not sorted by Date."
    assert not data.index.duplicated().any(), "DataFrame contains duplicate Date entries."
    return data[['Open', 'High', 'Low', 'Close']]

desired_metrics = [
    'Equity Final [$]',
    'Equity Peak [$]',
    'Return [%]',
    'Buy & Hold Return [%]',
    'Return (Ann.) [%]',
    'Volatility (Ann.) [%]',
    'Sharpe Ratio',
    'Sortino Ratio',
    'Calmar Ratio',
    'Max. Drawdown [%]',
    'Avg. Trade [%]',
    'Win Rate [%]'
]

def run_backtests(bt_data):
    strategies = [Strategy1, Strategy2, Strategy3, Strategy4, Strategy5]
    results = {}
    for i, strategy in enumerate(strategies, 1):
        bt = Backtest(bt_data, strategy, cash=10_000, commission=0.002)
        run_result = bt.run()
        run_result_dict = dict(run_result)
        # Filter out only desired metrics and ensure values are serializable
        filtered_results = {}
        for k in desired_metrics:
            if k in run_result_dict:
                val = run_result_dict[k]
                # Ensure numeric values are floats, and fallback to string if needed
                if isinstance(val, (int, float)):
                    filtered_results[k] = float(val)
                else:
                    filtered_results[k] = str(val)
        results[f"Strategy {i}"] = filtered_results
    return results

def handler(job):
    input_payload = job['input']
    csv_url = input_payload.get('csv_url')

    if not csv_url:
        return {"error": "No CSV URL provided in input payload."}
    data = download_csv(csv_url)
    bt_data = preprocess_data(data)
    results = run_backtests(bt_data)
    return {"results": results}

runpod.serverless.start({"handler": handler})
