from backtesting import Backtest, Strategy
import pandas as pd
import runpod
import pandas as pd
import requests
from backtesting import Backtest, Strategy
from io import StringIO

# Define your strategies here
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

# Strategy 2: Buy for max gains, no stop loss, sell if prediction changes
class Strategy2(Strategy):
    def init(self):
        pass

    def next(self):
        if self.data.Close[-1] == self.data.Close.max():
            self.buy(size=1)
        elif self.position and self.data.Close[-1] != self.data.Close.max():
            self.position.close()

# Strategy 3: Test returns in different market environments
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

# Strategy 4: Buy for max gains, hold for upward prediction, sell if downward prediction
class Strategy4(Strategy):
    def init(self):
        pass

    def next(self):
        if self.data.Close[-1] == self.data.Close.max():
            self.buy(size=1)
        elif self.position and self.data.Close[-1] < self.data.Close.max():
            self.position.close()

# Strategy 5: Optimize stop loss and take profit in different market environments
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
    data['High'] = data['Close']  # Placeholder
    data['Low'] = data['Close']   # Placeholder
    data['Date'] = pd.to_datetime(data['t'])
    data.set_index('Date', inplace=True)
    data = data[~data.index.duplicated(keep='first')]
    data = data.sort_index()
    assert data.index.is_monotonic_increasing, "DataFrame is not sorted by Date."
    assert not data.index.duplicated().any(), "DataFrame contains duplicate Date entries."
    return data[['Open', 'High', 'Low', 'Close']]

def run_backtests(bt_data):
    strategies = [Strategy1]  # Add other strategies if desired
    results = {}
    for i, strategy in enumerate(strategies, 1):
        bt = Backtest(bt_data, strategy, cash=10_000, commission=0.002)
        run_result = bt.run()

        # Convert run_result to a serializable dict, excluding non-serializable fields.
        # run_result is a pandas Series-like object, we can convert it to dict directly.
        result_dict = dict(run_result)
        # Remove the '_trades' key to avoid serialization issues
        if '_trades' in result_dict:
            del result_dict['_trades']

        # Ensure all keys are JSON-serializable by converting to str where needed
        # (Most values should already be serializable floats, ints, or strings.)
        for key, value in result_dict.items():
            # Convert timestamps/durations to string if present
            if pd.api.types.is_datetime64_any_dtype(type(value)):
                result_dict[key] = str(value)
            elif isinstance(value, pd.Timedelta):
                result_dict[key] = str(value)

        results[f"Strategy {i}"] = result_dict

    return results

def handler(job):
    input_payload = job['input']
    csv_url = input_payload.get('csv_url')
    if not csv_url:
        return {"error": "No CSV URL provided in input payload."}

    try:
        data = download_csv(csv_url)
        print(data)
        bt_data = preprocess_data(data)
        
        results = run_backtests(bt_data)
        return {"results": results}
    except Exception as e:
        return {"error": str(e)}

runpod.serverless.start({"handler": handler})
