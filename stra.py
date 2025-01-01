import pandas as pd
import requests
from io import StringIO
from backtesting import Backtest, Strategy
import runpod
import json

# Define Strategies with No Hardcoded Parameters
class Strategy1(Strategy):
    def init(self):
        # Retrieve strategy-specific parameters from self.params
        self.take_profit_ratio = self.params.get('take_profit_ratio')
        self.stop_loss_ratio = self.params.get('stop_loss_ratio')

        # Validate parameters
        if self.take_profit_ratio is None:
            raise ValueError("Strategy1 requires 'take_profit_ratio'.")
        if self.stop_loss_ratio is None:
            raise ValueError("Strategy1 requires 'stop_loss_ratio'.")

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
        self.size = self.params.get('size')

        if self.size is None:
            raise ValueError("Strategy2 requires 'size'.")

    def next(self):
        if self.data.Close[-1] == self.data.Close.max():
            self.buy(size=self.size)
        elif self.position and self.data.Close[-1] != self.data.Close.max():
            self.position.close()

class Strategy3(Strategy):
    def init(self):
        self.take_profit_ratio = self.params.get('take_profit_ratio')
        self.stop_loss_ratio = self.params.get('stop_loss_ratio')

        if self.take_profit_ratio is None:
            raise ValueError("Strategy3 requires 'take_profit_ratio'.")
        if self.stop_loss_ratio is None:
            raise ValueError("Strategy3 requires 'stop_loss_ratio'.")

    def next(self):
        if self.data.Close[-1] == self.data.Close.max():
            self.buy(
                tp=self.data.Close[-1] * (1 + self.take_profit_ratio),
                sl=self.data.Close[-1] * (1 - self.stop_loss_ratio)
            )

class Strategy4(Strategy):
    def init(self):
        self.size = self.params.get('size')

        if self.size is None:
            raise ValueError("Strategy4 requires 'size'.")

    def next(self):
        if self.data.Close[-1] == self.data.Close.max():
            self.buy(size=self.size)
        elif self.position and self.data.Close[-1] < self.data.Close.max():
            self.position.close()

class Strategy5(Strategy):
    def init(self):
        self.take_profit_ratio = self.params.get('take_profit_ratio')
        self.stop_loss_ratio = self.params.get('stop_loss_ratio')

        if self.take_profit_ratio is None:
            raise ValueError("Strategy5 requires 'take_profit_ratio'.")
        if self.stop_loss_ratio is None:
            raise ValueError("Strategy5 requires 'stop_loss_ratio'.")

    def next(self):
        if self.data.Close[-1] == self.data.Close.max():
            self.buy(
                tp=self.data.Close[-1] * (1 + self.take_profit_ratio),
                sl=self.data.Close[-1] * (1 - self.stop_loss_ratio)
            )

# Mapping Strategy Names to Classes
STRATEGY_MAPPING = {
    "Strategy1": Strategy1,
    "Strategy2": Strategy2,
    "Strategy3": Strategy3,
    "Strategy4": Strategy4,
    "Strategy5": Strategy5
}

def download_csv(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Ensure the request was successful
        return pd.read_csv(StringIO(response.text))
    except requests.RequestException as e:
        raise ValueError(f"Failed to download CSV from URL '{url}': {str(e)}")

def preprocess_data(data):
    required_columns = ['predicted_close_price', 'last_actual_close', 't']
    for col in required_columns:
        if col not in data.columns:
            raise ValueError(f"Missing required column '{col}' in CSV data.")

    data['Close'] = data['predicted_close_price']
    data['Open'] = data['last_actual_close']
    data['High'] = data['Close']
    data['Low'] = data['Close']
    data['Date'] = pd.to_datetime(data['t'])
    data.set_index('Date', inplace=True)
    data = data[~data.index.duplicated(keep='first')]
    data = data.sort_index()
    if not data.index.is_monotonic_increasing:
        raise ValueError("DataFrame is not sorted by Date.")
    if data.index.duplicated().any():
        raise ValueError("DataFrame contains duplicate Date entries.")
    return data[['Open', 'High', 'Low', 'Close']]

# Default Metrics (Users must provide desired_metrics)
DEFAULT_METRICS = [
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

def run_backtests(bt_data, strategies, backtest_settings, metrics):
    results = {}
    for strategy_info in strategies:
        strategy_class = strategy_info['class']
        strategy_params = strategy_info.get('params', {})

        # Assign parameters to the strategy class
        strategy_class.params = strategy_params

        # Initialize Backtest with dynamic settings
        try:
            bt = Backtest(
                bt_data,
                strategy_class,
                cash=backtest_settings['cash'],
                commission=backtest_settings['commission'],
                **backtest_settings.get('additional', {})
            )
            run_result = bt.run()
        except Exception as e:
            results[strategy_class.__name__] = {"error": f"Backtest execution failed: {str(e)}"}
            continue

        run_result_dict = dict(run_result)

        # Filter out only desired metrics and ensure values are serializable
        filtered_results = {}
        for metric in metrics:
            if metric in run_result_dict:
                value = run_result_dict[metric]
                if isinstance(value, (int, float)):
                    filtered_results[metric] = float(value)
                else:
                    filtered_results[metric] = str(value)
            else:
                filtered_results[metric] = None  # Metric not available

        results[strategy_class.__name__] = filtered_results
    return results

def handler(job):
    input_payload = job.get('input', {})

    # List of required top-level parameters
    required_top_level_params = ['csv_url', 'cash', 'commission', 'desired_metrics', 'strategies']

    # Check for missing top-level parameters
    missing_params = [param for param in required_top_level_params if input_payload.get(param) is None]
    if missing_params:
        return json.dumps({"error": f"Missing required parameter(s): {', '.join(missing_params)}."})

    # Extract and validate CSV URL
    csv_url = input_payload.get('csv_url')
    try:
        data = download_csv(csv_url)
    except ValueError as e:
        return json.dumps({"error": str(e)})

    # Preprocess data
    try:
        bt_data = preprocess_data(data)
    except ValueError as e:
        return json.dumps({"error": f"Data preprocessing failed: {str(e)}"})

    # Extract Backtest Settings
    backtest_settings = {
        'cash': input_payload.get('cash'),
        'commission': input_payload.get('commission'),
        'additional': input_payload.get('backtest_additional', {})
    }

    # Validate Backtest Settings
    if not isinstance(backtest_settings['cash'], (int, float)):
        return json.dumps({"error": "'cash' must be a number."})
    if not isinstance(backtest_settings['commission'], (int, float)):
        return json.dumps({"error": "'commission' must be a number."})

    # Extract Desired Metrics
    desired_metrics = input_payload.get('desired_metrics')
    if not isinstance(desired_metrics, list) or not all(isinstance(m, str) for m in desired_metrics):
        return json.dumps({"error": "'desired_metrics' must be a list of strings."})

    # Extract Strategies
    strategies_input = input_payload.get('strategies')
    if not isinstance(strategies_input, list):
        return json.dumps({"error": "'strategies' must be a list of strategy configurations."})

    strategies = []
    for idx, strat in enumerate(strategies_input, 1):
        if not isinstance(strat, dict):
            return json.dumps({"error": f"Strategy at index {idx} must be an object with 'class' and 'params'."})
        strat_name = strat.get('class')
        strat_params = strat.get('params')

        if not strat_name:
            return json.dumps({"error": f"Strategy at index {idx} is missing the 'class' field."})
        if not isinstance(strat_params, dict):
            return json.dumps({"error": f"'params' for strategy '{strat_name}' must be a dictionary."})

        strategy_class = STRATEGY_MAPPING.get(strat_name)
        if not strategy_class:
            return json.dumps({"error": f"Strategy class '{strat_name}' is not recognized."})

        strategies.append({
            'class': strategy_class,
            'params': strat_params
        })

    # Run Backtests
    try:
        results = run_backtests(bt_data, strategies, backtest_settings, desired_metrics)
    except Exception as e:
        return json.dumps({"error": f"Backtesting failed: {str(e)}"})

    # Prepare Success Response
    results_dict = {
        "status": "success",
        "message": "",
        "details": results
    }

    return json.dumps(results_dict)

# Start the serverless handler
runpod.serverless.start({"handler": handler})
