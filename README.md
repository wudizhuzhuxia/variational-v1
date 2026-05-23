# variational-v1

邀请链接：
- Variational: [https://omni.variational.io/?ref=OMNIQUANT](https://omni.variational.io/?ref=OMNIQUANT)（直升 Bronze，获得 12% 积分加成）
- Lighter: [https://app.lighter.xyz/?referral=QUANTGUY](https://app.lighter.xyz/?referral=QUANTGUY)

English version is below.

## 中文

### 概述
`variational-v1` 是一个基于 Chrome 插件转发的运行时工具，用于：
1. 跟踪 Variational 订单生命周期，
2. 在终端展示实时看板，
3. 可选地在 Lighter 自动对冲。

交易对会从 Variational 的 REST/WS 消息中自动识别，不需要手动输入 ticker。

### 核心功能
- 记录 Variational/Lighter 的订单关键信息（成交、价格、方向、价差）。
- Rich 终端看板实时展示双边盘口、价差百分比和最近订单。
- 可选Lighter自动对冲功能（默认开启，可用 `--no-hedge` 关闭）。
- 支持页面重连与交易资产自动切换（切换后自动重置对应历史窗口）。

### 项目结构
- `main.py`：主程序
- `variational/listener.py`：本地接收与监控解析
- `chrome_extension/`：CDP 转发插件

### 环境准备
#### macOS / Linux
```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

#### Windows（PowerShell）
```powershell
py -3 -m venv env
.\env\Scripts\Activate.ps1
pip install -r requirements.txt
```

创建 `.env`并填入Lighter的信息：
```bash
LIGHTER_ACCOUNT_INDEX=...
LIGHTER_API_KEY_INDEX=...
LIGHTER_PRIVATE_KEY=...
```

如需在 Lighter WebSocket 新旧逻辑切换期间临时强制使用旧的应用层 ping/pong 逻辑，可额外设置：
```bash
LIGHTER_WS_SERVER_PINGS=true
```
不设置时默认使用新的兼容模式：客户端依赖 WebSocket protocol ping frame 保活，同时仍兼容旧服务端发出的 `ping` 消息。

### 加载 Chrome 插件
1. 打开 `chrome://extensions`
2. 在右上角开启 `Developer mode`
3. 在左上角点击 `Load unpacked`，选择：
`variational-v1/chrome_extension`

### 运行
```bash
python main.py
```

关闭自动对冲：
```bash
python main.py --no-hedge
```

Python 脚本开始运行后，打开 Variational 的交易页面，
打开 Chrome 插件列表，点击 “Variational CDP Forwarder” -> 点击 `Start`

切换看板语言为英文：
```bash
python main.py --lang en
```

### 输出日志
默认目录：`./log`
- `runtime.log`（程序运行日志）
- `order_metrics.jsonl`
- `trade_records.csv`（当前交易记录快照，dashboard 刷新时按最新状态覆盖写）
- `signal_samples.jsonl`（每秒价差信号样本，用于校准开平仓参数）

说明：终端仅用于显示 dashboard。程序不会落盘原始 REST/WS 消息，只会写运行日志、订单指标日志和交易记录 CSV 快照。

### 开平仓参数校准
程序运行时会持续写入 `log/signal_samples.jsonl`，里面记录每秒的双边盘口、当前跨所价差、扣除平均买卖价差后的 adjusted 价差，以及 5m/30m/1h 中位数。

先用观察模式收集一段时间数据：
```bash
python main.py --no-hedge
```

收集 1-24 小时后，用脚本查看阈值候选：
```bash
python scripts/analyze_signal_samples.py --file log/signal_samples.jsonl --window-hours 6
```

脚本会输出 `aggressive`、`balanced`、`conservative` 三组候选：
- `entry`：建议开仓阈值，来自 adjusted 价差的 p90/p95/p99。
- `exit`：建议平仓阈值，默认用 adjusted 价差中位数 p50。
- 样本越多越可靠；少于几百条时只适合粗看，不适合直接实盘。

---

## English

Referral Links:
- Variational: [https://omni.variational.io/?ref=OMNIQUANT](https://omni.variational.io/?ref=OMNIQUANT) (instant Bronze tier + 12% points bonus)
- Lighter: [https://app.lighter.xyz/?referral=QUANTGUY](https://app.lighter.xyz/?referral=QUANTGUY)

### Overview
`variational-v1` is a Chrome-extension-assisted runtime for:
1. tracking Variational order lifecycle,
2. showing a terminal dashboard,
3. optionally auto-hedging on Lighter.

Ticker is auto-derived from incoming Variational REST/WS messages.

### Core Features
- Tracks key Variational/Lighter order data (fills, prices, direction, spread).
- Rich terminal dashboard for live two-venue quotes, spread percentages, and recent orders.
- Optional auto-hedge (enabled by default, disable with `--no-hedge`).
- Handles page reconnects and automatic asset switching (with related history reset on switch).

### Repository Layout
- `main.py`: main runtime
- `variational/listener.py`: local receiver + monitor parsing
- `chrome_extension/`: CDP forwarder extension

### Setup
#### macOS / Linux
```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

#### Windows (PowerShell)
```powershell
py -3 -m venv env
.\env\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create `.env`:
```bash
LIGHTER_ACCOUNT_INDEX=...
LIGHTER_API_KEY_INDEX=...
LIGHTER_PRIVATE_KEY=...
```

If you need to temporarily force Lighter's legacy application-level ping/pong behavior during the rollout window, you can also set:
```bash
LIGHTER_WS_SERVER_PINGS=true
```
When unset, the runtime uses the forward-compatible path: it keeps the socket alive with WebSocket protocol ping frames and still responds to legacy server `ping` messages.

### Load Chrome Extension
1. Open `chrome://extensions`
2. Enable `Developer mode` (top-right)
3. Click `Load unpacked` (top-left), then choose:
`variational-v1/chrome_extension`

### Run
```bash
python main.py
```

Disable hedge:
```bash
python main.py --no-hedge
```

After the Python script starts, open the Variational trading page,
open the Chrome extensions list, click `Variational CDP Forwarder`, then click `Start`.

Switch dashboard language to Chinese:
```bash
python main.py --lang zh
```

### Output Logs
Default path: `./log`
- `runtime.log` (runtime log messages)
- `order_metrics.jsonl`
- `trade_records.csv` (current trade-record snapshot, overwritten on dashboard refresh with latest state)
- `signal_samples.jsonl` (per-second signal samples for entry/exit calibration)

Note: the terminal is reserved for the dashboard. Raw REST/WS payloads are not persisted; only runtime logs, order-metrics logs, and trade-record CSV snapshots are written.

### Entry/exit parameter calibration
While the program runs, it writes `log/signal_samples.jsonl` with per-second quotes, current cross-venue spreads, adjusted spreads after subtracting the average book spread, and 5m/30m/1h medians.

Collect data in observation mode first:
```bash
python main.py --no-hedge
```

After collecting 1-24 hours of samples, inspect threshold candidates:
```bash
python scripts/analyze_signal_samples.py --file log/signal_samples.jsonl --window-hours 6
```

The script prints `aggressive`, `balanced`, and `conservative` candidates:
- `entry`: suggested entry threshold from adjusted-spread p90/p95/p99.
- `exit`: suggested exit threshold, using adjusted-spread p50 by default.
- More samples make the output more reliable; with only a few hundred samples, treat it as rough guidance only.
