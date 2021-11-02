#!/bin/bash
nohup /root/anaconda3/bin/python -u future_followstrategy.py>/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u future_marketdepth_strategy.py>/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u grid_future_long.py >/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u grid_future_short.py >/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u martin_future_long.py >/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u martin_future_short.py >/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u dual_thrust.py >/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u rsrs_strategy.py >/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u keltner_strategy.py >/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u macd_atr.py >/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u multi_factor.py >/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u spiderweb_strategy.py >/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u turtle_strategy.py >/dev/null 2>error.log  2>&1 &