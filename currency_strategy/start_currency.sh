#!/bin/bash
nohup /root/anaconda3/bin/python -u balance_strategy.py>/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u Cover_strategy.py>/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u crash_callback.py>/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u follow_strategy.py>/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u grid_strategy.py>/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u kama_strategy.py>/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u marketdepth_strategy.py>/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u trace_strategy.py>/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u momentum_strategy.py>/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u machine_learning.py>/dev/null 2>error.log  2>&1 &
nohup /root/anaconda3/bin/python -u pricing_strategy.py>/dev/null 2>error.log  2>&1 &