#!/bin/bash
nohup /root/anaconda3/bin/python -u order_copy.py>/dev/null 2>error.log  2>&1 &