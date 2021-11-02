# encoding=utf-8
import logging
from logging.handlers import TimedRotatingFileHandler
import sys
sys.path.append("..")
# 网格记录
logHandler = TimedRotatingFileHandler("../logs/logfile.log", when="midnight")
logFormatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
logHandler.setFormatter(logFormatter)
logger = logging.getLogger('MyLogger')
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)
