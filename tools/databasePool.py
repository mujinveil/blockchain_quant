# encoding=utf-8
import redis
from tools.Config import RedisHost, RedisPassword, MysqlHost, MysqlPassword
from DBUtils.PooledDB import PooledDB
#from dbutils.pooled_db import PooledDB
import pymysql

# redis连接池(存储策略信息)
pool1 = redis.ConnectionPool(host=RedisHost, port=6379, password=RedisPassword, decode_responses=True, db=15)
r1 = redis.Redis(connection_pool=pool1)

pool2 = redis.ConnectionPool(host=RedisHost, port=6379, password=RedisPassword, decode_responses=True, db=14)
r2 = redis.Redis(connection_pool=pool2)

pool3 = redis.ConnectionPool(host=RedisHost, port=6379, password=RedisPassword, decode_responses=True, db=13)
r3 = redis.Redis(connection_pool=pool3)

pool4 = redis.ConnectionPool(host=RedisHost, port=6379, password=RedisPassword, decode_responses=True, db=11)
r4 = redis.Redis(connection_pool=pool4)

pool5 = redis.ConnectionPool(host=RedisHost, port=6379, password=RedisPassword, decode_responses=True, db=12)
r5 = redis.Redis(connection_pool=pool5)

pool0 = redis.ConnectionPool(host=RedisHost, port=6379, password=RedisPassword, decode_responses=True, db=0)
r0= redis.Redis(connection_pool=pool0)

pool6 = redis.ConnectionPool(host=RedisHost, port=6379, password=RedisPassword, decode_responses=True, db=6)
r6 = redis.Redis(connection_pool=pool6)

# mysql连接池
POOL = PooledDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxconnections=0,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=5,  # 链接池中最多闲置的链接，0和None不限制
    maxshared=3,
    # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
    blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
    maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
    setsession=[],  # 开始会话前执行的命令列表。
    ping=0,  # ping MySQL服务端，检查是否服务可用。
    host=MysqlHost,
    port=3306,
    # user='quantify',   # 量化app正式
    # password="58h59PIef2Jj7ozJ",
    user='admin',
    password=MysqlPassword,
    database="py_orderlist",
    charset='utf8'
)

# mysql网格交易对连接池
POOL_grid = PooledDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxconnections=100,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=5,  # 链接池中最多闲置的链接，0和None不限制
    maxshared=3,
    # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
    blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
    maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
    setsession=[],  # 开始会话前执行的命令列表。
    ping=0,  # ping MySQL服务端，检查是否服务可用。
    host=MysqlHost,
    port=3306,
    # user='quantify',  # 量化app正式
    # password='58h59PIef2Jj7ozJ',
    user='admin',
    password=MysqlPassword,
    # database='t_better_bourse',
    database='t_better_bourse_im',  # IFS对应的表
    charset='utf8'
)
