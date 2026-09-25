# 数据库维护测试使用的真实 SQLite 写入进程，提交数据后等待父进程终止。

import sqlite3
import sys


connection = sqlite3.connect(sys.argv[1])
connection.execute("PRAGMA journal_mode=WAL")
connection.execute("PRAGMA wal_autocheckpoint=0")
connection.execute("CREATE TABLE evidence (value TEXT)")
connection.execute("INSERT INTO evidence VALUES ('committed')")
connection.commit()
print("committed", flush=True)
sys.stdin.read()
connection.close()
