import os
import sys
import sqlite3

import cfg

cfg = cfg.Config()
db_dir = cfg.setting('database', 'path')
db_name = cfg.setting('database', 'db')
db_path = os.path.join(db_dir, db_name)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

select_cmd = "select tree FROM parsed_pdfs"

cur.execute(
