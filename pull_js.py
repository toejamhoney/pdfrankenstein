import os
import sys

import db_mgmt

dbgw = db_mgmt.DBGateway(sys.argv[1])
js = dbgw.select("obf_js FROM parsed_pdfs")

for idx, script in enumerate(js):
    print script[0]
    if idx > 3:
        sys.exit(0)
