import sys
import json, os

with open('logs/report.json', 'r') as fh:
    for line in fh.readlines():
        json_line = json.loads(line)
        result_line = json_line["message"]
        if "pae" in result_line and result_line["pae"] and float(result_line["pae"]) >= 0.9:
            sys.stdout.write(str(result_line["article-id"]))
            sys.stdout.write(" ")
