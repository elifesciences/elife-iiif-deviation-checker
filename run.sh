#!/bin/bash
set -e
echo > debug.json > error.json > report.json
lein run
echo "wrote debug.json, error.json and report.json"
