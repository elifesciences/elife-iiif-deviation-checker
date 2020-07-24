#!/bin/bash
set -e
lein run
echo "wrote debug.json, error.json and report.json"
