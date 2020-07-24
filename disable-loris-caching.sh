#!/bin/bash
# tweaks the loris configuration and reloads iiif service
set -e
sed --in-place 's/enable_caching = True/enable_caching = False/g' /opt/loris/loris2.conf
sudo systemctl restart iiif-service
