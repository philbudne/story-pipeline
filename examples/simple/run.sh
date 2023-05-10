#!/bin/sh

rm -rf /tmp/simple
mkdir /tmp/simple
python -m scripts.configure -f examples/simple/plumbing.json configure
supervisord -c examples/simple/supervisord.conf
