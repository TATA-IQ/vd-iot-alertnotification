#!/bin/bash
redis-server --daemonize yes &
python cache.py &
python app.py
