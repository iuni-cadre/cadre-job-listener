#!/usr/bin/env bash
pushd /home/ubuntu/cadre-job-listener
source venv/bin/activate
exec python run_cadre_tool_listener.py
