#!/bin/bash

clear

export MSYS_NO_PATHCONV=1
#export TOOL=/c/Users/DKing/GitHub/dataops-xt-pipelines
#export PYTHONPATH="$PYTHONPATH:$TOOL:$TOOL/app:$TOOL/app/common"
#export PYTHONPATH="$TOOL"
export PYTHONPATH=/c/Users/DKing/GitHub/dataops-xt-pipelines/app
#echo $PYTHONPATH

py main.py
