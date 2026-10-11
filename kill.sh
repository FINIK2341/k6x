#!/bin/bash

FILE=./k6

cd /home/test/Genk6/

pid=$(ps aux | grep -v grep | grep $FILE | awk '{print $2}')

if [ -z "$pid" ] ; then
	echo -e "\e[42m \e[0m ===>>> Procces close <<<==="
else
	echo "Find old process $pid. Kill old process"
	kill $pid
	echo -e "\e[44m \e[0m ===>>> Procces closed <<<==="
fi
