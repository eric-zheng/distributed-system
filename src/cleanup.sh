#!/bin/bash
ssh -n 128.100.13.235 nohup "lsof -t -i:52735 | xargs kill"
ssh -n 128.100.13.235 nohup "lsof -t -i:52734 | xargs kill"
ssh -n 128.100.13.235 nohup "lsof -t -i:52733 | xargs kill"
ssh -n 128.100.13.235 nohup "lsof -t -i:52732 | xargs kill"
ssh -n 128.100.13.235 nohup "lsof -t -i:52731 | xargs kill"
ssh -n 128.100.13.235 nohup "lsof -t -i:52730 | xargs kill"
./zookeeper/bin/zkServer.sh stop
rm -r ./zookeeper/data
rm -f ~/*.dat
