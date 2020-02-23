#!/bin/bash

for ((i=1;i<=$1;i++));
do
	go test --count=1 ./... > "log${i}.txt"
done