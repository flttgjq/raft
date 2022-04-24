#!/bin/bash
for ((i=1; i<= 500; i=i+1))
do
    echo test_num_$i;
    go test -run Figure8Unreliable2C;
done