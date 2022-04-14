#!/bin/bash
for ((i=1; i<= 50; i=i+1))
do
    echo test_num_$i;
    go test -run 2D;
done