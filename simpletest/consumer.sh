#!/bin/bash
cur=$(date +%Y%m%d-%T)
echo $cur
for i in {1..10}
do
   echo $i
   python consumer.py > consumer/$i.log &
done
