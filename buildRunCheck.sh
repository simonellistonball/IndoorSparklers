#!/bin/sh

rm -rf out ;

# build
sbt assembly &&
# run with spark
~/spark-1.3.1/bin/spark-submit --master 'local[*]' --class RandomData --num-executors 1 target/scala-2.10/RandomData-assembly-1.0.jar -n 100 -p 1 -l 100 -o out &&
# check first output
head out/part-00000
