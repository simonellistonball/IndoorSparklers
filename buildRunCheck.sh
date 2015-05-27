#!/bin/sh

rm -rf out ;

# build
sbt assembly

# run with spark
~/spark-1.3.1/bin/spark-submit --master 'local[*]' --class RandomData --num-executors 8 target/scala-2.10/RandomData-assembly-1.0.jar -n 100000 -p 2 -l 100 -o out

# check first output
head out/part-00000
