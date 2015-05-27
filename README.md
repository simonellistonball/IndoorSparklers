Generates random data of a specific template and form.

To build, run and check:

    rm -rf out ; sbt assembly && ~/spark-1.3.1/bin/spark-submit --master 'local[*]' --class RandomData --num-executors 8 target/scala-2.10/RandomData-assembly-1.0.jar -n 100000 -p 2 -l 100 -o out ; head out/part-00000
