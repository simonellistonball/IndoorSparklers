Indoor Sparklers
================

Generates random data of a specific template and form using Spark, outputting to ORC files and creating a table in Hive based on said ORC files.


Everyone knows that indoor fireworks are not real. This program is the beginnings of a utility to generate fake data in the Spark job, mainly for unit testing, and load testing Spark jobs. It's kind of basic for now, but will hopefully grow as I end up doing a lot more Spark work.

* TODO: configurable data creation pattern
* TODO: other output formats from command line config
* TODO: parameter sanity checking
* TODO: auto-guess-magic parallelism settings
