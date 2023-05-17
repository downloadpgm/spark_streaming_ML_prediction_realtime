# kafka sbt

To run, prepare a Spark environment having YARN/Standalone cluster manager

$ docker stack deploy -c docker-composer.yml kfk

1) download sbt
```shell
$ wget https://github.com/sbt/sbt/releases/download/v1.3.8/sbt-1.3.8.tgz
$ tar zxvf sbt-1.3.8.tgz
$ mv sbt /usr/local
$ export PATH=$PATH:/usr/local/sbt/bin
```

2) run sbt to prepare enviroment
```shell
$ sbt
```

3) create directory for build
```shell
$ mkdir app
$ cd app
$ # copy kafka-consumer.sbt and KafkaStream.scala
```

4) build and create jar
```shell
$ sbt package
$ cd ~
```

5) create pipelinemodel and save to HDFS (follow instructions on fit_lr_model_and_save.script)

6) run the package
```shell
$ spark-submit --packages mysql:mysql-connector-java:8.0.32,io.delta:delta-core_2.12:0.7.0 --class FileStreamML.FileStreamML app/target/scala-2.12/file-consumer_2.12-1.0.0.jar
```