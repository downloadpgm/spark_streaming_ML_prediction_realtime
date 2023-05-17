name := "file-consumer"

version := "1.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.3" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.0.3" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.0.3" % "provided",
  "mysql" % "mysql-connector-java" % "8.0.32",
  "io.delta" %% "delta-core" % "0.7.0"
)