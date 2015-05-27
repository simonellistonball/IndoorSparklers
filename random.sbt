name := "RandomData"

version := "1.0"
scalaVersion := "2.10.4"


//libraryDependencies += "org.mortbay.jetty" % "jetty-util" % "6.1.26.hwx" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0.2.2.4.4-16" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1.2.2.4.4-16" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.1.2.2.4.4-16" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.3.1.2.2.4.4-16" % "provided"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"


resolvers += "hdp-repo" at "http://repo.hortonworks.com/content/repositories/releases/"
resolvers += "spring-releases" at "https://repo.spring.io/libs-release"

//resolvers += Resolver.sonatypeRepo("public")
resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
