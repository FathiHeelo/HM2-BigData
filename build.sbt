ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "hm2-bigdata",
    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-sql"             % "4.2.0",
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.7.0",
      // same version the connector resolves ([5.1.1, 5.1.99))
      "org.mongodb"        % "mongodb-driver-sync"   % "5.1.4"
    ),
    // Spark needs a forked JVM with these module options on JDK 17+
    fork := true,
    run / connectInput := true,
    run / outputStrategy := Some(StdoutOutput),
    javaOptions ++= Seq(
      "-XX:+IgnoreUnrecognizedVMOptions",
      "--add-modules=jdk.incubator.vector",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "-Djdk.reflect.useDirectMethodHandle=false",
      "-Dio.netty.tryReflectionSetAccessible=true"
    )
  )
