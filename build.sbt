
val scalaVer = "2.13.1"
val crossScalaVer = Seq(scalaVer)

name         := "tqa-db"
description  := "PostgreSQL-backed TQA URI resolvers"
organization := "eu.cdevreeze.tqa"
version      := "0.1.0-SNAPSHOT"

scalaVersion       := scalaVer
crossScalaVersions := crossScalaVer

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings", "-Xlint", "-target:jvm-1.8")

Test / publishArtifact := false
publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  }

pomExtra := pomData
pomIncludeRepository := { _ => false }

libraryDependencies ++= Seq(
  "eu.cdevreeze.tqa" %% "tqa" % "0.8.11",
  "net.sf.saxon" % "Saxon-HE" % "9.9.1-5",
  "org.jooq" % "jooq" % "3.12.3",
  "org.springframework" % "spring-jdbc" % "5.2.0.RELEASE",
  "com.google.code.findbugs" % "jsr305" % "3.0.2" % Optional, // This solved compiler error "... could not find MAYBE in enum"
  "org.postgresql" % "postgresql" % "42.2.8",
  "com.zaxxer" % "HikariCP" % "3.4.1",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe" % "config" % "1.4.0",

  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)

lazy val pomData =
  <url>https://github.com/dvreeze/tqa-db</url>
  <licenses>
    <license>
      <name>Apache License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      <distribution>repo</distribution>
      <comments>Tqa-db is licensed under Apache License, Version 2.0</comments>
    </license>
  </licenses>
  <scm>
    <connection>scm:git:git@github.com:dvreeze/tqa-db.git</connection>
    <url>https://github.com/dvreeze/tqa-db.git</url>
    <developerConnection>scm:git:git@github.com:dvreeze/tqa-db.git</developerConnection>
  </scm>
  <developers>
    <developer>
      <id>dvreeze</id>
      <name>Chris de Vreeze</name>
      <email>chris.de.vreeze@caiway.net</email>
    </developer>
  </developers>

