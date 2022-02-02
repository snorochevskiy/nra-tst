import Dependencies._

ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "snorochevskiy"
ThisBuild / organizationName := "snorochevskiy"

val sparkVersion = "3.2.0"

lazy val root = (project in file("."))
  .settings(
    name := "nra-tst",
    assemblySettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.sedona" %% "sedona-core-3.0" % "1.1.1-incubating" % "provided",
      "org.apache.sedona" %% "sedona-sql-3.0" % "1.1.1-incubating",
      "org.locationtech.jts"% "jts-core"% "1.18.0" % "compile",
      "org.wololo" % "jts2geojson" % "0.14.3" % "compile",
      "org.datasyslab" % "geotools-wrapper" % "1.1.0-25.2",
      scalaTest % Test
    )
  )

lazy val assemblySettings = Seq(
  //assemblyJarName in assembly := name.value + ".jar",
  artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
    artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision + "." + artifact.extension
  },
  // To merge duplicated artifacts in different artifacts storages: ~/.m2, ~/.cache/coursier, ~/.ivy
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case _                             => MergeStrategy.first
  }
)