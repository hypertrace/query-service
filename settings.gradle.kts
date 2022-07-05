rootProject.name = "query-service-root"

pluginManagement {
  repositories {
    mavenLocal()
    gradlePluginPortal()
    maven("https://hypertrace.jfrog.io/artifactory/maven")
  }
}

plugins {
  id("org.hypertrace.version-settings") version "0.2.0"
}

include(":query-service-api")
include(":query-service-client")
include(":query-service-impl")
include(":query-service")
include(":query-service-factory")
