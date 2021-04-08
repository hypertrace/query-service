rootProject.name = "query-service"

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
