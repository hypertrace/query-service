plugins {
  `java-library`
  jacoco
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":query-service-api"))
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.11.2")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.32")
  // Config
  implementation("com.typesafe:config:1.4.1")
}
