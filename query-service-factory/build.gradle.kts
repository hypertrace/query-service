plugins {
  `java-library`
}

dependencies {
  api("org.hypertrace.core.serviceframework:platform-grpc-service-framework:0.1.53")

  implementation(project(":query-service-impl"))
  implementation("com.google.inject:guice:5.0.1")
}
