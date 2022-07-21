plugins {
  `java-library`
}

dependencies {
  api("org.hypertrace.core.serviceframework:platform-grpc-service-framework:0.1.40-SNAPSHOT")

  implementation(project(":query-service-impl"))
  implementation("com.google.inject:guice:5.0.1")
}
