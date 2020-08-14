plugins {
  java
  application
  id("org.hypertrace.docker-java-application-plugin") version "0.4.0"
  id("org.hypertrace.docker-publish-plugin") version "0.4.0"
}

dependencies {
  implementation(project(":query-service-impl"))
  implementation("org.hypertrace.core.grpcutils:grpc-server-utils:0.1.3")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.2")
  implementation("io.grpc:grpc-netty:1.30.2")

  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")

  implementation("com.typesafe:config:1.4.0")
}

application {
  mainClassName = "org.hypertrace.core.serviceframework.PlatformServiceLauncher"
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dbootstrap.config.uri=file:${projectDir}/src/main/resources/configs", "-Dservice.name=${project.name}")
}
