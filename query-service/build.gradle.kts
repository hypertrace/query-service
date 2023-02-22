plugins {
  java
  application
  jacoco
  id("org.hypertrace.docker-java-application-plugin") version "0.9.4"
  id("org.hypertrace.docker-publish-plugin") version "0.9.4"
  id("org.hypertrace.integration-test-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  implementation(project(":query-service-factory"))
  implementation("org.hypertrace.core.grpcutils:grpc-server-utils:0.11.2")
  implementation("org.hypertrace.core.serviceframework:platform-grpc-service-framework:0.1.49")
  implementation("org.slf4j:slf4j-api:1.7.32")
  implementation("com.typesafe:config:1.4.1")

  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
  runtimeOnly("io.grpc:grpc-netty")
  integrationTestImplementation("com.google.protobuf:protobuf-java-util:3.22.0")
  integrationTestImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  integrationTestImplementation("org.testcontainers:testcontainers:1.16.2")
  integrationTestImplementation("org.testcontainers:junit-jupiter:1.16.2")
  integrationTestImplementation("org.testcontainers:kafka:1.16.2")
  integrationTestImplementation("org.hypertrace.core.serviceframework:integrationtest-service-framework:0.1.49")
  integrationTestImplementation("com.github.stefanbirkner:system-lambda:1.2.0")

  integrationTestImplementation("org.apache.kafka:kafka-clients:5.5.1-ccs")
  integrationTestImplementation("org.apache.kafka:kafka-streams:5.5.1-ccs")
  integrationTestImplementation("org.apache.avro:avro:1.11.1")
  constraints {
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.4.2") {
      because("version 2.12.7.1 has a vulnerability https://snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-3038424")
    }
  }
  integrationTestImplementation("com.google.guava:guava:31.1-jre")
  integrationTestImplementation("org.hypertrace.core.datamodel:data-model:0.1.12")
  integrationTestImplementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-serdes:0.1.13")

  integrationTestImplementation(project(":query-service-client"))
  integrationTestImplementation("org.hypertrace.core.attribute.service:attribute-service-client:0.12.3")
}

application {
  mainClass.set("org.hypertrace.core.serviceframework.PlatformServiceLauncher")
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dbootstrap.config.uri=file:$projectDir/src/main/resources/configs", "-Dservice.name=${project.name}")
}

tasks.integrationTest {
  useJUnitPlatform()
}

hypertraceDocker {
  defaultImage {
    javaApplication {
      port.set(8090)
    }
  }
}

tasks.jacocoIntegrationTestReport {
  sourceSets(project(":query-service-impl").sourceSets.getByName("main"))
  sourceSets(project(":query-service-client").sourceSets.getByName("main"))
}
