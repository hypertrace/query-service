plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  constraints {
    implementation("com.fasterxml.jackson.core:jackson-databind:2.12.2") {
      because("Multiple vulnerabilities")
    }
    implementation("io.netty:netty:3.10.6.Final") {
      because("https://snyk.io/vuln/SNYK-JAVA-IONETTY-30430")
    }
    implementation("org.apache.zookeeper:zookeeper:3.6.2") {
      because("Multiple vulnerabilities")
    }
    implementation("io.netty:netty-transport-native-epoll:4.1.68.Final") {
      because("Multiple vulnerabilities")
    }
    implementation("io.netty:netty-handler:4.1.68.Final") {
      because("Multiple vulnerabilities")
    }
  }
  api(project(":query-service-api"))
  api("com.typesafe:config:1.4.1")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.4.0")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.4.0")
  implementation("org.hypertrace.core.grpcutils:grpc-server-rx-utils:0.4.0")
  implementation("org.hypertrace.core.attribute.service:attribute-service-api:0.12.3")
  implementation("org.hypertrace.core.attribute.service:attribute-projection-registry:0.12.3")
  implementation("org.hypertrace.core.attribute.service:caching-attribute-service-client:0.12.3")
  implementation("org.hypertrace.core.serviceframework:service-framework-spi:0.1.28")
  implementation("com.google.inject:guice:5.0.1")
  implementation("org.apache.pinot:pinot-java-client:0.6.0") {
    // We want to use log4j2 impl so exclude the log4j binding of slf4j
    exclude("org.slf4j", "slf4j-log4j12")
  }
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("commons-codec:commons-codec:1.15")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.28")
  implementation("com.google.protobuf:protobuf-java-util:3.15.6")
  implementation("com.google.guava:guava:30.1.1-jre")
  implementation("io.reactivex.rxjava3:rxjava:3.0.11")

  testImplementation(project(":query-service-api"))
  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("org.mockito:mockito-junit-jupiter:3.8.0")
  testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.14.1")
}
