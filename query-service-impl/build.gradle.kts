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
    implementation("io.netty:netty:3.10.6.Final") {
      because("https://snyk.io/vuln/SNYK-JAVA-IONETTY-30430")
    }
    implementation("io.netty:netty-common@4.1.77.Final") {
      because("https://snyk.io/vuln/SNYK-JAVA-IONETTY-2812456")
    }
    implementation("org.apache.zookeeper:zookeeper:3.6.2") {
      because("Multiple vulnerabilities")
    }
    implementation("io.netty:netty-transport-native-epoll:4.1.71.Final") {
      because("Multiple vulnerabilities")
    }
    implementation("io.netty:netty-handler:4.1.71.Final") {
      because("Multiple vulnerabilities")
    }
    implementation("org.jetbrains.kotlin:kotlin-stdlib:1.6.0") {
      because(
        "Improper Locking [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGJETBRAINSKOTLIN-2628385] " +
          "in org.jetbrains.kotlin:kotlin-stdlib@1.4.10"
      )
    }
  }
  api(project(":query-service-api"))
  api("com.typesafe:config:1.4.1")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.7.2")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.7.2")
  implementation("org.hypertrace.core.grpcutils:grpc-server-rx-utils:0.7.2")
  implementation("org.hypertrace.core.attribute.service:attribute-service-api:0.12.3")
  implementation("org.hypertrace.core.attribute.service:attribute-projection-registry:0.12.3")
  implementation("org.hypertrace.core.attribute.service:caching-attribute-service-client:0.12.3")
  implementation("org.hypertrace.core.serviceframework:service-framework-spi:0.1.33")
  implementation("com.google.inject:guice:5.0.1")
  implementation("org.apache.pinot:pinot-java-client:0.6.0") {
    // We want to use log4j2 impl so exclude the log4j binding of slf4j
    exclude("org.slf4j", "slf4j-log4j12")
  }
  implementation("org.slf4j:slf4j-api:1.7.32")
  implementation("commons-codec:commons-codec:1.15")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.33")
  implementation("com.google.protobuf:protobuf-java-util:3.20.1")
  implementation("com.google.guava:guava:31.1-jre")
  implementation("io.reactivex.rxjava3:rxjava:3.0.11")
  implementation("com.squareup.okhttp3:okhttp:4.9.3")
  annotationProcessor("org.projectlombok:lombok:1.18.20")
  compileOnly("org.projectlombok:lombok:1.18.20")

  testImplementation(project(":query-service-api"))
  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("org.mockito:mockito-junit-jupiter:3.8.0")
  testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
  testImplementation("com.squareup.okhttp3:mockwebserver:4.9.3")
}
