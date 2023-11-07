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
    implementation("org.jetbrains.kotlin:kotlin-stdlib:1.6.0") {
      because(
        "Improper Locking [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGJETBRAINSKOTLIN-2628385] " +
          "in org.jetbrains.kotlin:kotlin-stdlib@1.4.10",
      )
    }
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.2") {
      because("Multiple vulnerabilities")
    }
    implementation("org.apache.calcite:calcite-core:1.34.0") {
      because("CVE-2022-39135")
    }
    implementation("org.apache.calcite:calcite-babel:1.34.0") {
      because("CVE-2022-39135")
    }
    implementation("org.apache.avro:avro:1.11.3") {
      because("CVE-2023-39410")
    }
    implementation("org.apache.commons:commons-compress:1.24.0") {
      because("CVE-2023-42503")
    }
    implementation("org.apache.helix:helix-core:1.3.0") {
      because("CVE-2022-47500")
    }
    implementation("org.apache.zookeeper:zookeeper:3.7.2") {
      because("CVE-2023-44981")
    }
    implementation("org.webjars:swagger-ui:5.1.0") {
      because("CVE-2019-16728,CVE-2020-26870")
    }
    implementation("net.minidev:json-smart:2.4.11") {
      because("CVE-2023-1370")
    }
    implementation("org.xerial.snappy:snappy-java:1.1.10.5") {
      because("CVE-2023-43642")
    }
    implementation("org.yaml:snakeyaml:2.0") {
      because("CVE-2022-1471")
    }
    implementation("org.codehaus.janino:commons-compiler:3.1.10")
    implementation("org.codehaus.janino:janino:3.1.10")
    implementation("com.squareup.okio:okio:3.4.0") {
      because("CVE-2023-3635")
    }
    implementation("org.apache.zookeeper:zookeeper:3.7.2") {
      because("CVE-2023-44981")
    }
  }
  api(project(":query-service-api"))
  api("com.typesafe:config:1.4.1")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.12.6")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.12.6")
  implementation("org.hypertrace.core.grpcutils:grpc-server-rx-utils:0.12.6")
  implementation("org.hypertrace.core.attribute.service:attribute-service-api:0.14.26")
  implementation("org.hypertrace.core.attribute.service:attribute-projection-registry:0.14.26")
  implementation("org.hypertrace.core.attribute.service:caching-attribute-service-client:0.14.26")
  implementation("com.google.inject:guice:5.0.1")
  implementation("org.apache.pinot:pinot-java-client:0.12.1") {
    // We want to use log4j2 impl so exclude the log4j binding of slf4j
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("log4j", "log4j")
  }
  implementation("org.slf4j:slf4j-api:1.7.32")
  implementation("commons-codec:commons-codec:1.15")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.62")
  implementation("com.google.protobuf:protobuf-java-util:3.22.0")
  implementation("com.google.guava:guava:32.1.2-jre")
  implementation("io.reactivex.rxjava3:rxjava:3.0.11")
  implementation("com.squareup.okhttp3:okhttp:4.11.0")
  implementation("org.postgresql:postgresql:42.4.3")
  implementation("io.trino:trino-jdbc:423")

  annotationProcessor("org.projectlombok:lombok:1.18.20")
  compileOnly("org.projectlombok:lombok:1.18.20")

  testImplementation(project(":query-service-api"))
  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("org.mockito:mockito-junit-jupiter:3.8.0")
  testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
  testImplementation("com.squareup.okhttp3:mockwebserver:4.11.0")
}
