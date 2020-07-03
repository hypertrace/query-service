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
    implementation("com.fasterxml.jackson.core:jackson-databind:2.11.0") {
      because("Deserialization of Untrusted Data [High Severity][https://snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-561587] in com.fasterxml.jackson.core:jackson-databind@2.9.8\n" +
          "   used by org.apache.pinot:pinot-java-client")
    }
    implementation("io.netty:netty:3.10.3.Final") {
      because("HTTP Request Smuggling [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-IONETTY-473694] in io.netty:netty@3.9.6.Final\n" +
          "    introduced by org.apache.pinot:pinot-java-client")
    }
    implementation("org.apache.zookeeper:zookeeper:3.6.1") {
      because("Authentication Bypass [High Severity][https://snyk.io/vuln/SNYK-JAVA-ORGAPACHEZOOKEEPER-32301] in org.apache.zookeeper:zookeeper@3.4.6\n" +
          "    introduced by org.apache.pinot:pinot-java-client")
    }
    implementation("commons-codec:commons-codec:1.13") {
      because("Information Exposure [Low Severity][https://snyk.io/vuln/SNYK-JAVA-COMMONSCODEC-561518] in commons-codec:commons-codec@1.11"
          + " introduced org.apache.httpcomponents:httpclient@4.5.12")
    }
  }
  api(project(":query-service-api"))
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.1.0")
  implementation("org.apache.pinot:pinot-java-client:0.3.0") {
    // We want to use log4j2 impl so exclude the log4j binding of slf4j
    exclude("org.slf4j", "slf4j-log4j12")
  }
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("com.typesafe:config:1.3.2")

  testImplementation(project(":query-service-api"))
  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:3.3.3")
}
