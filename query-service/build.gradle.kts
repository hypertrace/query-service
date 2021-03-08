plugins {
  java
  application
  id("org.hypertrace.docker-java-application-plugin") version "0.8.1"
  id("org.hypertrace.docker-publish-plugin") version "0.8.1"
}

dependencies {
  constraints {
    runtimeOnly("io.netty:netty-codec-http2:4.1.59.Final") {
      because("https://snyk.io/vuln/SNYK-JAVA-IONETTY-1020439")
    }
    runtimeOnly("io.netty:netty-handler-proxy:4.1.59.Final") {
      because("https://snyk.io/vuln/SNYK-JAVA-IONETTY-1020439s")
    }
  }

  implementation(project(":query-service-impl"))
  implementation("org.hypertrace.core.grpcutils:grpc-server-utils:0.3.0")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.21")
  implementation("io.grpc:grpc-netty:1.33.0")
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("com.typesafe:config:1.4.0")

  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")
}

application {
  mainClass.set("org.hypertrace.core.serviceframework.PlatformServiceLauncher")
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dbootstrap.config.uri=file:${projectDir}/src/main/resources/configs", "-Dservice.name=${project.name}")
}

hypertraceDocker {
  defaultImage {
    javaApplication {
      port.set(8090)
    }
  }
}