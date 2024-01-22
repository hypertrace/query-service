import com.google.protobuf.gradle.id

plugins {
  `java-library`
  id("com.google.protobuf") version "0.9.2"
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

val generateLocalGoGrpcFiles = false

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:3.21.12"
  }
  plugins {
    id("grpc") {
      artifact = "io.grpc:protoc-gen-grpc-java:1.60.0"
    }
  }
  generateProtoTasks {
    ofSourceSet("main").configureEach {
      plugins {
        // Apply the "grpc" plugin whose spec is defined above, without options.
        id("grpc")
      }
    }
  }
}

sourceSets {
  main {
    java {
      srcDirs("build/generated/source/proto/main/java", "build/generated/source/proto/main/grpc_java")
    }
  }
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  api(platform("io.grpc:grpc-bom:1.60.0"))
  api("io.grpc:grpc-protobuf")
  api("io.grpc:grpc-stub")
  api("javax.annotation:javax.annotation-api:1.3.2")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("com.google.protobuf:protobuf-java-util:3.22.0")
}
