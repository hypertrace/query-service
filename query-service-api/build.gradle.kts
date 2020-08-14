import com.google.protobuf.gradle.*

plugins {
  `java-library`
  id("com.google.protobuf") version "0.8.8"
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

val generateLocalGoGrpcFiles = false

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:3.12.3"
  }
  plugins {
    // Optional: an artifact spec for a protoc plugin, with "grpc" as
    // the identifier, which can be referred to in the "plugins"
    // container of the "generateProtoTasks" closure.
    id("grpc_java") {
      artifact = "io.grpc:protoc-gen-grpc-java:1.30.2"
    }

    if (generateLocalGoGrpcFiles) {
      id("grpc_go") {
        path = "<go-path>/bin/protoc-gen-go"
      }
    }
  }
  generateProtoTasks {
    ofSourceSet("main").forEach {
      it.plugins {
        // Apply the "grpc" plugin whose spec is defined above, without options.
        id("grpc_java")

        if (generateLocalGoGrpcFiles) {
          id("grpc_go")
        }
      }
      it.builtins {
        java

        if (generateLocalGoGrpcFiles) {
          id("go")
        }
      }
    }
  }
}

sourceSets {
  main {
    java {
      srcDirs("src/main/java", "build/generated/source/proto/main/java", "build/generated/source/proto/main/grpc_java")
    }

    proto {
      srcDirs("src/main/proto")
    }
  }
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  api("io.grpc:grpc-protobuf:1.30.2")
  api("io.grpc:grpc-stub:1.30.2")
  api("javax.annotation:javax.annotation-api:1.3.2")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("com.google.protobuf:protobuf-java-util:3.12.2")
}
