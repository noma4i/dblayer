plugins {
  id("com.android.library") version "9.4.1"
  id("org.jetbrains.kotlin.plugin.serialization") version "2.4.21"
  id("org.jetbrains.kotlin.plugin.compose") version "2.4.21"
  id("maven-publish")
}

group = "com.noma4i"
version = providers.gradleProperty("releaseVersion").orNull ?: "0.0.0"

android {
  namespace = "com.noma4i.dblayer"
  compileSdk = 37

  defaultConfig {
    minSdk = 33
    consumerProguardFiles("consumer-rules.pro")
    ndk {
      abiFilters += listOf("arm64-v8a", "armeabi-v7a", "x86_64", "x86")
    }
  }

  publishing {
    singleVariant("release")
  }

  buildTypes {
    debug {
      enableUnitTestCoverage = true
    }
  }

  testOptions {
    unitTests.isIncludeAndroidResources = true
    unitTests.all {
      it.forkEvery = 1
      it.extensions.configure(org.gradle.testing.jacoco.plugins.JacocoTaskExtension::class.java) { isIncludeNoLocationClasses = true; excludes = listOf("jdk.internal.*") }
      it.systemProperty("jna.library.path", file("../target/debug").absolutePath)
    }
  }
}

dependencies {
  api("androidx.compose.runtime:runtime-annotation:1.11.4")
  api("androidx.compose.runtime:runtime:1.11.4")
  api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
  api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")
  implementation("com.squareup.okhttp3:okhttp:5.5.0")
  implementation("com.google.android.gms:play-services-auth:22.0.0")
  implementation("androidx.lifecycle:lifecycle-process:2.11.0")
  implementation("net.java.dev.jna:jna:5.19.1@aar")
  testImplementation("net.java.dev.jna:jna:5.19.1")
  testImplementation("junit:junit:4.13.2")
  testImplementation("org.robolectric:robolectric:4.17")
  testImplementation("com.squareup.okhttp3:mockwebserver:5.5.0")
  testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
}

if (providers.gradleProperty("releaseVersion").isPresent && providers.gradleProperty("stagingRepo").isPresent) {
  afterEvaluate {
    publishing {
      publications {
        register<MavenPublication>("release") {
          groupId = "com.noma4i"
          artifactId = "dblayer"
          version = providers.gradleProperty("releaseVersion").get()
          from(components["release"])
        }
      }
      repositories {
        maven {
          name = "staging"
          url = uri(providers.gradleProperty("stagingRepo").get())
        }
      }
    }
  }
}
