plugins {
  id("com.android.library") version "9.2.1"
  id("org.jetbrains.kotlin.plugin.serialization") version "2.2.10"
}

android {
  namespace = "com.noma4i.dblayer"
  compileSdk = 36

  defaultConfig {
    minSdk = 26
    ndk {
      abiFilters += listOf("arm64-v8a", "armeabi-v7a", "x86_64", "x86")
    }
  }

  testOptions {
    unitTests.all {
      it.systemProperty("jna.library.path", file("../target/debug").absolutePath)
    }
  }
}

dependencies {
  api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
  api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")
  implementation("com.squareup.okhttp3:okhttp:4.12.0")
  implementation("net.java.dev.jna:jna:5.17.0@aar")
  testImplementation("net.java.dev.jna:jna:5.17.0")
  testImplementation("junit:junit:4.13.2")
  testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
  testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
}
