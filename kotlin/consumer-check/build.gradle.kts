plugins {
  id("com.android.library")
  id("org.jetbrains.kotlin.plugin.serialization")
  id("org.jetbrains.kotlin.plugin.compose")
}

android {
  namespace = "com.noma4i.dblayer.consumercheck"
  compileSdk = 37
  defaultConfig { minSdk = 33 }
}

dependencies {
  implementation(project(":"))
}

androidComponents {
  onVariants { variant ->
    variant.sources.kotlin?.addStaticSourceDirectory("../src/test/kotlin/com/noma4i/dblayer/models")
  }
}
