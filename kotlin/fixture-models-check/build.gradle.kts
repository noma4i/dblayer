plugins {
  id("com.android.library")
  id("org.jetbrains.kotlin.plugin.serialization")
}

android {
  namespace = "com.noma4i.dblayer.fixturemodelscheck"
  compileSdk = 37
  defaultConfig { minSdk = 33 }
}

dependencies {
  implementation(project(":"))
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
  compilerOptions.allWarningsAsErrors.set(true)
}

androidComponents {
  onVariants { variant ->
    variant.sources.kotlin?.addStaticSourceDirectory("../src/test/kotlin/com/noma4i/dblayer/models")
  }
}
