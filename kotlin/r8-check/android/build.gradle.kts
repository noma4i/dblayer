plugins {
    id("com.android.application") version "9.4.1"
}

android {
    namespace = "com.noma4i.dblayer.r8check"
    compileSdk = 37
    defaultConfig {
        applicationId = "com.noma4i.dblayer.r8check"
        minSdk = 33
        targetSdk = 37
    }
    buildTypes {
        release {
            isMinifyEnabled = true
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                file(providers.gradleProperty("rules").getOrElse("../../consumer-rules.pro")),
                file("../probe.pro"),
            )
        }
    }
}

dependencies {
    implementation(files("../build/libs/dblayer-r8-check.jar"))
    implementation("net.java.dev.jna:jna:5.17.0@aar")
    implementation("org.jetbrains.kotlin:kotlin-stdlib:2.4.21")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
}
