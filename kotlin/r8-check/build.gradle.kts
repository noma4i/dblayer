plugins {
    kotlin("jvm") version "2.4.21"
}

kotlin {
    jvmToolchain(17)
    sourceSets.main {
        kotlin.srcDirs("../src/main/kotlin", "src")
        kotlin.include("**/dblayer.kt", "**/R8Probe.kt")
    }
}

dependencies {
    implementation("net.java.dev.jna:jna:5.17.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
}

val rulesFile = providers.gradleProperty("rules").orElse("../consumer-rules.pro")
val shrink = tasks.register<JavaExec>("shrink") {
    dependsOn(tasks.jar)
    val androidHome = System.getenv("ANDROID_HOME") ?: "${System.getProperty("user.home")}/Library/Android/sdk"
    classpath = files("$androidHome/build-tools/37.0.0/lib/d8.jar")
    mainClass = "com.android.tools.r8.R8"
    doFirst {
        val destination = layout.buildDirectory.file("shrunk.jar").get().asFile
        destination.delete()
        args("--release", "--classfile", "--no-data-resources", "--output", destination.absolutePath,
             "--pg-conf", file(rulesFile.get()).absolutePath,
             "--pg-conf", file("probe.pro").absolutePath,
             "--pg-map-output", layout.buildDirectory.file("mapping.txt").get().asFile.absolutePath,
             "--lib", System.getProperty("java.home"),
             tasks.jar.get().archiveFile.get().asFile.absolutePath)
        configurations.runtimeClasspath.get().files.forEach {
            if (it.name.startsWith("jna-")) args(it.absolutePath)
            else args("--lib", it.absolutePath)
        }
    }
}

val androidRelease = tasks.register<GradleBuild>("androidRelease") {
    dependsOn(project.tasks.jar)
    dir = file("android")
    tasks = listOf("assembleRelease")
    startParameter.projectProperties = mapOf("rules" to file(rulesFile.get()).absolutePath)
}

tasks.register<JavaExec>("probe") {
    dependsOn(shrink, androidRelease)
    classpath = files(layout.buildDirectory.file("shrunk.jar")) + configurations.runtimeClasspath.get()
    mainClass = "com.noma4i.dblayer.R8ProbeKt"
    systemProperty("jna.library.path", file("../../target/debug").absolutePath)
    jvmArgs("-ea")
    doLast {
        val mapping = layout.buildDirectory.file("mapping.txt").get().asFile.readLines()
        check(mapping.any {
            it.startsWith("com.noma4i.dblayer.Config -> ") &&
                it != "com.noma4i.dblayer.Config -> com.noma4i.dblayer.Config:"
        }) { "Config is still pinned by a blanket keep rule" }
        val dexMapping = file("android/build/outputs/mapping/release/mapping.txt").readLines()
        check(dexMapping.any {
            it.startsWith("com.noma4i.dblayer.Config -> ") &&
                it != "com.noma4i.dblayer.Config -> com.noma4i.dblayer.Config:"
        }) { "Android Config is still pinned by a blanket keep rule" }
        println("R8 ordinary record obfuscation: passed")
    }
}
