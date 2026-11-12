// swift-tools-version: 6.0

import PackageDescription

let package = Package(
  name: "DBlayer",
  platforms: [
    .iOS(.v17),
    .macOS(.v14)
  ],
  products: [
    .library(name: "DBlayer", targets: ["DBlayer"]),
    .library(name: "DBlayerTesting", targets: ["DBlayerTesting"])
  ],
  targets: [
    .binaryTarget(name: "dblayerFFI", path: "DBlayerFFI.xcframework"),
    .target(
      name: "DBlayer",
      dependencies: ["dblayerFFI"],
      swiftSettings: [.swiftLanguageMode(.v5)]
    ),
    .target(
      name: "DBlayerTesting",
      dependencies: ["DBlayer"],
      swiftSettings: [.swiftLanguageMode(.v5)]
    ),
    .testTarget(
      name: "DBlayerTests",
      dependencies: ["DBlayer", "DBlayerTesting"],
      linkerSettings: [.linkedLibrary("sqlite3")]
    ),
    .testTarget(
      name: "FixtureModelsCompileTests",
      dependencies: ["DBlayer"],
      swiftSettings: [.unsafeFlags(["-warnings-as-errors"])]
    )
  ]
)
