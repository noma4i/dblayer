// swift-tools-version: 6.0

import PackageDescription

let package = Package(
  name: "DBlayerCore",
  platforms: [
    .iOS(.v17),
    .macOS(.v14)
  ],
  products: [
    .library(name: "DBlayerCore", targets: ["DBlayerCore"])
  ],
  targets: [
    .binaryTarget(name: "dblayerFFI", path: "DBlayerFFI.xcframework"),
    .target(
      name: "DBlayerCore",
      dependencies: ["dblayerFFI"],
      swiftSettings: [.swiftLanguageMode(.v5)]
    ),
    .testTarget(
      name: "DBlayerCoreTests",
      dependencies: ["DBlayerCore"]
    )
  ]
)
