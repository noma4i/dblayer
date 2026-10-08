// swift-tools-version: 6.0

import PackageDescription

let package = Package(
  name: "DBlayer",
  platforms: [
    .iOS(.v17),
    .macOS(.v14)
  ],
  products: [
    .library(name: "DBlayer", targets: ["DBlayer"])
  ],
  targets: [
    .binaryTarget(name: "dblayerFFI", path: "DBlayerFFI.xcframework"),
    .target(
      name: "DBlayer",
      dependencies: ["dblayerFFI"],
      swiftSettings: [.swiftLanguageMode(.v5)]
    ),
    .testTarget(
      name: "DBlayerTests",
      dependencies: ["DBlayer"]
    )
  ]
)
