// swift-tools-version: 6.0

import PackageDescription

let package = Package(
  name: "DBlayer",
  platforms: [.iOS(.v17), .macOS(.v14)],
  products: [
    .library(name: "DBlayer", targets: ["DBlayer"]),
    .library(name: "DBlayerTesting", targets: ["DBlayerTesting"])
  ],
  targets: [
    .binaryTarget(
      name: "dblayerFFI",
      url: "https://github.com/noma4i/dblayer/releases/download/6.25.3/DBlayerFFI.xcframework.zip",
      checksum: "1f587097159e690213d4a22107a066cba7afb11b7555e62f8f5d7b85cefbed0a"
    ),
    .target(
      name: "DBlayer",
      dependencies: ["dblayerFFI"],
      path: "swift/Sources/DBlayer",
      swiftSettings: [.swiftLanguageMode(.v5)]
    ),
    .target(
      name: "DBlayerTesting",
      dependencies: ["DBlayer"],
      path: "swift/Sources/DBlayerTesting",
      swiftSettings: [.swiftLanguageMode(.v5)]
    )
  ]
)
