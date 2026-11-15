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
      url: "https://github.com/noma4i/dblayer/releases/download/6.39.0/DBlayerFFI.xcframework.zip",
      checksum: "0f8a023b5de817a4e6be0dc4943d8e35e039979b3b58c469211a93889b9d9317"
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
