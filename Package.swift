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
      url: "https://github.com/noma4i/dblayer/releases/download/6.52.0/DBlayerFFI.xcframework.zip",
      checksum: "c66785d8ea89bfa747e6361b01455b69f87fd4642bc83a5aa77313b69c874d2a"
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
