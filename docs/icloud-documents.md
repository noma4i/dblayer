# iCloud Drive documents

Use iCloud Documents to share user files between your app, Files on iPhone, and iCloud Drive in Finder. Apple synchronizes files in the app's `Documents` directory across devices on the same Apple Account. DBlayer exposes folder creation, file transfer, and folder updates through its Swift package. [Apple's iCloud documents guide](https://developer.apple.com/documentation/UIKit/synchronizing-documents-in-the-icloud-environment)

## Opt in

Enable the iCloud capability for the host target in Xcode. Select iCloud Documents and choose the same iCloud container for the app on each platform. Configure the display name and folder access in the host target's `Info.plist`. Use a build number greater than the previous build after changing this metadata. Apple publishes the app folder after its `Documents` directory contains a document. The system controls the folder artwork and presentation. [Apple's Xcode setup guide](https://developer.apple.com/documentation/xcode/configuring-icloud-services) [Apple's publishing instructions](https://developer.apple.com/documentation/UIKit/synchronizing-documents-in-the-icloud-environment)

```xml
<key>NSUbiquitousContainers</key>
<dict>
  <key>iCloud.com.example.scans</key>
  <dict>
    <key>NSUbiquitousContainerIsDocumentScopePublic</key>
    <true/>
    <key>NSUbiquitousContainerName</key>
    <string>Example Scans</string>
    <key>NSUbiquitousContainerSupportedFolderLevels</key>
    <string>One</string>
  </dict>
</dict>
```

Store the user's opt-in as an app preference. While enabled, use `ICloudDocuments(containerIdentifier: "iCloud.com.example.scans")` and start observing the chosen folder. Cancel that observation when the setting is off. Documents already stored in iCloud Drive remain under Apple's sync control. CloudKit model sync has its own setting. [Cloud sync settings](cloud-sync.md#turn-cloud-sync-on-or-off)

## Add and read files

1. Create a folder with `try await documents.createFolder("Scans")`. Each folder and filename is a single path component.
2. Add a file with `try await documents.add(pdfURL, named: "Receipt.pdf", to: "Scans")`. This keeps the original and reports a collision if the filename already exists. The completed call means the copy is in the iCloud container. The system uploads it when it can.
3. Show entries from `documents.changes(in: "Scans")`. The first snapshot contains the current entries. Later snapshots include files added, renamed, or removed in Finder or Files.
4. Call `try await documents.download("Receipt.pdf", in: "Scans")` for a cloud-only document. Wait for the document's download status to reach `current`. Export the bytes with `try await documents.copy("Receipt.pdf", in: "Scans", to: exportURL)`. Use a new destination. A pending download returns `downloadRequired` from `copy`.

## Show file status

Each `ICloudDocument` contains its name, `downloadStatus`, and `uploadStatus`.

| Download | Meaning |
| --- | --- |
| `notDownloaded` | iCloud lists the document and its local copy is absent |
| `downloading` | The system is fetching document bytes |
| `downloaded` | An earlier version is available locally |
| `current` | The current version is available locally |
| `failed` | The system reports a download error |
| `unknown` | The system has yet to report a state |

Upload reports `notUploaded`, `uploading`, `uploaded`, `failed`, or `unknown`. Both states come from Apple, so they can change while the app is open.

Keep only user-facing documents in the published folder. If iCloud is unavailable or the account changes, show the error and restart observation once the current account is available. [Apple's file storage guidance](https://developer.apple.com/videos/play/tech-talks/204/)
