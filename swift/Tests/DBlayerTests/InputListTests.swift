import Foundation
import DBlayerTesting
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  private static let widgets = [
    StickerWidgetInput(kind: .emoji, props: .emoji(StickerEmojiInput(code: "fire", kind: .value(.gif))), extras: .value([.tags(["a"]), .text(StickerTextInput(body: "hi"))])),
    StickerWidgetInput(kind: .gif, props: .tags(["b"]))
  ]
  private static let widgetsJSON = #"[{"kind":"EMOJI","props":{"emoji":{"code":"fire","kind":"GIF"}},"extras":[{"tags":["a"]},{"text":{"body":"hi"}}]},{"kind":"GIF","props":{"tags":["b"]}}]"#

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aListOfInputObjectsWithOneOfValuesIsSentWithOneKeyPerValue() async throws {
    let transport = try Self.openOperationRig("WidgetsSend", document: "mutation WidgetsSend($widgets: [StickerWidgetInput!]) { widgetsSend(widgets: $widgets) { id } }", reply: #"{"data":{"widgetsSend":{"id":"w1"}}}"#)

    let result = try await Operations.widgetsSend(widgets: Self.widgets)

    #expect(result.id == "w1")
    let sent = transport.calls.first { $0.operationName == "WidgetsSend" }?.variablesJson ?? ""
    #expect(JSONValue.parse(sent) == JSONValue.parse(#"{"widgets":\#(Self.widgetsJSON)}"#), "variables sent: \(sent)")
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func anOmittedOptionalListIsNotSent() async throws {
    let transport = try Self.openOperationRig("WidgetsSend", document: "mutation WidgetsSend($widgets: [StickerWidgetInput!]) { widgetsSend(widgets: $widgets) { id } }", reply: #"{"data":{"widgetsSend":{"id":"w0"}}}"#)

    _ = try await Operations.widgetsSend()

    let sent = transport.calls.first { $0.operationName == "WidgetsSend" }?.variablesJson ?? ""
    #expect(JSONValue.parse(sent) == JSONValue.parse("{}"), "variables sent: \(sent)")
  }

  @Test
  func theUploadArgsOfAModelBecomeTypedVariables() {
    #expect(Clip.uploadVariables(widgets: Self.widgets) == ["widgets": JSONValue.parse(Self.widgetsJSON)])
    #expect(Clip.uploadVariables().isEmpty)
  }
}
