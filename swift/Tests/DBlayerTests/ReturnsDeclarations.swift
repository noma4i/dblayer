import Foundation

let fixtureReturns: [String: String] = [
  "KeywordCall": Operations.whenReturns,
  "QrCodeCreate": Operations.qrCodeCreateReturns,
  "SignOut": Operations.signOutReturns,
  "StickerSend": Operations.stickerSendReturns,
  "Pricing": Operations.pricingLoadReturns,
  "MomentCreate": Operations.momentCreateReturns,
  "Giphy": Operations.giphyLoadReturns,
  "WidgetsSend": Operations.widgetsSendReturns,
  "containerMedia": Operations.containerMediaLoadReturns,
]

let rawReturns = #"{"name":"raw","kind":{"type":"raw"}}"#

func declaringReturns(_ defsJson: String) throws -> String {
  guard var defs = try JSONSerialization.jsonObject(with: Data(defsJson.utf8)) as? [[String: Any]] else { return defsJson }
  for index in defs.indices where defs[index]["returns"] == nil {
    defs[index]["returns"] = fixtureReturns[defs[index]["name"] as? String ?? ""] ?? rawReturns
  }
  return String(decoding: try JSONSerialization.data(withJSONObject: defs), as: UTF8.self)
}
