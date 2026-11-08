import DBlayerTesting
import Foundation
@_spi(Testing) @testable import DBlayer

let modelsArrayJSON: String = {
  let parsed = try! JSONSerialization.jsonObject(with: Data(modelsJSON.utf8)) as! [String: Any]
  return String(decoding: try! JSONSerialization.data(withJSONObject: parsed["models"]!), as: UTF8.self)
}()

let emptySpecs = #"{"models":[]}"#

func specs(_ models: String) -> String { #"{"models":\#(models)}"# }

/// The two orders of the open inputs: the platform reports online before the operations are declared, or the test sets it after them.
enum OpenOrder: String, CaseIterable {
  case onlineAtOpen, onlineAfterRegister
  var onlineAtOpen: Bool { self == .onlineAtOpen }
}

/// The schema with one server subscription: a schema holds one subscription, so a test of a model-level subscription runs on a schema that declares only that one.
func schema(onlySubscription name: String, in source: String = modelsJSON) -> String {
  var models = ((try? JSONSerialization.jsonObject(with: Data(source.utf8))) as? [String: Any])?["models"] as? [[String: Any]] ?? []
  for index in models.indices {
    models[index].removeValue(forKey: "change_feed")
    if let listens = models[index]["listens"] as? [[String: Any]] { models[index]["listens"] = listens.filter { $0["subscription"] as? String == name } }
    if var relations = models[index]["relations"] as? [[String: Any]] {
      for at in relations.indices { if let updates = relations[at]["updates"] as? [[String: Any]] { relations[at]["updates"] = updates.filter { $0["subscription"] as? String == name } } }
      models[index]["relations"] = relations
    }
  }
  return specs(String(decoding: (try? JSONSerialization.data(withJSONObject: models)) ?? Data(), as: UTF8.self))
}
