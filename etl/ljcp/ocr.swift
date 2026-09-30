// Local macOS Vision OCR; no network or UI automation.
// Compile: swiftc -module-cache-path /tmp/ljcp-swift-cache ocr.swift -o /tmp/ljcp-ocr
// Usage: /tmp/ljcp-ocr /absolute/path/to/raw_data/ljcp/ocr/annual_2021
import Foundation
import Vision
import AppKit

let directory = URL(fileURLWithPath: CommandLine.arguments[1])
let paths = try FileManager.default.contentsOfDirectory(at: directory, includingPropertiesForKeys: nil)
for path in paths.filter({ $0.lastPathComponent.hasPrefix("crop_") && $0.pathExtension == "png" }).sorted(by: {$0.path < $1.path}) {
    let output = path.deletingPathExtension().appendingPathExtension("json")
    if FileManager.default.fileExists(atPath: output.path) { continue }
    let image = NSImage(contentsOf: path)!
    var rect = CGRect(origin: .zero, size: image.size)
    let cg = image.cgImage(forProposedRect: &rect, context: nil, hints: nil)!
    let request = VNRecognizeTextRequest()
    request.recognitionLevel = .accurate
    request.usesLanguageCorrection = false
    try VNImageRequestHandler(cgImage: cg, options: [:]).perform([request])
    let rows = (request.results ?? []).map { r -> [String: Any] in
        let c = r.topCandidates(1).first!
        return ["text": c.string, "confidence": c.confidence,
                "x": r.boundingBox.minX, "y": r.boundingBox.minY,
                "w": r.boundingBox.width, "h": r.boundingBox.height]
    }
    let data = try JSONSerialization.data(withJSONObject: rows, options: [.prettyPrinted,.sortedKeys])
    try data.write(to: output)
}
