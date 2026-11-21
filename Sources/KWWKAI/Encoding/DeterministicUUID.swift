import Crypto
import Foundation

/// RFC-4122-shaped UUID derived from SHA-256 of `seed`.
func deterministicUUID(_ seed: String) -> String {
    var bytes = Array(SHA256.hash(data: Data(seed.utf8)).prefix(16))
    bytes[6] = (bytes[6] & 0x0F) | 0x50
    bytes[8] = (bytes[8] & 0x3F) | 0x80
    let hex = bytes.map { String(format: "%02x", $0) }.joined()
    let parts = [hex.prefix(8), hex.dropFirst(8).prefix(4), hex.dropFirst(12).prefix(4),
                 hex.dropFirst(16).prefix(4), hex.dropFirst(20)]
    return parts.map(String.init).joined(separator: "-")
}
