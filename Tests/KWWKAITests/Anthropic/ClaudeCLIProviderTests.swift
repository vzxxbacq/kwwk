#if os(macOS) || os(Linux)
import Foundation
import Testing
@testable import KWWKAI

@Suite("Claude Code subprocess")
struct ClaudeCLIProviderTests {
    let model = AnthropicProviderTests.sampleModel
    let context = Context(messages: [.user(UserMessage(text: "hello"))])

    private func executable(records: [String], ending: String = "exit 0") throws -> URL {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: false)
        let script = """
        #!/bin/sh
        read -r init
        read -r user
        /bin/cat <<'RECORDS'
        \(records.joined(separator: "\n"))
        RECORDS
        \(ending)
        """
        let file = dir.appendingPathComponent("claude")
        try Data(script.utf8).write(to: file)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: file.path)
        return dir
    }

    /// Compare intermediate UI-visible payloads as well as the final message.
    /// Each transport stamps its own wall clock when it publishes a snapshot.
    private struct Observation: Equatable {
        let type: String
        var index: Int?
        var text: String?
        var tool: ToolCall?
        var reason: StopReason?
        var message: AssistantMessage

        init(_ event: AssistantMessageEvent) {
            type = event.type
            switch event {
            case .start(let m): message = m
            case .textStart(let i, let m), .thinkingStart(let i, let m), .toolCallStart(let i, let m):
                (index, message) = (i, m)
            case .textDelta(let i, let s, let m), .textEnd(let i, let s, let m),
                 .thinkingDelta(let i, let s, let m), .thinkingEnd(let i, let s, let m),
                 .toolCallDelta(let i, let s, let m):
                (index, text, message) = (i, s, m)
            case .toolCallEnd(let i, let call, let m):
                (index, tool, message) = (i, call, m)
            case .done(let r, let m), .error(let r, let m):
                (reason, message) = (r, m)
            }
            message.timestamp = 0
        }
    }

    @Test("The same model frames produce the same events, contents, usage and stop reason",
          arguments: [AnthropicProviderTests.textSSE, AnthropicProviderTests.toolUseSSE,
                      AnthropicProviderTests.redactedThinkingSSE, AnthropicProviderTests.usageSSE])
    func parity(sse: String) async throws {
        let parser = SSEParser()
        parser.ingest(sse)
        let frames = parser.finish()
        let records = frames.map { frame in
            let event = frame.data.replacingOccurrences(of: #""name":"calc""#, with: #""name":"mcp__kwwk__calc""#)
            return #"{"type":"stream_event","event":\#(event)}"#
        }
        let dir = try executable(records: records)
        defer { try? FileManager.default.removeItem(at: dir) }

        let direct = AnthropicProvider(client: StubSSEClient(body: sse), defaultAPIKey: "fixture")
        let cli = ClaudeCLIProvider(environment: ["PATH": dir.path])
        let a = direct.stream(model: model, context: context, options: nil)
        let b = cli.stream(model: model, context: context, options: StreamOptions(apiKey: "fixture"))
        var expected: [Observation] = [], received: [Observation] = []
        for await event in a { expected.append(Observation(event)) }
        for await event in b { received.append(Observation(event)) }
        #expect(received == expected)
        var original = await a.result(), bridged = await b.result()
        original.timestamp = 0
        bridged.timestamp = 0
        #expect(bridged == original)
    }

    @Test("CLI errors retain the HTTP status for the auth and retry layers")
    func cliError() async throws {
        let dir = try executable(records: [#"{"type":"result","is_error":true,"api_error_status":401,"result":"expired"}"#])
        defer { try? FileManager.default.removeItem(at: dir) }
        let provider = ClaudeCLIProvider(environment: ["PATH": dir.path])
        let reply = await provider.stream(model: model, context: context, options: StreamOptions(apiKey: "fixture")).result()
        #expect(reply.stopReason == .error)
        #expect(reply.failure?.httpStatus == 401)
        #expect(reply.errorMessage == "expired")
    }

    @Test("A silent, unresponsive child can be cancelled after the first event")
    func cancel() async throws {
        let dir = try executable(
            records: [#"{"type":"stream_event","event":{"type":"message_start","message":{"id":"m"}}}"#],
            ending: "trap '' TERM; exec /bin/sleep 10"
        )
        defer { try? FileManager.default.removeItem(at: dir) }
        let signal = CancellationHandle()
        let provider = ClaudeCLIProvider(environment: ["PATH": dir.path])
        let stream = provider.stream(model: model, context: context,
                                     options: StreamOptions(apiKey: "fixture", cancellation: signal))
        let start = ContinuousClock.now
        for await event in stream where event.type == "start" { signal.cancel() }
        #expect(await stream.result().stopReason == .aborted)
        #expect(start.duration(to: .now) < .seconds(5))
    }
}
#endif
