#if os(macOS) || os(Linux)
import Foundation
#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#else
import Glibc
#endif

/// Run one model turn in Claude Code. The outer agent alone owns the transcript
/// and executes tools, so a cancelled or resumed request needs no hidden session.
struct ClaudeCLIProvider: APIProvider {
    let api = "anthropic-messages"
    struct Gateway: Sendable {
        let baseURL: String
        let key: String
    }

    var context1m = false
    var gateway: Gateway?
    var environment = ProcessInfo.processInfo.environment

    func stream(model: Model, context: Context, options: StreamOptions?) -> AssistantMessageStream {
        let output = AssistantMessageStream()
        let (events, sink) = AsyncThrowingStream<SSEMessage, Error>.makeStream()
        let signal = options?.cancellation ?? CancellationHandle()
        let fallback = gateway == nil && AnthropicProvider.usesServerSideFallback(
            model: model, url: URL(string: "https://api.anthropic.com")!, options: options
        )
        DispatchQueue.global(qos: .userInitiated).async {
            do {
                try signal.throwIfCancelled()
                try run(model: model, context: context, options: options, signal: signal, fallback: fallback, sink: sink)
                sink.finish()
            } catch {
                sink.finish(throwing: error)
            }
        }
        Task {
            let state = AnthropicStreamState(api: api, provider: model.provider, modelId: model.id)
            state.requestModel = model
            state.signal = signal
            state.fallbackEnabled = fallback
            do {
                try await AnthropicProvider.drive(events: events, out: output, state: state)
            } catch {
                let failure = ProviderFailure.capture(error)
                let message = signal.isCancelled ? state.asAborted() :
                    AssistantMessage.makeError(api: api, model: model, text: failure.message, failure: failure)
                output.push(.error(reason: message.stopReason, error: message))
                output.end(message)
            }
        }
        return output
    }

    private func run(model: Model, context: Context, options: StreamOptions?, signal: CancellationHandle, fallback: Bool,
                     sink: AsyncThrowingStream<SSEMessage, Error>.Continuation) throws {
        guard let token = gateway?.key ?? options?.resolvedAuth?.token ?? options?.apiKey, !token.isEmpty else {
            throw ProviderFailure(message: "Claude CLI requires an Anthropic subscription login", httpStatus: 401)
        }
        let fm = FileManager.default
        let dir = fm.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
        try fm.createDirectory(at: dir, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o700])
        defer { try? fm.removeItem(at: dir) }

        var wireModel = model
        var compat = model.compat ?? ModelCompat()
        compat.supportsMidConvoToolChanges = false
        compat.supportsServerCompaction = false
        wireModel.compat = compat
        // The CLI chooses breakpoints after it inserts its own messages.
        // Suppress only our wire annotations, preserving the child's cache policy.
        var encodingOptions = options ?? StreamOptions()
        encodingOptions.cacheRetention = CacheRetention.none
        let encoded = try AnthropicProvider.encodeBody(model: wireModel, context: context, options: encodingOptions,
                                                       maximumOutputTokens: AnthropicProvider.claudeCodeMaximumOutputTokens,
                                                       fallbackEnabled: fallback)
        guard let body = try JSONSerialization.jsonObject(with: encoded) as? Object,
              var messages = body["messages"] as? [Object], var last = messages.popLast(),
              last["role"] as? String == "user" else {
            throw ProviderFailure(message: "Claude CLI requires a final user message or tool result")
        }
        // On resume Claude Code repairs an assistant-final tool_use as an
        // interrupted call before reading stdin. Restore its real results in
        // the same transcript, then trigger the next sampling turn.
        if let blocks = last["content"] as? [Object],
           blocks.contains(where: { $0["type"] as? String == "tool_result" }) {
            let results = blocks.filter { $0["type"] as? String == "tool_result" }
            let remainder = blocks.filter { $0["type"] as? String != "tool_result" }
            messages.append(["role": "user", "content": results])
            last["content"] = remainder.isEmpty ? [["type": "text", "text": "Continue from the tool results."]] : remainder
        }
        let requestTools: [Object] = (context.tools ?? []).map { tool in
            ["name": tool.name, "description": tool.description,
             "inputSchema": anyFromJSONValue(ToolSchemaRoot.objectRoot(tool.parameters)) ?? Object()]
        }
        var args = ["-p", "--verbose", "--include-partial-messages", "--output-format", "stream-json",
                    "--input-format", "stream-json", "--max-turns", "1", "--no-session-persistence",
                    "--tools", "", "--permission-mode", "dontAsk", "--strict-mcp-config",
                    "--disable-slash-commands", "--setting-sources", "", "--model", model.id + (context1m ? "[1m]" : "")]
        let session = options?.sessionId.flatMap { id in
            id.isEmpty ? nil : (UUID(uuidString: id)?.uuidString.lowercased() ?? deterministicUUID("kwwk:claude-cli:" + id))
        } ?? UUID().uuidString.lowercased()
        args += ["--session-id", session]
        if let prompt = context.systemPrompt, !prompt.isEmpty {
            let file = dir.appendingPathComponent("system.txt")
            try Data(prompt.utf8).write(to: file)
            args += ["--system-prompt-file", file.path]
        }
        if !messages.isEmpty {
            let history = try writeHistory(messages, in: dir, model: model.id)
            args += ["--resume", history.path, "--fork-session"]
        }

        let child = Process()
        // Only inherit system/proxy configuration. In particular, never send a
        // subscription token to an endpoint selected by another agent's env.
        var env = environment.filter { ["PATH", "LANG", "LC_ALL", "TMPDIR", "HTTPS_PROXY", "HTTP_PROXY",
                                          "NO_PROXY", "https_proxy", "http_proxy", "no_proxy",
                                          "SSL_CERT_FILE", "NODE_EXTRA_CA_CERTS"].contains($0.key) }
        // Claude Code stores its device ID in its own config. Keep this
        // independent of the transient stdin/history and of the user's CLI.
        let configRoot = options?.sessionId?.isEmpty == false
            ? OAuthStore.defaultURL().deletingLastPathComponent() : dir
        let config = configRoot
            .appendingPathComponent("claude-cli", isDirectory: true)
            .appendingPathComponent(gateway.map { deterministicUUID($0.baseURL) } ?? "subscription", isDirectory: true)
        try fm.createDirectory(at: config, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        env["HOME"] = config.path
        env["CLAUDE_CONFIG_DIR"] = config.path
        if let gateway {
            guard let url = URLComponents(string: gateway.baseURL),
                  ["http", "https"].contains(url.scheme?.lowercased()),
                  let host = url.host, !host.isEmpty else {
                throw ProviderFailure(message: "Invalid base URL in the Anthropic API-key login")
            }
            env["ANTHROPIC_BASE_URL"] = gateway.baseURL
            env["ANTHROPIC_AUTH_TOKEN"] = gateway.key
        } else {
            env["CLAUDE_CODE_OAUTH_TOKEN"] = token
        }
        env["CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC"] = "1"
        env["DISABLE_AUTO_COMPACT"] = "1"
        env["ENABLE_CLAUDEAI_MCP_SERVERS"] = "0"
        if fallback { env["ANTHROPIC_BETAS"] = "server-side-fallback-2026-06-01" }
        if options?.cacheRetention == CacheRetention.none { env["DISABLE_PROMPT_CACHING"] = "1" }
        env["ENABLE_TOOL_SEARCH"] = "false"
        env["CLAUDE_CODE_DISABLE_AUTO_MEMORY"] = "1"
        var extra = body.filter { ["max_tokens", "thinking", "temperature", "output_config", "tool_choice", "fallbacks"].contains($0.key) }
        if var choice = extra["tool_choice"] as? Object, let name = choice["name"] as? String {
            choice["name"] = Self.toolPrefix + name
            extra["tool_choice"] = choice
        }
        env["CLAUDE_CODE_EXTRA_BODY"] = String(decoding: try json(extra), as: UTF8.self)
        guard let path = (env["PATH"] ?? "").split(separator: ":").map({
            URL(fileURLWithPath: String($0)).appendingPathComponent("claude")
        }).first(where: { fm.isExecutableFile(atPath: $0.path) }) else {
            throw ProviderFailure(message: "Claude Code executable 'claude' was not found on PATH")
        }
        child.executableURL = path
        child.arguments = args
        child.environment = env
        child.currentDirectoryURL = config
        let input = try CLIInput()
        child.standardInput = input.child
        let pipe = Pipe()
        child.standardOutput = pipe
        let diagnostic = dir.appendingPathComponent("stderr")
        fm.createFile(atPath: diagnostic.path, contents: nil)
        let errors = try FileHandle(forWritingTo: diagnostic)
        defer { try? errors.close() }
        child.standardError = errors
        try child.run()
        try? input.child.close()
        // The child has no persistent session or in-process tools to unwind.
        // SIGKILL also wakes a blocked read/write if the CLI is wedged.
        let stop: @Sendable () -> Void = {
            if child.isRunning { _ = kill(child.processIdentifier, SIGKILL) }
        }
        let cancel = signal.onCancel { _ in stop() }
        defer {
            cancel.cancel()
            stop()
            child.waitUntilExit()
        }
        try signal.throwIfCancelled()
        try input.write(["type": "control_request", "request_id": "init",
                         "request": ["subtype": "initialize", "sdkMcpServers": requestTools.isEmpty ? [] : ["kwwk"]]])
        try input.write(["type": "user", "message": last])
        var buffer = Data()
        while true {
            let chunk = pipe.fileHandleForReading.availableData
            if chunk.isEmpty { break }
            buffer.append(chunk)
            while let newline = buffer.firstIndex(of: 10) {
                let line = buffer[..<newline]
                buffer = buffer[(newline + 1)...]
                guard !line.isEmpty, let record = try JSONSerialization.jsonObject(with: line) as? Object else { continue }
                if let terminal = try consume(record, tools: requestTools, input: input, sink: sink) {
                    // Let Claude Code flush its generated device identity before
                    // publishing the terminal frame (the next turn can start then).
                    input.finishWriting()
                    let deadline = DispatchWorkItem(block: stop)
                    DispatchQueue.global().asyncAfter(deadline: .now() + 2, execute: deadline)
                    defer { deadline.cancel() }
                    while !pipe.fileHandleForReading.availableData.isEmpty {}
                    child.waitUntilExit()
                    sink.yield(terminal)
                    return
                }
            }
        }
        try signal.throwIfCancelled()
        child.waitUntilExit()
        let file = try FileHandle(forReadingFrom: diagnostic)
        defer { try? file.close() }
        let detail = String(decoding: try file.read(upToCount: 2048) ?? Data(), as: UTF8.self)
            .replacingOccurrences(of: token, with: "[redacted]")
        throw ProviderFailure(message: "Claude CLI ended before message_stop (exit \(child.terminationStatus)). \(detail)")
    }

    private typealias Object = [String: Any]
    private static let toolPrefix = "mcp__kwwk__"

    private func writeHistory(_ messages: [Object], in dir: URL, model: String) throws -> URL {
        let session = UUID().uuidString
        let file = dir.appendingPathComponent(session + ".jsonl")
        var rows = Data()
        var parent: Any = NSNull()
        for var message in messages {
            let id = UUID().uuidString
            let role = message["role"] as? String ?? "user"
            if role == "assistant" {
                message["type"] = "message"
                message["id"] = "msg_" + id
                message["model"] = model
                if var blocks = message["content"] as? [Object] {
                    for i in blocks.indices where blocks[i]["type"] as? String == "tool_use" {
                        if let name = blocks[i]["name"] as? String { blocks[i]["name"] = Self.toolPrefix + name }
                    }
                    message["content"] = blocks
                }
            }
            rows.append(try json(["type": role, "uuid": id, "parentUuid": parent, "sessionId": session,
                                  "cwd": dir.path, "timestamp": ISO8601DateFormatter().string(from: Date()),
                                  "isSidechain": false, "message": message]))
            rows.append(10)
            parent = id
        }
        try rows.write(to: file)
        return file
    }

    private func consume(_ record: Object, tools: [Object], input: CLIInput,
                         sink: AsyncThrowingStream<SSEMessage, Error>.Continuation) throws -> SSEMessage? {
        switch record["type"] as? String {
        case "stream_event":
            guard var event = record["event"] as? Object, let type = event["type"] as? String else { break }
            if var block = event["content_block"] as? Object, let name = block["name"] as? String,
               block["type"] as? String == "tool_use", name.hasPrefix(Self.toolPrefix) {
                block["name"] = String(name.dropFirst(Self.toolPrefix.count))
                event["content_block"] = block
            }
            let frame = SSEMessage(event: type, data: String(decoding: try json(event), as: UTF8.self))
            if type == "message_stop" || type == "error" { return frame }
            sink.yield(frame)
        case "control_request":
            guard let id = record["request_id"] as? String, let request = record["request"] as? Object else { break }
            var response: Object = ["subtype": "error", "request_id": id, "error": "Unsupported CLI control request"]
            if request["subtype"] as? String == "mcp_message", request["server_name"] as? String == "kwwk",
               let message = request["message"] as? Object {
                var rpc: Object = ["jsonrpc": "2.0", "id": message["id"] ?? NSNull()]
                switch message["method"] as? String {
                case "initialize":
                    rpc["result"] = ["protocolVersion": "2025-03-26", "capabilities": ["tools": Object()],
                                     "serverInfo": ["name": "kwwk", "version": "1"]]
                case "tools/list": rpc["result"] = ["tools": tools]
                case "notifications/initialized", "ping": rpc["result"] = Object()
                default: rpc["error"] = ["code": -32601, "message": "kwwk executes tools after the model turn"]
                }
                response = ["subtype": "success", "request_id": id, "response": ["mcp_response": rpc]]
            }
            try input.write(["type": "control_response", "response": response])
        case "result":
            throw ProviderFailure(message: record["result"] as? String ?? "Claude CLI ended without a complete model turn",
                                  httpStatus: record["api_error_status"] as? Int)
        default: break
        }
        return nil
    }
}

private func json(_ object: Any) throws -> Data { try JSONSerialization.data(withJSONObject: object) }

/// A local socket used as the child's stdin. A child exiting during a write
/// must report EPIPE to this request instead of delivering SIGPIPE to kwwk.
private final class CLIInput {
    let child: FileHandle
    private let writer: FileHandle
    init() throws {
        var fds = [Int32](repeating: -1, count: 2)
        #if canImport(Glibc)
        let kind = Int32(SOCK_STREAM.rawValue)
        #else
        let kind = SOCK_STREAM
        #endif
        guard socketpair(AF_UNIX, kind, 0, &fds) == 0 else { throw POSIXError(.EMFILE) }
        child = FileHandle(fileDescriptor: fds[0], closeOnDealloc: true)
        writer = FileHandle(fileDescriptor: fds[1], closeOnDealloc: true)
        for fd in fds { _ = fcntl(fd, F_SETFD, FD_CLOEXEC) }
        #if os(macOS)
        var on: Int32 = 1
        guard setsockopt(fds[1], SOL_SOCKET, SO_NOSIGPIPE, &on, socklen_t(MemoryLayout.size(ofValue: on))) == 0
        else { throw POSIXError(.EIO) }
        #endif
    }

    func finishWriting() {
        _ = shutdown(writer.fileDescriptor, Int32(SHUT_WR))
    }

    func write(_ record: [String: Any]) throws {
        var data = try json(record)
        data.append(10)
        try data.withUnsafeBytes { raw in
            var offset = 0
            while offset < raw.count {
                #if os(Linux)
                let flags = Int32(MSG_NOSIGNAL)
                #else
                let flags: Int32 = 0
                #endif
                let size = send(writer.fileDescriptor, raw.baseAddress!.advanced(by: offset), raw.count - offset, flags)
                if size < 0, errno == EINTR { continue }
                guard size > 0 else { throw POSIXError(.EPIPE) }
                offset += size
            }
        }
    }
}
#endif
