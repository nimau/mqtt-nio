//===----------------------------------------------------------------------===//
//
// This source file is part of the MQTTNIO project
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Logging
import NIO
import NIOConcurrencyHelpers
import NIOHTTP1
import NIOTransportServices
import NIOWebSocket
import XCTest

@testable import MQTTNIO

/// The close listeners only get the channel's close future, so the log is the only place the cause
/// of a dropped connection is reported. These tests run the client against a local WebSocket server
/// that misbehaves in a given way and check what the client logs.
final class MQTTCloseCauseLoggingTests: XCTestCase {
    func testOversizedWebSocketFrameIsLogged() throws {
        let records = try self.connectAndCollectLogs(serverBehaviour: .sendFrame(byteCount: 20_000), maxFrameSize: 1 << 14)

        let closed = try XCTUnwrap(records.first { $0.message == "Network connection closed" })
        XCTAssertEqual(closed.level, .warning)
        XCTAssertEqual(closed.metadata["mqtt_close_cause"], "error: invalidFrameLength")
        XCTAssertTrue(records.contains { $0.message == "MQTT channel error" && $0.level == .error && $0.metadata["mqtt_error"] == "invalidFrameLength" })
    }

    func testFrameUnderTheConfiguredLimitIsAccepted() throws {
        // Same 20 000 byte frame, accepted with a larger limit: the connect then fails by timing out
        // on the CONNACK, and no frame error is reported.
        let records = try self.connectAndCollectLogs(
            serverBehaviour: .sendFrame(byteCount: 20_000),
            maxFrameSize: 1 << 18,
            timeout: .milliseconds(500),
            waitForClose: false
        )

        XCTAssertFalse(records.contains { $0.metadata["mqtt_error"] == "invalidFrameLength" })
        XCTAssertFalse(records.contains { $0.metadata["mqtt_close_cause"] == "error: invalidFrameLength" })
    }

    func testWebSocketCloseFrameFromPeerIsLogged() throws {
        let records = try self.connectAndCollectLogs(serverBehaviour: .sendClose(code: .messageTooLarge, reason: "too big"))

        let closed = try XCTUnwrap(records.first { $0.message == "Network connection closed" })
        XCTAssertEqual(closed.level, .warning)
        XCTAssertEqual(closed.metadata["mqtt_close_cause"], "WebSocket closed by peer, code 1009, reason: too big")
    }

    func testConnectionDroppedByPeerIsLogged() throws {
        let records = try self.connectAndCollectLogs(serverBehaviour: .drop)

        let closed = try XCTUnwrap(records.first { $0.message == "Network connection closed" })
        XCTAssertEqual(closed.level, .warning)
        XCTAssertEqual(closed.metadata["mqtt_close_cause"], "closed by the peer or the network, no error and no WebSocket close frame")
    }

    // MARK: - Helpers

    /// Connects a client to a local WebSocket server that reacts to the CONNECT packet with
    /// `serverBehaviour`, and returns what the client logged once its connection closed.
    private func connectAndCollectLogs(
        serverBehaviour: ServerBehaviour,
        maxFrameSize: Int = 1 << 14,
        timeout: TimeAmount = .seconds(5),
        waitForClose: Bool = true
    ) throws -> [LogRecord] {
        let serverGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try serverGroup.syncShutdownGracefully()) }

        let serverChannel = try ServerBootstrap(group: serverGroup)
            .childChannelInitializer { channel in
                let upgrader = NIOWebSocketServerUpgrader(
                    maxFrameSize: 1 << 20,
                    shouldUpgrade: { channel, _ in channel.eventLoop.makeSucceededFuture(HTTPHeaders()) },
                    upgradePipelineHandler: { channel, _ in
                        channel.pipeline.addHandler(MisbehavingServerHandler(behaviour: serverBehaviour))
                    }
                )
                return channel.pipeline.configureHTTPServerPipeline(withServerUpgrade: (upgraders: [upgrader], completionHandler: { _ in }))
            }
            .bind(host: "127.0.0.1", port: 0)
            .wait()
        defer { XCTAssertNoThrow(try serverChannel.close().wait()) }

        let port = try XCTUnwrap(serverChannel.localAddress?.port)
        let clientGroup = NIOTSEventLoopGroup(loopCount: 1)
        defer { XCTAssertNoThrow(try clientGroup.syncShutdownGracefully()) }

        let collector = LogCollector()
        var logger = Logger(label: "MQTTCloseCauseLoggingTests") { _ in CollectingLogHandler(collector: collector) }
        logger.logLevel = .debug
        let client = MQTTClient(
            host: "127.0.0.1",
            port: port,
            identifier: "close-cause-logging",
            eventLoopGroupProvider: .shared(clientGroup),
            logger: logger,
            configuration: .init(
                version: .v5_0,
                timeout: timeout,
                webSocketConfiguration: .init(urlPath: "/mqtt", maxFrameSize: maxFrameSize)
            )
        )
        defer { XCTAssertNoThrow(try client.syncShutdownGracefully()) }

        // The server never answers with a CONNACK, so the connect always fails.
        XCTAssertThrowsError(try client.v5.connect().wait())

        // The close is logged from the channel's close future, which may complete after the connect fails.
        let deadline = Date().addingTimeInterval(waitForClose ? 5 : 0)
        while !collector.records.contains(where: { $0.message == "Network connection closed" }), Date() < deadline {
            Thread.sleep(forTimeInterval: 0.01)
        }
        return collector.records
    }
}

private enum ServerBehaviour {
    /// Sends one binary frame of `byteCount` bytes.
    case sendFrame(byteCount: Int)
    /// Sends a WebSocket close frame, then closes.
    case sendClose(code: WebSocketErrorCode, reason: String)
    /// Closes the connection without a word.
    case drop
}

/// Reacts to the first frame it receives (the client's CONNECT packet) with its behaviour.
private final class MisbehavingServerHandler: ChannelInboundHandler {
    typealias InboundIn = WebSocketFrame
    typealias OutboundOut = WebSocketFrame

    private let behaviour: ServerBehaviour
    private var reacted = false

    init(behaviour: ServerBehaviour) {
        self.behaviour = behaviour
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !self.reacted else { return }
        self.reacted = true

        switch self.behaviour {
        case .sendFrame(let byteCount):
            let buffer = context.channel.allocator.buffer(repeating: 0xFF, count: byteCount)
            context.writeAndFlush(self.wrapOutboundOut(WebSocketFrame(fin: true, opcode: .binary, data: buffer)), promise: nil)
        case .sendClose(let code, let reason):
            var buffer = context.channel.allocator.buffer(capacity: 2 + reason.utf8.count)
            buffer.write(webSocketErrorCode: code)
            buffer.writeString(reason)
            context.writeAndFlush(self.wrapOutboundOut(WebSocketFrame(fin: true, opcode: .connectionClose, data: buffer))).whenComplete { _ in
                context.close(promise: nil)
            }
        case .drop:
            context.close(promise: nil)
        }
    }
}

private struct LogRecord {
    let level: Logger.Level
    let message: String
    let metadata: [String: String]
}

private final class LogCollector: @unchecked Sendable {
    private let lock = NIOLock()
    private var storage: [LogRecord] = []

    var records: [LogRecord] {
        self.lock.withLock { self.storage }
    }

    func append(_ record: LogRecord) {
        self.lock.withLock { self.storage.append(record) }
    }
}

private struct CollectingLogHandler: LogHandler {
    let collector: LogCollector
    var metadata: Logger.Metadata = [:]
    var logLevel: Logger.Level = .debug

    subscript(metadataKey key: String) -> Logger.Metadata.Value? {
        get { self.metadata[key] }
        set { self.metadata[key] = newValue }
    }

    // swift-format-ignore
    func log(level: Logger.Level, message: Logger.Message, metadata: Logger.Metadata?, source: String, file: String, function: String, line: UInt) {
        let merged = self.metadata.merging(metadata ?? [:]) { _, new in new }
        self.collector.append(LogRecord(level: level, message: "\(message)", metadata: merged.mapValues { "\($0)" }))
    }
}
