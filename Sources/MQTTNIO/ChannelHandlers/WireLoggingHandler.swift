//===----------------------------------------------------------------------===//
//
// This source file is part of the MQTTNIO project
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Logging
import NIO

/// First handler of the pipeline: logs what crosses the socket once TLS is removed, and every
/// channel event, so a dropped connection can be traced to what was read, written or asked just
/// before. Installed only when the client's logger is at debug level or below.
final class WireLoggingHandler: ChannelDuplexHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer
    typealias OutboundIn = IOData
    typealias OutboundOut = IOData

    private let logger: Logger
    private var bytesIn = 0
    private var bytesOut = 0
    private var readCount = 0
    private var writeCount = 0

    init(logger: Logger) {
        self.logger = logger
    }

    func handlerAdded(context: ChannelHandlerContext) {
        self.logger.debug("Wire: handler added", metadata: self.metadata(context, ["wire_active": "\(context.channel.isActive)"]))
    }

    func channelActive(context: ChannelHandlerContext) {
        self.logger.debug("Wire: channel active", metadata: self.metadata(context))
        context.fireChannelActive()
    }

    func channelInactive(context: ChannelHandlerContext) {
        self.logger.debug("Wire: channel inactive", metadata: self.metadata(context))
        context.fireChannelInactive()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = self.unwrapInboundIn(data)
        self.readCount += 1
        self.bytesIn += buffer.readableBytes
        self.logger.debug(
            "Wire in",
            metadata: [
                "wire_bytes": "\(buffer.readableBytes)",
                "wire_read": "\(self.readCount)",
                "wire_total_in": "\(self.bytesIn)",
                "wire_head": "\(Self.hexHead(buffer))",
            ]
        )
        context.fireChannelRead(data)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        self.logger.trace("Wire: read complete", metadata: ["wire_total_in": "\(self.bytesIn)"])
        context.fireChannelReadComplete()
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        self.logger.debug("Wire: writability changed", metadata: ["wire_writable": "\(context.channel.isWritable)"])
        context.fireChannelWritabilityChanged()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        self.logger.debug("Wire: inbound event", metadata: self.metadata(context, ["wire_event": "\(event)"]))
        context.fireUserInboundEventTriggered(event)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        self.logger.error("Wire: error caught", metadata: self.metadata(context, ["mqtt_error": "\(error)"]))
        context.fireErrorCaught(error)
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let bytes = self.unwrapOutboundIn(data).readableBytes
        self.writeCount += 1
        self.bytesOut += bytes
        let writeNumber = self.writeCount
        self.logger.debug(
            "Wire out",
            metadata: ["wire_bytes": "\(bytes)", "wire_write": "\(writeNumber)", "wire_total_out": "\(self.bytesOut)"]
        )
        let promise = promise ?? context.eventLoop.makePromise(of: Void.self)
        let logger = self.logger
        promise.futureResult.whenFailure { error in
            logger.error("Wire: write failed", metadata: ["wire_write": "\(writeNumber)", "mqtt_error": "\(error)"])
        }
        context.write(data, promise: promise)
    }

    func close(context: ChannelHandlerContext, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        // An outbound close reaching the head of the pipeline was asked by the client side: one of
        // its handlers, or the application. A drop by the peer shows as "channel inactive" alone.
        self.logger.warning("Wire: close asked by the client side", metadata: self.metadata(context, ["wire_close_mode": "\(mode)"]))
        context.close(mode: mode, promise: promise)
    }

    private func metadata(_ context: ChannelHandlerContext, _ extra: Logger.Metadata = [:]) -> Logger.Metadata {
        var metadata: Logger.Metadata = [
            "wire_total_in": "\(self.bytesIn)",
            "wire_total_out": "\(self.bytesOut)",
            "wire_reads": "\(self.readCount)",
            "wire_writes": "\(self.writeCount)",
        ]
        if let remote = context.channel.remoteAddress {
            metadata["wire_remote"] = "\(remote)"
        }
        if let local = context.channel.localAddress {
            metadata["wire_local"] = "\(local)"
        }
        return metadata.merging(extra) { _, new in new }
    }

    /// First bytes of a read, in hexadecimal: enough to recognise a WebSocket frame header.
    private static func hexHead(_ buffer: ByteBuffer) -> String {
        let bytes = buffer.getBytes(at: buffer.readerIndex, length: min(buffer.readableBytes, 12)) ?? []
        return bytes.map { byte in
            let hex = String(byte, radix: 16)
            return hex.count == 1 ? "0" + hex : hex
        }.joined(separator: " ")
    }
}
