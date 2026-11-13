//===----------------------------------------------------------------------===//
//
// This source file is part of the MQTTNIO project
//
// Copyright (c) 2020-2021 Adam Fowler
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Logging
import NIO
import NIOWebSocket

/// WebSocket channel handler. Sends WebSocket frames, receives and combines frames.
/// Code inspired from vapor/websocket-kit https://github.com/vapor/websocket-kit
/// and the WebSocket sample from swift-nio
/// https://github.com/apple/swift-nio/tree/main/Sources/NIOWebSocketClient
///
/// The WebSocket ping/pong is implemented but not used as the MQTT client already implements
/// PINGREQ messages
final class WebSocketHandler: ChannelDuplexHandler {
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = WebSocketFrame
    typealias InboundIn = WebSocketFrame
    typealias InboundOut = ByteBuffer

    static let pingData: String = "MQTTClient"

    var webSocketFrameSequence: WebSocketFrameSequence?
    var waitingOnPong: Bool = false
    var pingInterval: TimeAmount?
    /// Logs every frame when set at debug level.
    let logger: Logger?
    private var framesInMessage = 0
    private var frameCount = 0

    init(logger: Logger? = nil) {
        self.logger = logger
    }

    /// Write bytebuffer as WebSocket frame
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        guard context.channel.isActive else { return }

        let buffer = unwrapOutboundIn(data)
        self.send(context: context, buffer: buffer, opcode: .binary, fin: true, promise: promise)
    }

    /// Read WebSocket frame
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let frame = self.unwrapInboundIn(data)
        self.frameCount += 1
        self.framesInMessage += 1
        self.logger?.debug(
            "WebSocket frame in",
            metadata: [
                "ws_opcode": "\(frame.opcode)",
                "ws_fin": "\(frame.fin)",
                "ws_bytes": "\(frame.length)",
                "ws_frame": "\(self.frameCount)",
                "ws_frame_in_message": "\(self.framesInMessage)",
                "ws_message_bytes_so_far": "\((self.webSocketFrameSequence?.buffer.readableBytes ?? 0) + frame.length)",
                "ws_rsv": "\(frame.rsv1)\(frame.rsv2)\(frame.rsv3)",
            ]
        )

        switch frame.opcode {
        case .pong:
            self.pong(context: context, frame: frame)
        case .ping:
            self.ping(context: context, frame: frame)
        case .text:
            if var frameSeq = self.webSocketFrameSequence {
                frameSeq.append(frame)
                self.webSocketFrameSequence = frameSeq
            } else {
                var frameSeq = WebSocketFrameSequence(type: .text)
                frameSeq.append(frame)
                self.webSocketFrameSequence = frameSeq
            }
        case .binary:
            if var frameSeq = self.webSocketFrameSequence {
                frameSeq.append(frame)
                self.webSocketFrameSequence = frameSeq
            } else {
                var frameSeq = WebSocketFrameSequence(type: .binary)
                frameSeq.append(frame)
                self.webSocketFrameSequence = frameSeq
            }
        case .continuation:
            if var frameSeq = self.webSocketFrameSequence {
                frameSeq.append(frame)
                self.webSocketFrameSequence = frameSeq
            } else {
                self.logger?.error("WebSocket: continuation frame with no message in progress")
                self.close(context: context, code: .protocolError, promise: nil)
            }
        case .connectionClose:
            self.receivedClose(context: context, frame: frame)
        default:
            break
        }

        if let frameSeq = self.webSocketFrameSequence, frame.fin {
            self.logger?.debug(
                "WebSocket message complete",
                metadata: ["ws_message_bytes": "\(frameSeq.buffer.readableBytes)", "ws_frames": "\(self.framesInMessage)", "ws_type": "\(frameSeq.type)"]
            )
            self.framesInMessage = 0
            switch frameSeq.type {
            case .binary, .text:
                context.fireChannelRead(wrapInboundOut(frameSeq.buffer))
            default: break
            }
            self.webSocketFrameSequence = nil
        }
    }

    /// Send web socket frame to server
    private func send(
        context: ChannelHandlerContext,
        buffer: ByteBuffer,
        opcode: WebSocketOpcode,
        fin: Bool = true,
        promise: EventLoopPromise<Void>? = nil
    ) {
        let maskKey = self.makeMaskKey()
        let frame = WebSocketFrame(fin: fin, opcode: opcode, maskKey: maskKey, data: buffer)
        context.writeAndFlush(wrapOutboundOut(frame), promise: promise)
    }

    /// Send ping and setup task to check for pong and send new ping
    private func sendPingAndWait(context: ChannelHandlerContext) {
        guard context.channel.isActive, let pingInterval else {
            return
        }
        if self.waitingOnPong {
            // We never received a pong from our last ping, so the connection has timed out
            self.logger?.error("WebSocket: no pong received, closing")
            let promise = context.eventLoop.makePromise(of: Void.self)
            self.close(context: context, code: .unknown(1006), promise: promise)
            promise.futureResult.whenComplete { _ in
                // Usually, closing a WebSocket is done by sending the close frame and waiting
                // for the peer to respond with their close frame. We are in a timeout situation,
                // so the other side likely will never send the close frame. We just close the
                // channel ourselves.
                context.channel.close(mode: .all, promise: nil)
            }

        } else {
            let buffer = context.channel.allocator.buffer(string: Self.pingData)
            self.send(context: context, buffer: buffer, opcode: .ping)
            _ = context.eventLoop.scheduleTask(in: pingInterval) {
                self.sendPingAndWait(context: context)
            }
        }
    }

    /// Respond to pong from server. Verify contents of pong and clear waitingOnPong flag
    private func pong(context: ChannelHandlerContext, frame: WebSocketFrame) {
        var frameData = frame.unmaskedData
        guard let frameDataString = frameData.readString(length: Self.pingData.count),
            frameDataString == Self.pingData
        else {
            self.logger?.error("WebSocket: unexpected pong payload, closing")
            self.close(context: context, code: .goingAway, promise: nil)
            return
        }
        self.waitingOnPong = false
    }

    /// Respond to ping from server
    private func ping(context: ChannelHandlerContext, frame: WebSocketFrame) {
        if frame.fin {
            self.send(context: context, buffer: frame.unmaskedData, opcode: .pong, fin: true, promise: nil)
        } else {
            self.logger?.error("WebSocket: fragmented ping, closing")
            self.close(context: context, code: .protocolError, promise: nil)
        }
    }

    private func receivedClose(context: ChannelHandlerContext, frame: WebSocketFrame) {
        // Handle a received close frame. We're just going to close.
        // Tell the rest of the pipeline who closed and why: the code is the only hint a server or
        // a proxy gives (1009 "message too big", 1008 "policy violation", ...).
        var data = frame.unmaskedData
        let code = data.readWebSocketErrorCode()
        let reason = data.readString(length: data.readableBytes) ?? ""
        self.logger?.warning(
            "WebSocket: close frame received",
            metadata: ["ws_close_code": "\(code.map { "\(UInt16(webSocketErrorCode: $0))" } ?? "none")", "ws_close_reason": "\(reason)"]
        )
        context.fireUserInboundEventTriggered(WebSocketPeerClose(code: code, reason: reason))
        self.isClosed = true
        context.close(promise: nil)
    }

    /// Make mask key to be used in WebSocket frame
    func makeMaskKey() -> WebSocketMaskingKey? {
        let bytes: [UInt8] = (0...3).map { _ in UInt8.random(in: 1...255) }
        return WebSocketMaskingKey(bytes)
    }

    /// Close websocket connection
    public func close(context: ChannelHandlerContext, code: WebSocketErrorCode = .goingAway, promise: EventLoopPromise<Void>?) {
        guard self.isClosed == false else {
            promise?.succeed(())
            return
        }
        self.isClosed = true
        // 1006 is the bookkeeping close of a channel that already went down, not a decision to close.
        self.logger?.log(
            level: UInt16(webSocketErrorCode: code) == 1006 ? .debug : .warning,
            "WebSocket: sending close frame",
            metadata: ["ws_close_code": "\(UInt16(webSocketErrorCode: code))", "ws_channel_active": "\(context.channel.isActive)"]
        )

        let codeAsInt = UInt16(webSocketErrorCode: code)
        let codeToSend: WebSocketErrorCode
        if codeAsInt == 1005 || codeAsInt == 1006 {
            /// Code 1005 and 1006 are used to report errors to the application, but must never be sent over
            /// the wire (per https://tools.ietf.org/html/rfc6455#section-7.4)
            codeToSend = .normalClosure
        } else {
            codeToSend = code
        }

        var buffer = context.channel.allocator.buffer(capacity: 2)
        buffer.write(webSocketErrorCode: codeToSend)
        self.send(context: context, buffer: buffer, opcode: .connectionClose, fin: true, promise: promise)
    }

    func channelInactive(context: ChannelHandlerContext) {
        self.logger?.debug(
            "WebSocket: channel inactive",
            metadata: [
                "ws_frames": "\(self.frameCount)",
                "ws_message_in_progress_bytes": "\(self.webSocketFrameSequence?.buffer.readableBytes ?? 0)",
                "ws_already_closed": "\(self.isClosed)",
            ]
        )
        self.close(context: context, code: .unknown(1006), promise: nil)

        // We always forward the error on to let others see it.
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        let errorCode: WebSocketErrorCode
        if let error = error as? NIOWebSocketError {
            errorCode = WebSocketErrorCode(error)
        } else {
            errorCode = .unexpectedServerError
        }
        self.logger?.error("WebSocket: error caught", metadata: ["mqtt_error": "\(error)"])
        self.close(context: context, code: errorCode, promise: nil)

        // We always forward the error on to let others see it.
        context.fireErrorCaught(error)
    }

    private var isClosed: Bool = false
}

/// Close frame received from the WebSocket peer.
struct WebSocketPeerClose {
    let code: WebSocketErrorCode?
    let reason: String
}

struct WebSocketFrameSequence {
    var buffer: ByteBuffer
    var type: WebSocketOpcode

    init(type: WebSocketOpcode) {
        self.buffer = ByteBufferAllocator().buffer(capacity: 0)
        self.type = type
    }

    mutating func append(_ frame: WebSocketFrame) {
        var data = frame.unmaskedData
        switch self.type {
        case .binary, .text:
            self.buffer.writeBuffer(&data)
        default: break
        }
    }
}

extension WebSocketErrorCode {
    init(_ error: NIOWebSocketError) {
        switch error {
        case .invalidFrameLength:
            self = .messageTooLarge
        case .fragmentedControlFrame,
            .multiByteControlFrameLength:
            self = .protocolError
        }
    }
}
