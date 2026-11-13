# Fork notes

This repository is a fork of
[swift-server-community/mqtt-nio](https://github.com/swift-server-community/mqtt-nio)
carrying a small patch series.

## Branches

- `main` **mirrors upstream exactly.** Never commit to it. Sync it with GitHub's "Sync fork" button or
  `git fetch upstream && git push origin upstream/main:main`.
- `patches` **carries the patch series**, on top of the upstream release the consumers use. Upstream
  `main` is the 3.x line while the consumers are still on 2.x, so `patches` sits on the `2.13.0` tag
  and is not rebased onto `main`. Moving to a newer upstream release is
  `git rebase --onto <new tag> <old tag> patches && git push --force-with-lease origin patches`.
  Each commit is one self-contained change; a patch upstream absorbs is dropped from the series.

## Tags

Releases are tagged on `patches` as `<upstream version>-fork.<n>`, for example `2.13.0-fork.2`:
mqtt-nio 2.13.0 plus revision 2 of the patch series. The suffix keeps fork tags from ever being
mistaken for upstream's. Consumers pin with `exact:`, since every fork release is a deliberate change.

## Current patch series

1. Publish errors keep the acknowledgement: a failed publish throws `reasonErrorWithAck(MQTTAckV5)`
   instead of `reasonError`, so the user properties of an error PUBACK reach the caller.
   (`2.13.0-fork.1`)
2. Channel errors are logged: the last handler of the pipeline logs every error it catches (a WebSocket
   frame over the size limit, a packet that cannot be decoded, a DISCONNECT from the server). The close
   listeners only get the channel's close future, which succeeds whatever the cause. (`2.13.0-fork.2`)
3. The cause of a closed connection is logged: "Network connection closed" carries `mqtt_close_cause`
   (an error caught on the channel, a WebSocket close frame from the peer with its code and reason, a
   drop by the peer or the network, or a close asked by the client) and is a warning unless the
   client asked for the close. (`2.13.0-fork.3`)
