The main application task (spawned via call to `run_pipeline`) is responding to
messages from two sources: the GStreamer pipeline bus messages, and a request
channel which contains `Request` messages.

When a `Snapshot` request is received by the task, it creates a new `Bin` via
`snapshot_bin()` and links it to the raw tee via `link_tee()`. The snapshot bin
internally has an `appsink` which waits for a single PNG-encoded frame, and
sends it back to the original requestor via a oneshot channel that is threaded
through this entire flow. It then begins the process of destroying itself by
calling `unlink_tee`, which unlinks it from the raw tee, and sends an EoS
message to the bin. The pipeline is configured with `message-forwarding=true`,
so eventually this results in an `Element(EoS)` message being received by the
main application task, which then completes the cleanup by removing the bin
from the pipeline. I believe that this two-step cleanup process isn't strictly
necessary for this application which is just dealing with a single frame,
however it's critical if the consumer were, say, an MP4 muxer + filesink, where
properly flushing the muxer and filesink is necessary.

When the pipeline appears to stall, I've noticed two things:

    1. The debug .dot of the pipeline shows the `videotestsrc` element as
       having a paused task via `[t]` instead of the usual `[T]`.
    2. The appsink never receives a frame. When it receives the frame, it logs
       a message containing `"received snapshot sample"`, but the logs stall
       just before this would otherwise happen.
