//! An example of a GStreamer pipeline which has sinks dynamically added and removed.

use std::collections::HashSet;
use std::pin::pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{bail, ensure, Context, Error, Result};
use bytes::Bytes;
use futures::StreamExt;
use gst::prelude::*;
use tokio::{
    select, signal,
    sync::{mpsc, oneshot},
    time::sleep,
};
use tracing::{debug, error, field, info, trace, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    // Initialize GStreamer
    gst::init()?;

    let pipeline = Pipeline::videotestsrc().with_context(|| format!("failed to start pipeline"))?;

    let mut ctrl_c = pin!(signal::ctrl_c());
    loop {
        tokio::select! {
            // Take a snapshot. This will add a consumer to the raw tee, grab a PNG encoded frame
            // from it, then remove the consumer. The frame is sent back to this context via a
            // channel.
            snapshot = pipeline.snapshot() => {
                info!(len = snapshot?.len(), "snapshot received");

                // Adding some artificial delay between snapshot requests seems to make the
                // pipeline stall bug more reproducible.
                sleep(Duration::from_secs(1)).await;
            }
            _ = &mut ctrl_c => break,
        }
    }

    let shutdown = async move {
        info!("writing debug pipeline to debug.dot & shutting down pipeline");

        let debug = pipeline.debug_dot().await?;
        tokio::fs::write("debug.dot", debug).await?;
        pipeline.shutdown().await;

        Ok(())
    };

    tokio::select! {
        result = shutdown => result,
        _ = signal::ctrl_c() => {
            bail!("second CTRL-C; exiting now!")
        }
    }
}

/// A gstreamer pipeline, which allows video stream consumers to be dynamically added and removed from a
/// video source.
#[derive(Clone, Debug)]
struct Pipeline {
    send: mpsc::Sender<Request>,
}

/// A request message sent to the task which controls the GStreamer pipeline.
#[derive(Debug)]
enum Request {
    Snapshot { resp: oneshot::Sender<Bytes> },
    Debug { resp: oneshot::Sender<String> },
    Shutdown,
}

impl Pipeline {
    fn videotestsrc() -> Result<Pipeline> {
        let source = gst::ElementFactory::make("videotestsrc")
            .name("source")
            .build()?;

        Self::with_source(source)
    }

    /// Creates a new `Pipeline` which uses the provided video source.
    fn with_source(source: gst::Element) -> Result<Pipeline> {
        let (pipeline, raw_tee) = create_pipeline(source)?;
        pipeline.set_state(gst::State::Playing)?;

        let (send, mut recv_request) = mpsc::channel(1);

        tokio::spawn(async move {
            if let Err(error) = run_pipeline(&pipeline, raw_tee, &mut recv_request).await {
                error!(
                    error = format!("{error:#}"),
                    backtrace = %error.backtrace(),
                    "unrecoverable pipeline error",
                );
            }

            if let Err(error) = pipeline
                .call_async_future(|pipeline| pipeline.set_state(gst::State::Null))
                .await
            {
                error!(
                    error = format!("{error:#}"),
                    "failed to cleanly shut down gstreamer pipeline",
                );
            }
        });

        Ok(Self { send })
    }

    /// Returns a PNG-encoded image of the latest video frame.
    async fn snapshot(&self) -> Result<Bytes> {
        let (resp, recv) = oneshot::channel();

        self.send
            .send(Request::Snapshot { resp })
            .await
            .map_err(Error::from)
            .context("pipeline is shutdown")?;

        Ok(recv.await?)
    }

    /// Returns a graphviz DOT encoded descriptor of the gstreamer pipeline.
    async fn debug_dot(&self) -> Result<String> {
        let (resp, recv) = oneshot::channel();
        self.send
            .send(Request::Debug { resp })
            .await
            .map_err(Error::from)
            .context("pipeline is shutdown")?;

        Ok(recv.await?)
    }

    /// Stops the pipeline, including all in progress consumers.
    async fn shutdown(self) {
        let _ = self.send.send(Request::Shutdown).await;

        self.send.closed().await
    }
}

/// Creates a new pipeline, as well as a tee containing raw video which can be used to add new
/// sinks to the pipeline (see `link_tee` and `unlink_tee`).
fn create_pipeline(source: gst::Element) -> Result<(gst::Pipeline, gst::Element)> {
    let pipeline = gst::Pipeline::builder()
        // Configure the pipeline to forward EOS message from sinks immediately and not hold it
        // back until it also got an EOS message from all other sinks. See `unlink_tee()` for why
        // this is necessary.
        .message_forward(true)
        .build();

    let decode_bin = gst::ElementFactory::make("decodebin")
        .name("capture-decode")
        .build()?;

    let video_convert = gst::ElementFactory::make("videoconvert").build()?;

    let raw_filter = gst::ElementFactory::make("capsfilter")
        .property(
            "caps",
            gst_video::VideoCapsBuilder::for_encoding("video/x-raw")
                .format(gst_video::VideoFormat::Rgb)
                .build(),
        )
        .build()?;

    let identity = gst::ElementFactory::make("identity")
        .property("drop-allocation", true)
        .build()?;

    let raw_tee = gst::ElementFactory::make("tee")
        .name("raw-tee")
        .property("allow-not-linked", true)
        .build()?;

    pipeline.add_many([
        &source,
        &decode_bin,
        &video_convert,
        &raw_filter,
        &identity,
        &raw_tee,
    ])?;
    gst::Element::link_many([&source, &decode_bin])?;
    gst::Element::link_many([&video_convert, &raw_filter, &identity, &raw_tee])?;

    // Connect to decodebin's pad-added signal, that is emitted whenever
    // it found another stream from the input file and found a way to decode it to its raw format.
    // decodebin automatically adds a src-pad for this raw stream, which
    // we can use to build the follow-up pipeline.
    decode_bin.connect_pad_added(move |decode_bin, _src_pad| {
        if let Err(error) = decode_bin.link(&video_convert) {
            error!(error = format!("{error:#}"), "failed to link decodebin",);
        }
    });

    Ok((pipeline, raw_tee))
}

/// Returns a future which when polled will drive a pipeline.
async fn run_pipeline(
    pipeline: &gst::Pipeline,
    raw_tee: gst::Element,
    recv_request: &mut mpsc::Receiver<Request>,
) -> Result<()> {
    let Some(bus) = pipeline.bus() else {
        bail!("failed to retrieve pipeline bus");
    };

    let mut messages = bus.stream();

    let mut flushing_bins = HashSet::new();

    let handle_pipeline_bus_message = |flushing_bins: &mut HashSet<gst::Bin>,
                                       message: Option<gst::Message>|
     -> Result<()> {
        let Some(message) = message else {
            bail!("pipeline bus closed");
        };

        match message.view() {
            gst::MessageView::Error(err) => {
                if let Some(debug) = err.debug() {
                    // The debug string is more informative than the wrapped error.
                    bail!(debug)
                } else {
                    bail!(err.error())
                }
            }
            gst::MessageView::Element(element) => {
                debug!(
                    source = element.src().map(gst::Object::name).map(field::debug),
                    structure = element.message().structure().map(field::debug),
                    "element",
                );
                if let Some(bin) = element
                    .structure()
                    .and_then(|structure| structure.get("message").ok())
                    .filter(|message: &&gst::Message| message.type_() == gst::MessageType::Eos)
                    .and_then(|message: &gst::Message| message.src())
                    .and_then(|object| object.downcast_ref::<gst::Bin>())
                {
                    debug!(bin=?bin.name(), "removing flushed bin");
                    // Step 2 of dynamically removing a video consumer tee branch. At this point
                    // the consumer (`bin`) has been unlinked and flushed, and now only needs to be
                    // removed from the pipeline.
                    assert!(flushing_bins.remove(bin));

                    bin.call_async(move |bin| {
                        if let Err(error) = bin.set_state(gst::State::Null) {
                            error!(
                                consumer=?bin.name(),
                                error = format!("{error:#}"),
                                "failed to cleanly shut down consumer",
                            );
                        }
                    });
                    pipeline.remove(bin)?;
                }
            }
            gst::MessageView::Latency(latency) => {
                let element = latency.src().map(gst::Object::name).map(field::debug);
                trace!(element, "recalculating latency");
                // TODO: this appears to cause a pipeline stall ocassionally! What does it do? What
                // is it for?
                /*
                if pipeline.recalculate_latency().is_err() {
                    warn!(element, "failed to recalculate latency");
                }
                */
            }
            gst::MessageView::StateChanged(state_changed) => {
                debug!(
                    src = state_changed.src().map(gst::Object::name).map(field::debug),
                    old = ?state_changed.old(),
                    current = ?state_changed.current(),
                    pending = Some(state_changed.pending()).filter(|pending| pending != &gst::State::VoidPending).map(field::debug),
                    "state changed",
                );
            }
            gst::MessageView::AsyncStart(async_start) => {
                debug!(
                    src = async_start.src().map(gst::Object::name).map(field::debug),
                    structure = ?async_start.structure(),
                    "async start",
                );
            }
            gst::MessageView::AsyncDone(async_done) => {
                debug!(
                    src = async_done.src().map(gst::Object::name).map(field::debug),
                    structure = ?async_done.structure(),
                    "async done",
                );
            }
            gst::MessageView::Qos(qos) => {
                let (live, running_time, stream_time, timestamp, duration) = qos.get();
                let (jitter, proportion, quality) = qos.values();
                let (processed, dropped) = qos.stats();

                warn!(
                    source_class = qos.src().map(|obj| obj.type_()).map(field::display),
                    source_name = qos.src().map(|obj| obj.name()).map(field::display),
                    live,
                    running_time = running_time.map(field::display),
                    stream_time = stream_time.map(field::display),
                    timestamp = timestamp.map(field::display),
                    duration = duration.map(field::display),
                    jitter,
                    proportion,
                    quality,
                    %processed,
                    %dropped,
                    "QoS",
                );
            }
            message => debug!(?message),
        };

        Ok(())
    };

    let mut is_started = false;

    loop {
        let pipeline_state = pipeline.state(gst::ClockTime::default());
        let is_playing = pipeline_state.1 == gst::State::Playing;

        // Once the pipeline reaches playing state, it should stay playing since sinks are
        // configured with async=false.
        ensure!(
            !is_started || is_playing,
            "pipeline no longer playing: {pipeline_state:?}"
        );
        is_started |= is_playing;

        select! {
            message = messages.next() => handle_pipeline_bus_message(&mut flushing_bins, message)?,

            request = recv_request.recv() => {
                let Some(request) = request else {
                    break;
                };

                debug!(?request, "handling request");
                match request {
                    Request::Snapshot { resp } => {
                        let bin = snapshot_bin(resp)?;
                        info!(consumer=?bin.name(), "taking snapshot");
                        link_tee(pipeline, &raw_tee, &bin)?;
                        flushing_bins.insert(bin);
                        continue;
                    }
                    Request::Debug { resp } => {
                        let _ = resp.send(
                            pipeline.debug_to_dot_data(gst::DebugGraphDetails::all()).to_string()
                        );
                        continue;
                    },
                    Request::Shutdown => break,
                };
            }
        }
    }

    info!(
        flushing_consumers = flushing_bins.len(),
        "shutting down pipeline",
    );

    // Wait for all flushing consumers to be finalized.
    while !flushing_bins.is_empty() {
        handle_pipeline_bus_message(&mut flushing_bins, messages.next().await)?;
    }

    Ok(())
}

/// Returns a GStreamer Bin which, when attached to the raw tee, grabs a single PNG-encoded frame
/// from the pipeline, returns it to `resp`, and removes itself from the pipeline.
fn snapshot_bin(resp: oneshot::Sender<Bytes>) -> Result<gst::Bin> {
    let bin = gst::Bin::with_name(&format!("[{}] snapshot", unique_consumer_idx()));

    let queue = gst::ElementFactory::make("queue")
        .name("snapshot-queue")
        // Since we only need 1 frame, configure the queue to drop subsequent buffers instead of
        // blocking.
        .property_from_str("leaky", "1")
        .property("max-size-buffers", 1u32)
        .property("flush-on-eos", true)
        .build()?;

    let png_encoder = gst::ElementFactory::make("pngenc")
        .name("png-enc")
        .property("snapshot", true)
        .build()?;

    let app_sink = gst::ElementFactory::make("appsink")
        .name("app-sink")
        .property("async", false)
        .property("sync", false)
        .property("wait-on-eos", false)
        .property("enable-last-sample", false)
        .build()?;

    bin.add_many([&queue, &png_encoder, &app_sink])?;

    gst::Element::link_many([&queue, &png_encoder, &app_sink])?;

    let Some(queue_sink_pad) = queue.static_pad("sink") else {
        bail!("failed to get queue sink pad");
    };

    let ghost_pad = gst::GhostPad::with_target(&queue_sink_pad)?;
    bin.add_pad(&ghost_pad)?;

    let app_sink = app_sink.downcast::<gst_app::AppSink>().unwrap();

    let mut resp = Some(resp);
    app_sink.set_callbacks(
        gst_app::AppSinkCallbacks::builder()
            // Add a handler to the "new-sample" signal.
            .new_sample(move |app_sink| {
                debug!(
                    consumer = app_sink
                        .parent()
                        .and_downcast::<gst::Element>()
                        .as_ref()
                        .map(gst::Element::name)
                        .map(field::debug),
                    is_first = resp.is_some(),
                    "received snapshot sample",
                );

                // Make sure that we only handle a single buffer
                let Some(resp) = resp.take() else {
                    return Ok(gst::FlowSuccess::Ok);
                };

                let inner = || -> Result<()> {
                    // Pull the sample in question out of the appsink's buffer.
                    let sample = app_sink.pull_sample()?;
                    let Some(buffer) = sample.buffer() else {
                        bail!("snapshot sample must contain buffer");
                    };

                    let _ = resp.send(Bytes::copy_from_slice(&buffer.map_readable()?));
                    Ok(())
                };

                let bin = app_sink.parent().and_downcast::<gst::Bin>();

                if let Err(error) = inner() {
                    warn!(
                        consumer = bin.map(|bin| bin.name()).map(field::debug),
                        error = format!("{error:#}"),
                        "snapshot failed"
                    );
                    return Err(gst::FlowError::Error);
                }

                let Some(bin) = bin else {
                    warn!(
                        consumer = bin.map(|bin| bin.name()).map(field::debug),
                        "snapshot failed"
                    );
                    return Err(gst::FlowError::Error);
                };

                unlink_tee(bin).unwrap();

                Ok(gst::FlowSuccess::Ok)
            })
            .build(),
    );

    Ok(bin)
}

/// Adds `bin` as a new leg to `tee`. `bin` will be added and state-synchronized with `pipeline`.
fn link_tee(pipeline: &gst::Pipeline, tee: &gst::Element, bin: &gst::Bin) -> Result<()> {
    pipeline.add(bin)?;

    let Some(src_pad) = tee.request_pad_simple("src_%u") else {
        bail!("failed to request tee source pad");
    };
    let Some(sink_pad) = bin.static_pad("sink") else {
        bail!("failed to request bin sink pad");
    };

    debug!(
        pipeline = ?pipeline.name(),
        pipeline_state = ?pipeline.state(gst::ClockTime::default()),
        tee = ?tee.name(),
        bin = ?bin.name(),
        src_pad = ?src_pad.name(),
        sink_pad = ?sink_pad.name(),
        "linking bin to tee"
    );
    src_pad.link(&sink_pad).with_context(|| {
        format!(
            "failed to link tee consumer; src_caps: {:?}, sink_caps: {:?}",
            src_pad.caps(),
            sink_pad.caps()
        )
    })?;

    bin.sync_state_with_parent()?;

    // Send an event to tell all upstream producers of the tee to flush keyframes. Sinks with
    // muxers typically can only start working upon receiving the first keyframe, so this reduces
    // the latency to having a fully functional sink.
    let force_keyframe_event: gst::Event =
        gst::event::CustomUpstream::builder(gst::Structure::builder("GstForceKeyUnit").build())
            .build();
    src_pad.send_event(force_keyframe_event);

    Ok(())
}

/// Step 1 of stopping and removing a video consumer tee branch (`bin`).
///
/// This step unlinks the tee branch, effectively cutting off it's input stream of frames. Next,
/// buffers/muxers/encoders/etc internal to the tee branch are flushed by sending the end-of-stream
/// (EOS) signal.
///
/// Asynchronously, the pipeline event listener will see the EOS event after it's been passed
/// through the entire tee consumer to the sink, and at that point the `bin` will be removed from
/// the pipeline.
///
/// See the following resources for more discussion & examples of dynamic pipeline reconfiguration:
///
/// [1]: https://gitlab.freedesktop.org/-/snippets/1760
/// [2]: https://gstreamer.freedesktop.org/documentation/tutorials/basic/dynamic-pipelines.html
/// [3]: https://github.com/MaZderMind/dynamic-gstreamer-pipelines-cookbook
/// [4]: https://coaxion.net/blog/2014/01/gstreamer-dynamic-pipelines/
fn unlink_tee(bin: gst::Bin) -> Result<()> {
    fn inner(
        tee: &gst::Element,
        tee_pad: &gst::Pad,
        bin: &gst::Bin,
        bin_pad: &gst::Pad,
    ) -> Result<()> {
        debug!(
            tee = ?tee.name(),
            branch = ?bin.name(),
            "unlinking tee branch",
        );
        tee_pad.unlink(bin_pad)?;
        tee.release_request_pad(tee_pad);

        bin.call_async(|bin| {
            bin.send_event(gst::event::Eos::new());
        });

        Ok(())
    }

    let is_shutting_down = AtomicBool::new(false);

    let Some(bin_pad) = bin.static_pad("sink") else {
        bail!("failed to request bin sink pad");
    };
    let Some(tee_pad) = bin_pad.peer() else {
        bail!("failed to request tee pad");
    };
    let Some(tee) = tee_pad.parent_element() else {
        bail!("failed to request tee pad parent element");
    };

    // Register a probe that fires when the bin is idle. The associated callback will do the actual
    // unlinking.
    tee_pad.add_probe(
        gst::PadProbeType::IDLE,
        move |tee_pad: &gst::Pad, info: &mut gst::PadProbeInfo| {
            if is_shutting_down.fetch_or(true, Ordering::SeqCst) {
                warn!("pad probe already called!")
            } else {
                if let Err(error) = inner(&tee, tee_pad, &bin, &bin_pad) {
                    warn!(
                        tee = ?tee.name(),
                        bin = ?bin.name(),
                        error = format!("{error:#}"),
                        probe_info = ?info,
                        "failed to remove tee branch",
                    );
                };
            }

            gst::PadProbeReturn::Remove
        },
    );

    Ok(())
}

/// Element names within a pipeline must be unique. This function allocates a new unique index
/// which can be mixed into new pipeline consumers to ensure uniqueness.
fn unique_consumer_idx() -> usize {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}
