# gst-dynamic-sink

A GStreamer-based Rust application which minimally reproduces an issue in an
application which dynamically adds and removes sinks from a GStreamer pipeline.

This example application adds and removes a 'snapshot' consumer sink which
grabs a single frame from a pipeline, PNG-encodes it, and returns it via a
channel.

Running the application will reproduce the pipeline stall after a few or more
snapshots are generated. CTRL-C should shutdown the application and dump debug
pipeline to `debug.dot`.
