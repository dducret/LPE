---
type: Rust Function
title: send_state_change_event
resource: crates/lpe-jmap/src/eventsource.rs#L152-L168
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source
---

# Signature

`async fn send_state_change_event( sender: &mpsc::Sender<std::result::Result<Event, Infallible>>, changed: HashMap<String, HashMap<String, String>>, push_state: String, ) -> Result<()>`

# Called by

- [handle_event_source](../../../../../functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source.md)