# Your Kafka. In context. — descriptive transcript

This 36-second introduction uses recordings of StreamSkope connected to the local
AIO Kafka broker. Light and dark versions share the edit and original 120 BPM
electronic score. Music is instrumental; there is no spoken information.

## 0–6 seconds — From one record to its context

Two lines of the actual record show `orderId: ord-1042` and `status: accepted`.
The view opens to the inspector, then the complete workbench. Titles read
**It starts with one record.** and **Understanding takes context.**

## 6–14 seconds — Find what matters

The recorded filter-and-select interaction plays at its original speed: the key
filter is opened, `ord-1042` is entered and the matching record is selected.
A quick camera move finishes on its payload.

## 14–19 seconds — Read the details

The actual JSON payload is followed by the Metadata tab with topic, timestamp,
partition, offset and headers. The record belongs to `orders.events`, partition 0,
offset 42.

## 19–24 seconds — See where consumers stand

The `orders-workers` consumer group appears, followed by its offsets:
committed 42, end 49, lag 7. This local group has no active members.
Lag describes a Kafka offset gap; it does not prove completion of business effects.

## 24–30 seconds — One workbench

Two-second cuts visit connection profiles, topic inventory and the message workspace.

## 30–36 seconds — Your Kafka. In context.

The view returns to the opening record, then resolves to the StreamSkope mark:
**Your Kafka. In context.** and **Explore the preview.**

StreamSkope is Apache-2.0 software.
[Try it with local Kafka](../start/quickstart.md) or
[read the installation instructions](../start/installation.md).
