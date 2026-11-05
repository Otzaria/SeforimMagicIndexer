# Bounded line processing

`LinesProcessor` reads source ranges of 256 lines and creates LLM batches of five
eligible lines. A batch can span source pages. Blank content, content that becomes
blank after the existing HTML cleanup, and previously processed line IDs retain
their previous filtering behavior. `SeforimRepository.getLines` uses an inclusive
end index; consecutive pages therefore use `0..255`, `256..511`, and so on.

The producer checks processed IDs once per nonempty page with an `IN` query of at
most 256 IDs. It does not load all processed IDs or all source lines for a book.
Source IDs remain the resume key, with no schema or ID migration.

## Backpressure and ordering

The configured request concurrency still defaults to one and is clamped to the
existing range of 1–250. There are exactly that many request workers and one
producer. Both the work queue and the ordered completion queue have capacity
equal to the concurrency. The completion queue is entered before work is
submitted, so a slow first request also bounds completed results waiting behind
it. At most `concurrency + 2` batches have been prepared but not consumed: the
current consumer item, the bounded completion queue, and one blocked producer
item. A full source page and the current batch are retained alongside this work.

Requests may finish in any order; database commits follow source order. With
multiple requests this makes the existing first-insert-wins conflict behavior
predictable. It also preserves the source order in optional JSON backups. No
coroutine or completed result is retained for every batch in a book.

This is a bound on queued work and source row count, **not a hard byte limit**.
An unusually large source line or LLM response can still be large. The existing
optional per-book JSON backup (`saveJsonBackup=true`, enabled by the CLI) still
collects the newly generated entries in memory and serializes them at the end of
the book. Disable that option when using the API to measure the bounded pipeline
independently of backup retention.

## Resume and failures

A successful batch inserts its entries and all processed-line markers in one
SQLDelight transaction. An entry failure or marker failure rolls back both, so
the batch can be retried on a subsequent run. A database write failure propagates
and stops further work instead of continuing to spend requests with an unusable
output database. Empty/invalid LLM responses keep the existing behavior: no
processed markers are written, and the remaining source batches can continue.
A valid response with zero entries still marks its source lines processed.

Producer, worker and consumer failures cancel the pipeline. Its child jobs finish
before `processLines` closes either database. The existing HTTP provider remains
blocking and keeps its 160-second timeout; cancellation prevents a completed
response from being committed, but does not claim to immediately interrupt an
HTTP call that ignores interruption.

## Regression checks

Run the JVM tests from the normal checkout inside its required SeforimLibrary
parent build (the existing `settings.gradle.kts` uses `includeBuild("..")`):

```sh
./gradlew :magicindexer:jvmTest
```

The pipeline tests cover inclusive page boundaries, sparse/empty pages, blank
and processed lines across a batch boundary, a partial final batch, and integer
overflow at the last page. They also gate requests deterministically to check
concurrency, ordered commits, producer backpressure, and failure/cancellation on
each side of the pipeline without a live LLM.

The one-million-line source test blocks the request workers and observes only
one 256-row source page, one processed-ID lookup, and five prepared batches at
concurrency three. The remaining source is not read. This checks bounded
retention directly; it is not a production throughput or LLM latency benchmark.

The writer tests use a real SQLite driver and the generated SQLDelight schema.
Triggers inject failures during entry insertion and during marker insertion;
the tests verify rollback, preservation of previously committed data, successful
resume, idempotent markers, and page-scoped processed-ID lookup.
