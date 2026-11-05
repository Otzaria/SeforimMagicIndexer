package io.github.kdroidfilter.seforim.magicindexer

import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

class LineBatchPipelineTest {
    @Test
    fun pagingPreservesBatchBoundariesAndResumeOrder() = runBlocking {
        val source = linkedMapOf(
            0 to SourceLine(100, ""),
            1 to SourceLine(101, "one"),
            2 to SourceLine(102, "already processed"),
            3 to SourceLine(103, "<empty>"),
            5 to SourceLine(105, "two"),
            6 to SourceLine(106, "three"),
            7 to SourceLine(107, "already processed too"),
            8 to SourceLine(108, "four"),
            10 to SourceLine(110, "five"),
            11 to SourceLine(111, "  "),
            12 to SourceLine(112, "six"),
        )
        val pages = mutableListOf<IntRange>()
        val lookups = mutableListOf<List<Long>>()
        val batches = mutableListOf<LineBatch>()
        var skipped = 0
        produceLineBatches(
            lastLineIndex = 12,
            pageSize = 4,
            batchSize = 3,
            readPage = { first, last ->
                pages.add(first..last)
                source.filterKeys { it in first..last }.values.toList()
            },
            findProcessedIds = { ids ->
                lookups.add(ids)
                ids.filter { it in setOf(100L, 102L, 107L) }.toSet()
            },
            cleanHtml = { if (it == "<empty>") "" else it },
            onSkipped = { skipped += it },
            emit = { batches.add(it) },
        )
        assertEquals(listOf(0..3, 4..7, 8..11, 12..12), pages)
        assertEquals(listOf(4, 3, 3, 1), lookups.map { it.size })
        assertEquals(2, skipped) // Raw blank lines do not count as resume skips.
        assertEquals(
            listOf(
                LineBatch(listOf("one", "two", "three"), listOf(101L, 105L, 106L), 0, 6),
                LineBatch(listOf("four", "five", "six"), listOf(108L, 110L, 112L), 6, 11),
            ),
            batches,
        )
    }

    @Test
    fun finalPartialBatchIncludesTrailingSkippedRowsInProgress() = runBlocking {
        val source = listOf(
            SourceLine(1, "one"), SourceLine(2, "done"), SourceLine(3, "two"),
            SourceLine(4, " "), SourceLine(5, "done"),
        )
        val batches = mutableListOf<LineBatch>()
        produceLineBatches(
            lastLineIndex = 4,
            pageSize = 2,
            batchSize = 3,
            readPage = { first, last -> source.slice(first..last) },
            findProcessedIds = { setOf(2L, 5L) },
            cleanHtml = { it },
            onSkipped = {},
            emit = { batches.add(it) },
        )
        assertEquals(listOf(LineBatch(listOf("one", "two"), listOf(1L, 3L), 0, 5)), batches)
    }

    @Test
    fun emptyPagesDoNotTruncateSparseBooks() = runBlocking {
        val pages = mutableListOf<IntRange>()
        val batches = mutableListOf<LineBatch>()
        var lookups = 0
        produceLineBatches(
            lastLineIndex = 9,
            pageSize = 3,
            batchSize = 5,
            readPage = { first, last ->
                pages.add(first..last)
                listOf(0, 9).filter { it in first..last }.map { SourceLine(it.toLong(), "$it") }
            },
            findProcessedIds = { lookups++; emptySet() },
            cleanHtml = { it },
            onSkipped = {},
            emit = { batches.add(it) },
        )
        assertEquals(listOf(0..2, 3..5, 6..8, 9..9), pages)
        assertEquals(2, lookups)
        assertEquals(listOf(0L, 9L), batches.single().lineIds)
    }

    @Test
    fun emptyBookDoesNotQueryResumeOrEmitABatch() = runBlocking {
        produceLineBatches(
            lastLineIndex = 0,
            pageSize = 256,
            batchSize = 5,
            readPage = { first, last ->
                assertEquals(0..0, first..last)
                emptyList()
            },
            findProcessedIds = { error("No IDs to query") },
            cleanHtml = { error("No lines to clean") },
            onSkipped = {},
            emit = { error("No batch to emit") },
        )
    }

    @Test
    fun inclusiveLastIndexDoesNotOverflow() = runBlocking {
        val pages = mutableListOf<IntRange>()
        produceLineBatches(
            lastLineIndex = Int.MAX_VALUE,
            pageSize = Int.MAX_VALUE,
            batchSize = 5,
            readPage = { first, last -> pages.add(first..last); emptyList() },
            findProcessedIds = { error("No IDs to query") },
            cleanHtml = { it },
            onSkipped = {},
            emit = {},
        )
        assertEquals(listOf(0 until Int.MAX_VALUE, Int.MAX_VALUE..Int.MAX_VALUE), pages)
    }

    @Test
    fun slowFirstRequestBoundsOutstandingWorkAndCommitsInSourceOrder() = runBlocking {
        withTimeout(10_000) {
            val concurrency = 3
            val releaseFirst = CompletableDeferred<Unit>()
            val releaseOthers = CompletableDeferred<Unit>()
            val allWorkersStarted = CompletableDeferred<Unit>()
            val fastWorkersFinished = CompletableDeferred<Unit>()
            val producerAtBound = CompletableDeferred<Unit>()
            val active = AtomicInteger()
            val peakActive = AtomicInteger()
            val started = AtomicInteger()
            val fastFinished = AtomicInteger()
            val emitted = AtomicInteger()
            val committed = Collections.synchronizedList(mutableListOf<Int>())
            val task = async {
                runLineBatchPipeline(
                    concurrency = concurrency,
                    produce = { emit ->
                        repeat(50) { index ->
                            if (emitted.incrementAndGet() == concurrency + 2) producerAtBound.complete(Unit)
                            emit(batch(index))
                        }
                    },
                    process = { batch ->
                        val count = active.incrementAndGet()
                        peakActive.accumulateAndGet(count, ::maxOf)
                        if (started.incrementAndGet() == concurrency) allWorkersStarted.complete(Unit)
                        try {
                            if (batch.startIndex == 0) {
                                releaseFirst.await()
                            } else {
                                releaseOthers.await()
                                if (fastFinished.incrementAndGet() == concurrency - 1) fastWorkersFinished.complete(Unit)
                            }
                            batch.startIndex
                        } finally {
                            active.decrementAndGet()
                        }
                    },
                    consume = { _, value -> committed.add(value) },
                )
            }
            allWorkersStarted.await()
            producerAtBound.await()
            releaseOthers.complete(Unit)
            fastWorkersFinished.await()
            assertEquals(emptyList(), committed.toList())
            assertEquals(concurrency + 2, emitted.get())
            assertEquals(concurrency, peakActive.get())
            releaseFirst.complete(Unit)
            task.await()
            assertEquals((0 until 50).toList(), committed.toList())
            assertEquals(0, active.get())
        }
    }

    @Test
    fun workerFailureCancelsBlockedProducerAndJoinsWorkers() = runBlocking {
        withTimeout(10_000) {
            val active = AtomicInteger()
            val failure = assertFailsWith<IllegalStateException> {
                runLineBatchPipeline(
                    concurrency = 2,
                    produce = { emit -> repeat(10_000) { emit(batch(it)) } },
                    process = { item ->
                        active.incrementAndGet()
                        try {
                            if (item.startIndex == 0) error("worker failed")
                            awaitCancellation()
                        } finally {
                            active.decrementAndGet()
                        }
                    },
                    consume = { _, _ -> error("No successful results") },
                )
            }
            assertEquals("worker failed", failure.message)
            assertEquals(0, active.get())
        }
    }

    @Test
    fun requestCancellationDoesNotLeaveAnUncompletedPromise() = runBlocking {
        withTimeout(10_000) {
            val failure = assertFailsWith<CancellationException> {
                runLineBatchPipeline(
                    concurrency = 1,
                    produce = { emit -> repeat(100) { emit(batch(it)) } },
                    process = { throw CancellationException("request cancelled") },
                    consume = { _, _ -> error("No successful results") },
                )
            }
            assertEquals("request cancelled", failure.message)
        }
    }

    @Test
    fun producerFailureCancelsInFlightRequests() = runBlocking {
        withTimeout(10_000) {
            val active = AtomicInteger()
            val started = CompletableDeferred<Unit>()
            val failure = assertFailsWith<IllegalArgumentException> {
                runLineBatchPipeline(
                    concurrency = 2,
                    produce = { emit ->
                        emit(batch(0))
                        started.await()
                        throw IllegalArgumentException("source failed")
                    },
                    process = {
                        active.incrementAndGet()
                        started.complete(Unit)
                        try { awaitCancellation() } finally { active.decrementAndGet() }
                    },
                    consume = { _, _ -> error("No successful results") },
                )
            }
            assertEquals("source failed", failure.message)
            assertEquals(0, active.get())
        }
    }

    @Test
    fun consumerFailureCancelsTheRemainingPipeline() = runBlocking {
        withTimeout(10_000) {
            val emitted = AtomicInteger()
            val failure = assertFailsWith<IllegalStateException> {
                runLineBatchPipeline(
                    concurrency = 2,
                    produce = { emit -> repeat(10_000) { emitted.incrementAndGet(); emit(batch(it)) } },
                    process = { it.startIndex },
                    consume = { _, _ -> error("writer failed") },
                )
            }
            assertEquals("writer failed", failure.message)
            assertTrue(emitted.get() <= 5)
        }
    }

    @Test
    fun blockedWorkersStopReadingAOneMillionLineBookAfterOnePage() = runBlocking {
        withTimeout(10_000) {
            val atCapacity = CompletableDeferred<Unit>()
            val workersStarted = CompletableDeferred<Unit>()
            val pagesRead = AtomicInteger()
            val rowsRead = AtomicInteger()
            val lookups = AtomicInteger()
            val emitted = AtomicInteger()
            val active = AtomicInteger()
            val task = async {
                runLineBatchPipeline(
                    concurrency = 3,
                    produce = { emit ->
                        produceLineBatches(
                            lastLineIndex = 999_999,
                            pageSize = 256,
                            batchSize = 5,
                            readPage = { first, last ->
                                pagesRead.incrementAndGet()
                                rowsRead.addAndGet(last - first + 1)
                                (first..last).map { SourceLine(it.toLong(), "line-$it") }
                            },
                            findProcessedIds = { ids ->
                                assertTrue(ids.size <= 256)
                                lookups.incrementAndGet()
                                emptySet()
                            },
                            cleanHtml = { it },
                            onSkipped = {},
                            emit = { batch ->
                                if (emitted.incrementAndGet() == 5) atCapacity.complete(Unit)
                                emit(batch)
                            },
                        )
                    },
                    process = {
                        if (active.incrementAndGet() == 3) workersStarted.complete(Unit)
                        try { awaitCancellation() } finally { active.decrementAndGet() }
                    },
                    consume = { _, _ -> error("Workers should remain blocked") },
                )
            }
            atCapacity.await()
            workersStarted.await()
            assertEquals(1, pagesRead.get())
            assertEquals(256, rowsRead.get())
            assertEquals(1, lookups.get())
            assertEquals(5, emitted.get())
            task.cancelAndJoin()
            assertEquals(0, active.get())
        }
    }

    @Test
    fun producerCancellationDoesNotStrandTheOrderedConsumer() = runBlocking {
        withTimeout(10_000) {
            val started = CompletableDeferred<Unit>()
            val active = AtomicInteger()
            val failure = assertFailsWith<CancellationException> {
                runLineBatchPipeline(
                    concurrency = 1,
                    produce = { emit ->
                        emit(batch(0))
                        started.await()
                        throw CancellationException("source cancelled")
                    },
                    process = {
                        active.incrementAndGet()
                        started.complete(Unit)
                        try { awaitCancellation() } finally { active.decrementAndGet() }
                    },
                    consume = { _, _ -> error("No successful results") },
                )
            }
            assertEquals("source cancelled", failure.cause?.message)
            assertEquals(0, active.get())
        }
    }

    private fun batch(index: Int) = LineBatch(listOf("$index"), listOf(index.toLong()), index, index + 1)
}
