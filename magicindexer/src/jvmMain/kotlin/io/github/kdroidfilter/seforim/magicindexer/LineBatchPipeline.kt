package io.github.kdroidfilter.seforim.magicindexer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch

internal data class SourceLine(val id: Long, val content: String)

internal data class LineBatch(
    val contents: List<String>,
    val lineIds: List<Long>,
    val startIndex: Int,
    val endIndex: Int,
)

/**
 * Reads inclusive source ranges, keeping only one page and the current batch.
 * Batch boundaries are independent of page boundaries, blank lines and resume
 * lookups, so the LLM receives the same groups as in the whole-book traversal.
 */
internal suspend fun produceLineBatches(
    lastLineIndex: Int,
    pageSize: Int,
    batchSize: Int,
    readPage: suspend (startIndex: Int, endIndex: Int) -> List<SourceLine>,
    findProcessedIds: suspend (List<Long>) -> Set<Long>,
    cleanHtml: (String) -> String,
    onSkipped: (Int) -> Unit,
    emit: suspend (LineBatch) -> Unit,
) {
    require(lastLineIndex >= 0)
    require(pageSize > 0)
    require(batchSize > 0)

    var pageStart = 0L
    var rowsRead = 0
    var batchStart = 0
    val contents = ArrayList<String>(batchSize)
    val lineIds = ArrayList<Long>(batchSize)

    suspend fun emitBatch() {
        emit(LineBatch(contents.toList(), lineIds.toList(), batchStart, rowsRead))
        contents.clear()
        lineIds.clear()
        batchStart = rowsRead
    }

    // SeforimRepository.getLines includes BOTH ends of the range. Use Long
    // arithmetic for the next page to avoid overflow at Int.MAX_VALUE.
    while (pageStart <= lastLineIndex) {
        currentCoroutineContext().ensureActive()
        val pageEnd = minOf(lastLineIndex.toLong(), pageStart + pageSize - 1)
        val page = readPage(pageStart.toInt(), pageEnd.toInt())
        if (page.isNotEmpty()) {
            // Bound the IN lookup to one page, well below SQLite's bind limit.
            val processedIds = findProcessedIds(page.map { it.id })
            var skipped = 0
            for (line in page) {
                currentCoroutineContext().ensureActive()
                rowsRead++
                if (line.content.isBlank()) continue
                if (line.id in processedIds) {
                    skipped++
                    continue
                }
                val content = cleanHtml(line.content)
                if (content.isBlank()) continue
                contents.add(content)
                lineIds.add(line.id)
                if (contents.size == batchSize) emitBatch()
            }
            onSkipped(skipped)
        }
        // Empty pages may be gaps in lineIndex; they do not end the book.
        pageStart = pageEnd + 1
    }
    if (contents.isNotEmpty()) emitBatch()
}

private class PendingBatch<T>(val batch: LineBatch) {
    val result = CompletableDeferred<T>()
}

/**
 * A fixed worker pool with bounded input and completion channels. The single
 * consumer commits in source order, including when requests finish out of order.
 * A slow first request cannot cause unbounded completed-result retention: the
 * producer must enter the bounded completion channel before it submits work.
 *
 * At most concurrency + 2 batches have been emitted but not consumed (one in
 * the consumer, concurrency queued, and one producer blocked on send). All child
 * jobs are joined or cancelled before the caller may close its database drivers.
 */
internal suspend fun <T> runLineBatchPipeline(
    concurrency: Int,
    produce: suspend (emit: suspend (LineBatch) -> Unit) -> Unit,
    process: suspend (LineBatch) -> T,
    consume: suspend (LineBatch, T) -> Unit,
) = coroutineScope {
    require(concurrency > 0)
    val work = Channel<PendingBatch<T>>(concurrency)
    val ordered = Channel<PendingBatch<T>>(concurrency)

    repeat(concurrency) {
        launch(Dispatchers.IO) {
            for (pending in work) {
                // A worker failure cancels the scope and the producer. There is
                // no detached promise to wait on after scope cancellation.
                try {
                    pending.result.complete(process(pending.batch))
                } catch (e: Throwable) {
                    // CancellationException alone does not fail a launch's
                    // parent. Complete the promise as well so the ordered
                    // consumer cannot wait forever on a cancelled request.
                    pending.result.completeExceptionally(e)
                    throw e
                }
            }
        }
    }

    launch(Dispatchers.IO) {
        try {
            produce { batch ->
                val pending = PendingBatch<T>(batch)
                ordered.send(pending)
                work.send(pending)
            }
        } catch (e: Throwable) {
            // Also cancel the scope if a producer throws CancellationException
            // after publishing a promise but before putting it in the work queue.
            this@coroutineScope.cancel("Batch production failed", e)
            throw e
        } finally {
            work.close()
            ordered.close()
        }
    }

    for (pending in ordered) {
        consume(pending.batch, pending.result.await())
    }
}
