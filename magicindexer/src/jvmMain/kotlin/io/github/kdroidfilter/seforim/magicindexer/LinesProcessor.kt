package io.github.kdroidfilter.seforim.magicindexer

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforim.magicindexer.db.Database
import io.github.kdroidfilter.seforim.magicindexer.db.DatabaseQueries
import io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntry
import io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntrySerializer
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist
import java.io.File
import java.util.concurrent.atomic.AtomicInteger

/**
 * Processes lines from the SeforimLibrary database through an LLM to generate lexical entries.
 * Writes entries directly to the database incrementally to avoid memory issues.
 */
object LinesProcessor {

    private const val BATCH_SIZE = 5 // Number of lines to process together
    private const val PAGE_SIZE = 256 // Also bounds processed-ID lookups
    private const val TIMEOUT_SECONDS = 160 // Timeout per batch in seconds
    private const val INVALID_JSON_DIR = "build/invalid-llm-json"
    private const val DEFAULT_CONCURRENT_REQUESTS = 1 // Default to sequential processing

    /**
     * Cleans HTML tags from text using JSoup and returns plain text.
     * Removes all HTML tags, decodes HTML entities, and trims whitespace.
     */
    private fun cleanHtml(html: String): String {
        return Jsoup.clean(html, Safelist.none())
            .replace("&nbsp;", " ")
            .replace("\u00A0", " ")
            .trim()
    }

    /**
     * Processes lines from specified books in the SeforimLibrary database.
     * Writes entries directly to the lexical database incrementally.
     * Lines are processed in batches to optimize LLM calls.
     *
     * @param sourceDbPath Path to the source SeforimLibrary database
     * @param outputDbPath Path to save the generated lexical database
     * @param bookIds List of book IDs to process (defaults to 1-10)
     * @param saveJsonBackup If true, saves a JSON backup for debugging
     * @param concurrentRequests Number of concurrent LLM requests (default: 1 for sequential)
     * @return Number of entries processed
     */
    fun processLines(
        sourceDbPath: String,
        outputDbPath: String,
        bookIds: List<Long> = (1L..10L).toList(),
        saveJsonBackup: Boolean = false,
        concurrentRequests: Int = DEFAULT_CONCURRENT_REQUESTS
    ): Int = runBlocking {
        val effectiveConcurrency = concurrentRequests.coerceIn(1, 250) // Keep the existing supported range

        println("=== Starting Incremental Processing ===")
        println("Source DB: $sourceDbPath")
        println("Output DB: $outputDbPath")
        println("Books to process: ${bookIds.joinToString(", ")}")
        println("Concurrent requests: $effectiveConcurrency")

        // Initialize the source repository
        println("\nInitializing SeforimRepository...")
        val sourceDriver = JdbcSqliteDriver("jdbc:sqlite:$sourceDbPath")
        val repository = SeforimRepository(sourceDbPath, sourceDriver)

        // Initialize the output database (create if doesn't exist, or open existing)
        val dbFile = File(outputDbPath)
        if (dbFile.exists()) {
            println("Opening existing database: $outputDbPath")
        } else {
            println("Creating new database: $outputDbPath")
        }

        val outputDriver = JdbcSqliteDriver("jdbc:sqlite:$outputDbPath")
        Database.Schema.create(outputDriver)
        val database = Database(outputDriver)
        val queries = database.databaseQueries

        // Thread-safe counters
        val totalLinesProcessed = AtomicInteger(0)
        val totalLinesSkipped = AtomicInteger(0)
        val totalEntriesInserted = AtomicInteger(0)

        // Mutex for database operations (SQLite is not thread-safe)
        val dbMutex = Mutex()

        try {
            // Process specified books
            for (bookId in bookIds) {
                println("\n=== Processing Book ID: $bookId ===")

                val book = repository.getBook(bookId)
                if (book == null) {
                    println("Book $bookId not found, skipping...")
                    continue
                }

                println("Book: ${book.title}")
                println("Total lines: ${book.totalLines}")

                // Optional JSON backup; only the ordered consumer appends entries.
                val bookJsonBackup = if (saveJsonBackup) mutableListOf<LexicalEntry>() else null

                // Check how many lines already processed for this book
                val alreadyProcessedCount = dbMutex.withLock {
                    queries.countProcessedLinesForBook(bookId).executeAsOne()
                }
                if (alreadyProcessedCount > 0) {
                    println("Already processed: $alreadyProcessedCount lines (will skip)")
                }

                println("Reading pages of $PAGE_SIZE lines and processing batches of $BATCH_SIZE with $effectiveConcurrency workers")

                runLineBatchPipeline(
                    concurrency = effectiveConcurrency,
                    produce = { emit ->
                        produceLineBatches(
                            lastLineIndex = book.totalLines,
                            pageSize = PAGE_SIZE,
                            batchSize = BATCH_SIZE,
                            readPage = { first, last ->
                                repository.getLines(bookId, first, last).map { SourceLine(it.id, it.content) }
                            },
                            findProcessedIds = { ids ->
                                dbMutex.withLock {
                                    queries.selectProcessedLineIds(ids).executeAsList().toSet()
                                }
                            },
                            cleanHtml = ::cleanHtml,
                            onSkipped = { totalLinesSkipped.addAndGet(it) },
                            emit = emit,
                        )
                    },
                    process = { batch -> requestBatch(batch, bookId, book.totalLines) },
                    consume = { batch, entries ->
                        if (entries != null) {
                            persistBatch(
                                batch = batch,
                                entries = entries,
                                bookId = bookId,
                                queries = queries,
                                dbMutex = dbMutex,
                                bookJsonBackup = bookJsonBackup,
                                totalLinesProcessed = totalLinesProcessed,
                                totalEntriesInserted = totalEntriesInserted,
                            )
                        }
                    },
                )

                val bookProcessedCount = dbMutex.withLock {
                    queries.countProcessedLinesForBook(bookId).executeAsOne()
                }
                println("Completed book $bookId: ${totalLinesProcessed.get()} new lines processed, ${totalLinesSkipped.get()} skipped, $bookProcessedCount total processed for this book")

                // Save JSON backup for this book
                if (saveJsonBackup && bookJsonBackup != null && bookJsonBackup.isNotEmpty()) {
                    // Sanitize book title for filename
                    val sanitizedTitle = book.title
                        .replace(Regex("[^a-zA-Z0-9א-ת\\s-]"), "")
                        .replace(Regex("\\s+"), "-")
                        .take(50) // Limit length

                    val jsonPath = File(File(outputDbPath).parent, "book-${bookId}-${sanitizedTitle}.json").absolutePath
                    println("Saving JSON backup for book $bookId to: $jsonPath")

                    val json = LexicalEntrySerializer.toJson(
                        io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntries(bookJsonBackup)
                    )
                    File(jsonPath).writeText(json)
                    println("JSON backup saved: ${bookJsonBackup.size} entries")
                }
            }

            println("\n=== Processing Complete ===")
            println("Total new lines processed: ${totalLinesProcessed.get()}")
            println("Total lines skipped (already processed): ${totalLinesSkipped.get()}")
            println("Total entries inserted: ${totalEntriesInserted.get()}")
            println("Output DB: $outputDbPath")

            // Print statistics
            dbMutex.withLock {
                printStatistics(queries, bookIds)
            }

        } finally {
            // Close connections
            repository.close()
            outputDriver.close()
        }

        return@runBlocking totalEntriesInserted.get()
    }

    /** Requests one batch. Empty or invalid responses leave it eligible for resume. */
    private suspend fun requestBatch(
        batch: LineBatch,
        bookId: Long,
        totalLines: Int,
    ): List<LexicalEntry>? {
        var llmResponse: String? = null
        try {
            currentCoroutineContext().ensureActive()
            val combinedContent = batch.contents.joinToString("\n")
            println("Processing batch at lines ${batch.startIndex + 1}-${batch.endIndex}/$totalLines (${batch.contents.size} lines in batch, ${TIMEOUT_SECONDS}s timeout)")

            llmResponse = LLMProvider.generateResponse(combinedContent, TIMEOUT_SECONDS)
            // The provider is blocking. Do not commit its result if cancellation
            // happened while the HTTP call was finishing.
            currentCoroutineContext().ensureActive()
            if (llmResponse.isNullOrBlank()) {
                println("  ⚠ Empty response from LLM (possibly timeout or error), skipping batch...")
                return null
            }
            return parseLexicalEntriesOrNull(llmResponse, bookId, batch.endIndex)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            System.err.println("Error processing batch: ${e.message}")
            try {
                if (llmResponse != null) {
                    saveInvalidJsonResponse(llmResponse, bookId, batch.endIndex)
                }
            } catch (_: Exception) {
                // Ignore secondary failures when saving debug JSON.
            }
            e.printStackTrace()
            return null
        }
    }

    /** Commits a complete batch before recording progress or adding backup entries. */
    private suspend fun persistBatch(
        batch: LineBatch,
        entries: List<LexicalEntry>,
        bookId: Long,
        queries: DatabaseQueries,
        dbMutex: Mutex,
        bookJsonBackup: MutableList<LexicalEntry>?,
        totalLinesProcessed: AtomicInteger,
        totalEntriesInserted: AtomicInteger,
    ) {
        try {
            dbMutex.withLock {
                // Entries and resume markers must succeed together. A partial
                // insert must not permanently mark all source lines processed.
                commitLexicalBatch(queries, bookId, batch.lineIds, entries)
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            System.err.println("Error committing batch for book $bookId at ${batch.endIndex}; it will be retried on resume: ${e.message}")
            throw e
        }

        // Only the ordered consumer updates the backup; no whole-book result
        // list is retained unless the existing optional JSON backup is enabled.
        bookJsonBackup?.addAll(entries)
        totalEntriesInserted.addAndGet(entries.size)
        totalLinesProcessed.addAndGet(batch.contents.size)
        println("  → Processed ${batch.contents.size} lines | Generated ${entries.size} entries | Total: ${totalLinesProcessed.get()} new")
    }

    /**
     * Prints database statistics.
     */
    private fun printStatistics(queries: DatabaseQueries, bookIds: List<Long>) {
        println("\n=== Database Statistics ===")
        println("Database created successfully with incremental writes")

        // Statistics per book
        println("\nProcessed lines per book:")
        bookIds.forEach { bookId ->
            val count = queries.countProcessedLinesForBook(bookId).executeAsOne()
            println("  Book $bookId: $count lines processed")
        }

        println("===========================\n")
    }

    /**
     * Tries to parse the LLM JSON response into a list of lexical entries.
     * On failure, logs a concise error message and saves the raw response
     * into build/invalid-llm-json for offline inspection.
     */
    private fun parseLexicalEntriesOrNull(
        response: String,
        bookId: Long,
        batchIndex: Int
    ): List<LexicalEntry>? {
        return try {
            LexicalEntrySerializer.fromJson(response).entries
        } catch (e: Exception) {
            System.err.println(
                "  ⚠ Failed to parse LLM JSON for book $bookId, batch $batchIndex: ${e.message}"
            )
            try {
                saveInvalidJsonResponse(response, bookId, batchIndex)
            } catch (_: Exception) {
                // Ignore secondary failures when saving debug JSON
            }
            null
        }
    }

    /**
     * Saves an invalid LLM JSON response to disk for debugging.
     * Files are written under build/invalid-llm-json with a descriptive name.
     */
    private fun saveInvalidJsonResponse(
        response: String,
        bookId: Long,
        batchIndex: Int
    ) {
        val dir = File(INVALID_JSON_DIR)
        if (!dir.exists()) {
            dir.mkdirs()
        }
        val fileName = "book-${bookId}-batch-${batchIndex}-${System.currentTimeMillis()}.json"
        val file = File(dir, fileName)
        file.writeText(response)
        println("  ⚠ Saved invalid JSON response to: ${file.absolutePath}")
    }
}