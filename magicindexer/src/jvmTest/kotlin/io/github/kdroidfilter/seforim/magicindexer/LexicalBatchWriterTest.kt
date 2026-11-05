package io.github.kdroidfilter.seforim.magicindexer

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforim.magicindexer.db.Database
import io.github.kdroidfilter.seforim.magicindexer.db.DatabaseQueries
import io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntry
import java.sql.SQLException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class LexicalBatchWriterTest {
    @Test
    fun entryFailureRollsBackTheWholeBatchAndCanBeResumed() = withDatabase { driver, queries ->
        commitLexicalBatch(queries, 2, listOf(100), listOf(entry("existing")))
        driver.execute(
            null,
            "CREATE TRIGGER reject_variant BEFORE INSERT ON variant " +
                "WHEN NEW.value = 'reject' BEGIN SELECT RAISE(ABORT, 'injected entry failure'); END",
            0,
        )
        val entries = listOf(entry("first"), entry("second", "reject"))
        val failure = assertFailsWith<SQLException> {
            commitLexicalBatch(queries, 1, listOf(10, 11), entries)
        }
        assertTrue(failure.message.orEmpty().contains("injected entry failure"))
        assertNull(queries.selectBaseByValue("first").executeAsOneOrNull())
        assertNull(queries.selectSurfaceByValue("second").executeAsOneOrNull())
        assertTrue(queries.selectProcessedLineIds(listOf(10, 11)).executeAsList().isEmpty())
        assertEquals(listOf(100L), queries.selectProcessedLineIds(listOf(100)).executeAsList())
        assertEquals("existing", queries.selectSurfaceByValue("existing").executeAsOne().value_)

        driver.execute(null, "DROP TRIGGER reject_variant", 0)
        commitLexicalBatch(queries, 1, listOf(10, 11), entries)
        assertEquals(setOf(10L, 11L), queries.selectProcessedLineIds(listOf(10, 11)).executeAsList().toSet())
        assertEquals("first", queries.selectSurfaceByValue("first").executeAsOne().value_)
        assertEquals("second", queries.selectSurfaceByValue("second").executeAsOne().value_)
    }

    @Test
    fun markerFailureRollsBackEntriesAndEarlierMarkers() = withDatabase { driver, queries ->
        driver.execute(
            null,
            "CREATE TRIGGER reject_marker BEFORE INSERT ON processed_lines " +
                "WHEN NEW.line_id = 11 BEGIN SELECT RAISE(ABORT, 'injected marker failure'); END",
            0,
        )
        val failure = assertFailsWith<SQLException> {
            commitLexicalBatch(queries, 1, listOf(10, 11), listOf(entry("first")))
        }
        assertTrue(failure.message.orEmpty().contains("injected marker failure"))
        assertNull(queries.selectBaseByValue("first").executeAsOneOrNull())
        assertTrue(queries.selectProcessedLineIds(listOf(10, 11)).executeAsList().isEmpty())
        driver.execute(null, "DROP TRIGGER reject_marker", 0)
        commitLexicalBatch(queries, 1, listOf(10, 11), listOf(entry("first")))
        assertEquals(2L, queries.countProcessedLinesForBook(1).executeAsOne())
    }

    @Test
    fun pageLookupHandlesBoundedCollectionsWithoutLoadingOtherIds() = withDatabase { _, queries ->
        val ids = (0L until 1024L).filter { it % 2 == 0L }
        commitLexicalBatch(queries, 1, ids, emptyList())
        val page = (256L until 512L).toList()
        val result: List<Long> = queries.selectProcessedLineIds(page).executeAsList()
        assertEquals(page.filter { it % 2 == 0L }, result.sorted())
        assertTrue(queries.selectProcessedLineIds(emptyList()).executeAsList().isEmpty())
    }

    @Test
    fun successfulEmptyResultStillMarksTheBatchAndDuplicateWritesStayIdempotent() = withDatabase { _, queries ->
        commitLexicalBatch(queries, 1, listOf(10, 11), emptyList())
        commitLexicalBatch(queries, 1, listOf(10, 11), emptyList())
        assertEquals(2L, queries.countProcessedLinesForBook(1).executeAsOne())
    }

    private fun entry(value: String, variant: String = "$value-variant") =
        LexicalEntry(surface = value, base = value, variants = listOf(variant), notes = null)

    private fun withDatabase(block: (JdbcSqliteDriver, DatabaseQueries) -> Unit) {
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        try {
            driver.execute(null, "PRAGMA foreign_keys = ON", 0)
            Database.Schema.create(driver)
            block(driver, Database(driver).databaseQueries)
        } finally {
            driver.close()
        }
    }
}
