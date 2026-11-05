package io.github.kdroidfilter.seforim.magicindexer

import io.github.kdroidfilter.seforim.magicindexer.db.DatabaseQueries
import io.github.kdroidfilter.seforim.magicindexer.model.LexicalEntry

/**
 * Entries and resume markers form one atomic unit. Exceptions deliberately
 * propagate so the pipeline stops submitting requests after a database failure.
 * The caller serializes access to the output connection.
 */
internal fun commitLexicalBatch(
    queries: DatabaseQueries,
    bookId: Long,
    lineIds: List<Long>,
    entries: List<LexicalEntry>,
) {
    queries.transaction {
        entries.forEach { insertEntry(queries, it) }
        val currentTime = System.currentTimeMillis() / 1000
        lineIds.forEach { lineId ->
            queries.insertProcessedLine(lineId, bookId, currentTime)
        }
    }
}

/**
 * Inserts a single lexical entry into the database.
 * Handles the relationships: base -> surface -> variants
 */
private fun insertEntry(queries: DatabaseQueries, entry: LexicalEntry) {
    // 1. Insert base (will be ignored if already exists due to UNIQUE constraint)
    queries.insertBase(entry.base)

    // 2. Get base_id
    val baseId = queries.selectBaseByValue(entry.base).executeAsOne()

    // 3. Insert surface with base_id and optional notes
    queries.insertSurface(
        value_ = entry.surface,
        base_id = baseId,
        notes = entry.notes
    )

    // 4. Get surface_id
    val surfaceRow = queries.selectSurfaceByValue(entry.surface).executeAsOne()
    val surfaceId = surfaceRow.id

    // 5. Insert all variants (globally unique, shared across surfaces)
    entry.variants.forEach { variant ->
        // Insert variant (will be ignored if already exists due to UNIQUE constraint)
        queries.insertVariant(value_ = variant)

        // Get variant_id
        val variantRow = queries.selectVariantByValue(variant).executeAsOne()
        val variantId = variantRow.id

        // Link variant to surface (will be ignored if link already exists)
        queries.insertSurfaceVariant(
            surface_id = surfaceId,
            variant_id = variantId
        )
    }
}
