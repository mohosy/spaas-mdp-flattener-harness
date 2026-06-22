package com.example.spaas.transform.mdp;

import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;

/**
 * One derived column rule for the MDP flattener. A derived column is computed from the
 * event JSON by a specific operation instead of being copied straight from a single
 * field. There are exactly four operations, one concrete type each, because the
 * production MDP model needs exactly these four shapes and nothing more. This is
 * deliberately not a generic plugin system.
 *
 * These types hold plain data (no behavior). {@link MdpFlattener} reads the type and
 * applies the matching logic, so the mapping stays declarative and the flattener owns
 * the work. Each type validates itself on construction, so a misconfigured mapping
 * fails fast when it is loaded rather than later at runtime.
 */
public sealed interface MdpOperation extends Serializable
        permits FallbackOp, ArrayReduceOp, NumericStringSplitOp, MultiFormatTimestampOp {

    /**
     * Token in a multi format timestamp list that matches an ISO 8601 instant or offset
     * date time (for example {@code 2026-06-19T10:00:00Z}). Any other entry is a
     * calendar date pattern. Shared so the parser and the validator agree on one spelling.
     */
    String ISO_8601 = "ISO_8601";

    /** Returns {@code value}, or throws a clear message naming {@code what} if it is blank. */
    static String requireText(String value, String what) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(what + " is required");
        }
        return value;
    }
}

/**
 * Fallback chain (coalesce): the target column takes the value of the first source that
 * is present and not null, walking {@code sources} in order. If every source is missing
 * or null, the column is null. Example: product identity resolves from the instrument,
 * then the accessory, then the sub assembly.
 */
record FallbackOp(String column, List<String> sources) implements MdpOperation {
    FallbackOp {
        MdpOperation.requireText(column, "fallback.column");
        if (sources == null || sources.isEmpty()) {
            throw new IllegalArgumentException("fallback '" + column + "': sources must list at least one field");
        }
        sources = List.copyOf(sources);
    }
}

/**
 * Array reduce: scan an array of objects that each carry a name and a value, keep the
 * elements whose name equals {@code matchToken} (compared ignoring case), parse each
 * kept value as a decimal, drop values that are not numbers, and collapse the survivors
 * by {@code reduce} (MAX or MIN). With no match, or no numeric value, the column is null.
 * A single match returns that one value. Example: the upper and lower limits live as
 * named entries (UL, LL) inside the test specification attributes array.
 */
record ArrayReduceOp(String column, String arrayField, String nameKey, String valueKey,
                     String matchToken, ReduceKind reduce) implements MdpOperation {
    ArrayReduceOp {
        MdpOperation.requireText(column, "array_reduce.column");
        MdpOperation.requireText(arrayField, "array_reduce.source_array");
        MdpOperation.requireText(nameKey, "array_reduce.name_key");
        MdpOperation.requireText(valueKey, "array_reduce.value_key");
        MdpOperation.requireText(matchToken, "array_reduce.match");
        if (reduce == null) {
            throw new IllegalArgumentException("array_reduce '" + column + "': reduce must be max or min");
        }
    }
}

/** How {@link ArrayReduceOp} collapses the matched numeric values. */
enum ReduceKind { MAX, MIN }

/**
 * Numeric and string split: read one source field and route it to one of two columns. If
 * the value parses as a decimal, the numeric column gets the number and the string column
 * is null. If it does not parse, the string column gets the original text and the numeric
 * column is null. A present value that is not a number is kept as text, it is never
 * quarantined. Example: a measurement result that may be a number or a text code.
 */
record NumericStringSplitOp(String sourceField, String numericColumn, String stringColumn)
        implements MdpOperation {
    NumericStringSplitOp {
        MdpOperation.requireText(sourceField, "numeric_string_split.source");
        MdpOperation.requireText(numericColumn, "numeric_string_split.numeric_column");
        MdpOperation.requireText(stringColumn, "numeric_string_split.string_column");
        if (numericColumn.equals(stringColumn)) {
            throw new IllegalArgumentException(
                    "numeric_string_split: numeric_column and string_column must differ (both '" + numericColumn + "')");
        }
    }
}

/**
 * Multi format timestamp: parse a source string by trying an ordered list of formats and
 * keeping the first that parses. The token {@code ISO_8601} matches an ISO 8601 instant
 * or offset date time. Any other entry is a calendar date pattern (for example
 * {@code MM/dd/yyyy} or {@code yyyy-MM-dd}) read at midnight UTC. If nothing parses the
 * column is null, so a present but unparseable value is nulled rather than quarantined.
 * Each non ISO pattern is verified here on construction, so a typo in the mapping fails
 * fast on load.
 */
record MultiFormatTimestampOp(String column, String sourceField, List<String> formats)
        implements MdpOperation {
    MultiFormatTimestampOp {
        MdpOperation.requireText(column, "multi_format_timestamp.column");
        MdpOperation.requireText(sourceField, "multi_format_timestamp.source");
        if (formats == null || formats.isEmpty()) {
            throw new IllegalArgumentException(
                    "multi_format_timestamp '" + column + "': formats must list at least one format");
        }
        for (String format : formats) {
            if (format == null || format.isBlank()) {
                throw new IllegalArgumentException("multi_format_timestamp '" + column + "': a format entry is blank");
            }
            if (!MdpOperation.ISO_8601.equalsIgnoreCase(format)) {
                try {
                    DateTimeFormatter.ofPattern(format, Locale.ROOT);
                } catch (IllegalArgumentException ex) {
                    throw new IllegalArgumentException("multi_format_timestamp '" + column
                            + "': invalid date format '" + format + "' (" + ex.getMessage() + ")");
                }
            }
        }
        formats = List.copyOf(formats);
    }
}
