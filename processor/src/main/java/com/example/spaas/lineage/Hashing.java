package com.example.spaas.lineage;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Lineage hashing helpers.
 *
 * <ul>
 *   <li>{@link #sha256Hex(byte[])} — hash of the raw message bytes ({@code raw_payload_hash}).</li>
 *   <li>{@link #canonicalRowHash(Map)} — deterministic, key-sorted, null-safe hash of the
 *       BUSINESS columns only. This defines row identity and is the key used for
 *       intra-message deduplication. The same event always produces the same hash.</li>
 * </ul>
 */
public final class Hashing {

    /** Sentinel for a null value so {@code null} and the literal string "null" hash differently. */
    private static final String NULL_SENTINEL = "\u0000NULL\u0000";

    private Hashing() {}

    public static String sha256Hex(byte[] data) {
        MessageDigest md = newSha256();
        md.update(data);
        return toHex(md.digest());
    }

    public static String sha256Hex(String s) {
        return sha256Hex(s.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Canonical hash of the business columns. Deterministic regardless of map iteration
     * order: keys are sorted, each {@code key=value} pair is newline-delimited, nulls use a
     * sentinel, and values use a canonical string form so identical events always collide.
     */
    public static String canonicalRowHash(Map<String, Object> businessColumns) {
        // TreeMap => keys in stable sorted order, independent of insertion order.
        TreeMap<String, Object> sorted = new TreeMap<>(businessColumns);
        StringBuilder sb = new StringBuilder(256);
        for (Map.Entry<String, Object> e : sorted.entrySet()) {
            sb.append(e.getKey()).append('=').append(canonicalValue(e.getValue())).append('\n');
        }
        return sha256Hex(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    /** Canonical string form of a value. Numbers use their precise decimal/long form. */
    static String canonicalValue(Object v) {
        if (v == null) {
            return NULL_SENTINEL;
        }
        // String.valueOf is deterministic for String/Long/Integer/Double/Boolean.
        // Double.toString(12.34) is stable, and 12.340 == 12.34 as a double => same form.
        return String.valueOf(v);
    }

    private static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
