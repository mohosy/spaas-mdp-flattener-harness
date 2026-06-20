package com.example.spaas.lineage;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

class HashingTest {

    @Test
    void sha256_isStableAndHex() {
        String h = Hashing.sha256Hex("hello".getBytes());
        assertThat(h).hasSize(64).isEqualTo(Hashing.sha256Hex("hello".getBytes()));
        assertThat(h).matches("[0-9a-f]{64}");
    }

    @Test
    void canonicalRowHash_isOrderIndependent() {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("b", 2);
        a.put("a", 1);
        a.put("c", "x");
        Map<String, Object> b = new TreeMap<>();   // different insertion order/impl
        b.put("c", "x");
        b.put("a", 1);
        b.put("b", 2);
        assertThat(Hashing.canonicalRowHash(a)).isEqualTo(Hashing.canonicalRowHash(b));
    }

    @Test
    void canonicalRowHash_nullSafe_distinguishesNullFromString() {
        Map<String, Object> withNull = new LinkedHashMap<>();
        withNull.put("k", null);
        Map<String, Object> withStr = new LinkedHashMap<>();
        withStr.put("k", "null");
        assertThat(Hashing.canonicalRowHash(withNull)).isNotEqualTo(Hashing.canonicalRowHash(withStr));
    }

    @Test
    void canonicalRowHash_sensitiveToValues() {
        Map<String, Object> base = new LinkedHashMap<>();
        base.put("v", 12.34);
        base.put("t", Instant.parse("2026-06-19T10:00:00.050Z"));
        String h1 = Hashing.canonicalRowHash(base);

        Map<String, Object> changed = new LinkedHashMap<>(base);
        changed.put("v", 12.35);
        assertThat(Hashing.canonicalRowHash(changed)).isNotEqualTo(h1);

        // identical content -> identical hash
        Map<String, Object> same = new LinkedHashMap<>();
        same.put("t", Instant.parse("2026-06-19T10:00:00.050Z"));
        same.put("v", 12.34);
        assertThat(Hashing.canonicalRowHash(same)).isEqualTo(h1);
    }
}
