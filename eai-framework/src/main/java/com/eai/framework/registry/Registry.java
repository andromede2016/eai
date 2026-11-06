package com.eai.framework.registry;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.eai.framework.core.EaiException;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/** Generic, case-insensitive key registry. */
@RequiredArgsConstructor
public class Registry<T> {
    private final @NonNull String kind;
    private final @NonNull Function<T, String> keyFn;
    private final Map<String, T> items = new ConcurrentHashMap<>();

    public Registry<T> register(@NonNull T item) { items.put(keyFn.apply(item).toLowerCase(), item); return this; }
    public Optional<T> find(String key) { return key == null ? Optional.empty() : Optional.ofNullable(items.get(key.toLowerCase())); }
    public T get(String key) { return find(key).orElseThrow(() -> new EaiException("No " + kind + " registered for '" + key + "'")); }
}
