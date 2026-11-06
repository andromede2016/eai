package com.eai.framework.support;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.eai.framework.core.Message;
import com.eai.framework.spi.FormatCodec;

/** Simple CSV codec (header line, no quoting) to/from List&lt;Map&lt;String,String&gt;&gt;. */
public class CsvCodec implements FormatCodec {

    private final String sep;

    public CsvCodec() { this(";"); }
    public CsvCodec(String sep) { this.sep = sep; }

    @Override public String format() { return "csv"; }

    @Override
    public Object decode(byte[] raw, Message ctx) {
        String[] lines = new String(raw, StandardCharsets.UTF_8).split("\\R");
        List<Map<String, String>> rows = new ArrayList<>();
        if (lines.length == 0 || lines[0].isBlank()) return rows;
        String q = Pattern.quote(sep);
        String[] head = lines[0].split(q, -1);
        for (int i = 1; i < lines.length; i++) {
            if (lines[i].isBlank()) continue;
            String[] v = lines[i].split(q, -1);
            Map<String, String> row = new LinkedHashMap<>();
            for (int c = 0; c < head.length; c++) row.put(head[c], c < v.length ? v[c] : "");
            rows.add(row);
        }
        return rows;
    }

    @Override
    @SuppressWarnings("unchecked")
    public byte[] encode(Object body, Message ctx) {
        List<Map<String, Object>> rows = (List<Map<String, Object>>) body;
        StringBuilder sb = new StringBuilder();
        if (!rows.isEmpty()) {
            sb.append(String.join(sep, rows.get(0).keySet())).append('\n');
            for (Map<String, Object> r : rows) {
                List<String> vals = new ArrayList<>();
                r.values().forEach(v -> vals.add(String.valueOf(v)));
                sb.append(String.join(sep, vals)).append('\n');
            }
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
