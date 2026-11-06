package com.eai.person.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.eai.framework.core.Message;
import com.eai.framework.spi.FormatCodec;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Flat XML codec: &lt;root&gt;&lt;field&gt;value&lt;/field&gt;...&lt;/root&gt; &lt;-&gt; Map&lt;String,String&gt;.
 * Root element name is kept under key {@link #ROOT}. XXE protected.
 */
public class XmlCodec implements FormatCodec {

    public static final String ROOT = "_root";

    private final ObjectMapper mapper;

    public XmlCodec(ObjectMapper mapper) { this.mapper = mapper; }
    public XmlCodec() { this(new ObjectMapper().findAndRegisterModules()); }

    @Override public String format() { return "xml"; }

    @Override
    public Object decode(byte[] raw, Message ctx) throws Exception {
        DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
        f.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        f.setExpandEntityReferences(false);
        Document doc = f.newDocumentBuilder().parse(new ByteArrayInputStream(raw));
        Element root = doc.getDocumentElement();
        Map<String, String> map = new LinkedHashMap<>();
        map.put(ROOT, root.getTagName());
        NodeList children = root.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) map.put(n.getNodeName(), n.getTextContent().trim());
        }
        return map;
    }

    @Override
    public byte[] encode(Object body, Message ctx) throws Exception {
        Map<String, Object> map = toMap(body);
        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
        Element root = doc.createElement(String.valueOf(map.getOrDefault(ROOT, "root")));
        doc.appendChild(root);
        map.forEach((k, v) -> {
            if (!ROOT.equals(k) && v != null) {
                Element e = doc.createElement(k);
                e.setTextContent(v.toString());
                root.appendChild(e);
            }
        });
        Transformer t = TransformerFactory.newInstance().newTransformer();
        t.setOutputProperty(OutputKeys.INDENT, "yes");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        t.transform(new DOMSource(doc), new StreamResult(out));
        return out.toByteArray();
    }

    /**
     * Map bodies are written as-is. Any other object (e.g. Person) is converted to a flat map with Jackson
     * (same field names and date format as the JSON output); the root element is the class name: Person -> person.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> toMap(Object body) {
        if (body instanceof Map<?, ?> m) return (Map<String, Object>) m;
        Map<String, Object> map = new LinkedHashMap<>();
        String name = body.getClass().getSimpleName();
        map.put(ROOT, Character.toLowerCase(name.charAt(0)) + name.substring(1));
        map.putAll(mapper.convertValue(body, Map.class));
        return map;
    }
}
