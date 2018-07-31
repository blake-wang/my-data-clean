package com.ijunhai.common;

import org.bson.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

public class XMLUtil {

    public static Document xml2bson(ByteArrayOutputStream baos) throws JDOMException, IOException {
        Document json = new Document();
        InputStream is = new ByteArrayInputStream(baos.toByteArray());
        SAXBuilder sb = new SAXBuilder();
        org.jdom2.Document doc = sb.build(is);
        Element root = doc.getRootElement();
        json.put(root.getName(), iterateElement(root));
        return json;

    }

    private static Document iterateElement(Element element) {
        List node = element.getChildren();
        Element et;
        Document obj = new Document();
        List list;
        for (int i = 0; i < node.size(); i++) {
            list = new LinkedList();
            et = (Element) node.get(i);
            if (et.getTextTrim().equals("")) {
                if (et.getChildren().size() == 0) {
                    continue;
                }
                if (obj.containsKey(et.getName())) {
                    list = (List) obj.get(et.getName());
                }
                list.add(iterateElement(et));
                obj.put(et.getName(), list.get(0));
            } else {
                if (obj.containsKey(et.getName())) {
                    list = (List) obj.get(et.getName());
                }
                list.add(et.getTextTrim());
                obj.put(et.getName(), list.get(0));
            }
        }
        return obj;
    }
}
