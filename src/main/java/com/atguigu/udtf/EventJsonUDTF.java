package com.atguigu.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;

public class EventJsonUDTF extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //定义UDTF返回值类型和名称
        ArrayList<String> fieldName = new ArrayList<>();

        ArrayList<ObjectInspector> fieldType = new ArrayList<>();

        fieldName.add("event_name");
        fieldName.add("event_json");

        fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldType);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        //传入的是json array  => UDF 传入et
        String input = objects[0].toString();

        //合法校验
        if (StringUtils.isBlank(input)) {
            return;
        } else {
            JSONArray ja = new JSONArray(input);
            if (ja == null) {
                return;
            } else {
                //循环遍历array当中每一个元素，封装成返回 事件名称和事件内容
                for (int i = 0; i < ja.length(); i++) {
                    String[] result = new String[2];
                    /*
                    {"ett":"1583762198392",
                    "en":"praise",
                    "kv":{
                    "target_id":1,
                    "id":7,
                    "type":2,
                    "add_time":"1583718570667",
                    "userid":4}
                    }
                     */
                    try {

                        result[0] = ja.getJSONObject(i).getString("en");
                        result[1] = ja.getString(i);
                    } catch (JSONException e) {
                        continue;
                    }
                    //往外循环写出
                    forward(result);
                }
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
