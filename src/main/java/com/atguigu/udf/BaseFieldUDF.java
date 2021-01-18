package com.atguigu.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {
    public String evaluate(String line, String key) {
        // 切割
        String[] log = line.split("\\|");

        //合法性判断
        if (log.length != 2 || StringUtils.isBlank(log[1].trim())) {
            return "";
        }

        JSONObject json = new JSONObject(log[1].trim());

        String result = "";

        //根据传入的key取值
        if ("st".equals(key)) {
            //返回服务器时间
            result = log[0].trim();
        } else if ("et".equals(key)) {
            //获取事件数组
            if (json.has("et")) {
                result = json.getString("et");
            }
        } else {
            // 获取cm对应的value值
            JSONObject cm = json.getJSONObject("cm");
            if(cm.has(key)){
                result = cm.getString(key);
            }
        }

        return result;
    }

    public static void main(String[] args) throws JSONException {
        String line = "1583770459133|{\"cm\":{\"ln\":\"-103.2\",\"sv\":\"V2.5.4\",\"os\":\"8.2.9\",\"g\":\"PYF5YF83@gmail.com\",\"mid\":\"2\",\"nw\":\"3G\",\"l\":\"pt\",\"vc\":\"19\",\"hw\":\"1080*1920\",\"ar\":\"MX\",\"uid\":\"2\",\"t\":\"1583693243232\",\"la\":\"-35.6\",\"md\":\"HTC-3\",\"vn\":\"1.0.7\",\"ba\":\"HTC\",\"sr\":\"W\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1583717388920\",\"en\":\"display\",\"kv\":{\"goodsid\":\"0\",\"action\":\"1\",\"extend1\":\"1\",\"place\":\"1\",\"category\":\"92\"}},{\"ett\":\"1583707240969\",\"en\":\"newsdetail\",\"kv\":{\"entry\":\"2\",\"goodsid\":\"1\",\"news_staytime\":\"18\",\"loading_time\":\"4\",\"action\":\"2\",\"showtype\":\"0\",\"category\":\"94\",\"type1\":\"\"}},{\"ett\":\"1583747586768\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"0\",\"action\":\"3\",\"extend1\":\"\",\"type\":\"2\",\"type1\":\"325\",\"loading_way\":\"1\"}},{\"ett\":\"1583723138615\",\"en\":\"ad\",\"kv\":{\"activityId\":\"1\",\"displayMills\":\"54256\",\"entry\":\"1\",\"action\":\"5\",\"contentType\":\"0\"}},{\"ett\":\"1583695922061\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1583739364465\",\"action\":\"2\",\"type\":\"3\",\"content\":\"\"}},{\"ett\":\"1583710974971\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"1\"}},{\"ett\":\"1583683698633\",\"en\":\"error\",\"kv\":{\"errorDetail\":\"java.lang.NullPointerException\\\\n at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:7 2)\\\\n at cn.lift.dfdf.web.AbstractBaseController.validInbound\",\"errorBrief\":\"at cn.lift.dfdf.web.AbstractBaseController.validInbound(AbstractBaseController.java:72) \"}},{\"ett\":\"1583762198392\",\"en\":\"praise\",\"kv\":{\"target_id\":1,\"id\":7,\"type\":2,\"add_time\":\"1583718570667\",\"userid\":4}}]} ";
        String result = new BaseFieldUDF().evaluate(line, "et");
        System.out.println(result);
    }
}

