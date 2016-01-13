package com.yxl.util;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 过滤掉非指定的tagId
 */
public class JsonFilterUtil {

    private static final Log LOG = LogFactory.getLog(JsonFilterUtil.class.getName());

    private static final String KEY_TAG = "tag";
    private static final String KEY_TAG_ID = "tagid";


    //年龄/25-34 性别/女 旅游酒店/国内游/中国省级行政单位名称/北京 旅游酒店/国内游/中国省级行政单位名称/四川 旅游酒店/国内游/中国省级行政单位名称/重庆
    public static final String filter(String input,int tagId){
        StringBuffer sb = new StringBuffer();
        try{
            JSONArray jsonArray = JSONArray.fromObject(input);
            for (Object object: jsonArray){
                JSONObject o = JSONObject.fromObject(object);
                // 如果包含 tag 则获取对应的 key
                if (o.containsKey(KEY_TAG) && o.containsKey(KEY_TAG_ID) && tagId == o.getInt(KEY_TAG_ID)){
                    sb.append(o.getString(KEY_TAG));
                    sb.append(" "); // 空格隔开
                }
            }
        }catch (Exception e){
            //ignore
        }
        return sb.toString();
    }


    public static final Integer match(String input,int tagId,String tagname){
        try{
            JSONArray jsonArray = JSONArray.fromObject(input);
            for (Object object: jsonArray){
                JSONObject o = JSONObject.fromObject(object);
                // 如果包含 tag,tagid 则比较相等
                if (o.containsKey(KEY_TAG) && o.containsKey(KEY_TAG_ID) && tagId == o.getInt(KEY_TAG_ID)){
                    if (StringUtils.isBlank(tagname)){
                        return 1;
                    }else {
                        // 如果存在多个 tagid == 0 时，需要逐一匹配
                        if (StringUtils.startsWithIgnoreCase(o.getString(KEY_TAG),tagname)){
                            return 1;
                        }else{
                            continue;
                        }
                    }
                }
            }
        }catch (Exception e){
            LOG.error("json happend error: ",e);
        }
        return 0;
    }




    public static void main(String[] args) {
//        String tmp = "[{\"tag\":\"汽车\",\"tagid\":10000000,\"weight\":67},{\"tag\":\"餐饮美食\",\"tagid\":11000000,\"weight\":66},{\"tag\":\"餐饮美食/川菜\",\"tagid\":11031100,\"weight\":41},{\"tag\":\"餐饮美食/烹饪和菜谱\",\"tagid\":11100000,\"weight\":47},{\"tag\":\"母婴亲子\",\"tagid\":12000000,\"weight\":42},{\"tag\":\"教育培训\",\"tagid\":13000000,\"weight\":57},{\"tag\":\"教育培训/留学\",\"tagid\":13120000,\"weight\":31},{\"tag\":\"医疗健康\",\"tagid\":14000000,\"weight\":72},{\"tag\":\"资讯\",\"tagid\":18000000,\"weight\":76},{\"tag\":\"资讯/社会时政\",\"tagid\":18010000,\"weight\":58},{\"tag\":\"资讯/科技资讯\",\"tagid\":18030000,\"weight\":20},{\"tag\":\"资讯/娱乐八卦\",\"tagid\":18040000,\"weight\":54},{\"tag\":\"资讯/网络奇闻\",\"tagid\":18050000,\"weight\":40},{\"tag\":\"资讯/军事\",\"tagid\":18060000,\"weight\":54},{\"tag\":\"金融财经\",\"tagid\":19000000,\"weight\":71},{\"tag\":\"金融财经/贵金属\",\"tagid\":19030000,\"weight\":34},{\"tag\":\"商务服务\",\"tagid\":20000000,\"weight\":52},{\"tag\":\"商务服务/法律咨询\",\"tagid\":20020000,\"weight\":45},{\"tag\":\"体育健身\",\"tagid\":21000000,\"weight\":67},{\"tag\":\"影视音乐\",\"tagid\":24000000,\"weight\":76},{\"tag\":\"影视音乐/音乐\",\"tagid\":24010000,\"weight\":65},{\"tag\":\"影视音乐/电影\",\"tagid\":24020000,\"weight\":56},{\"tag\":\"影视音乐/电视剧\",\"tagid\":24030000,\"weight\":49},{\"tag\":\"影视音乐/动漫\",\"tagid\":24040000,\"weight\":53},{\"tag\":\"影视音乐/综艺\",\"tagid\":24050000,\"weight\":55},{\"tag\":\"网络购物\",\"tagid\":25000000,\"weight\":72},{\"tag\":\"网络购物/团购\",\"tagid\":25070000,\"weight\":54},{\"tag\":\"花鸟萌宠\",\"tagid\":26000000,\"weight\":54},{\"tag\":\"旅游酒店\",\"tagid\":28000000,\"weight\":70},{\"tag\":\"\",\"tagid\":28010200,\"weight\":36},{\"tag\":\"\",\"tagid\":28030300,\"weight\":30},{\"tag\":\"\",\"tagid\":28030800,\"weight\":22},{\"tag\":\"旅游酒店/国内游\",\"tagid\":28070000,\"weight\":49},{\"tag\":\"家电数码\",\"tagid\":29000000,\"weight\":49},{\"tag\":\"家电数码/手机\",\"tagid\":29060000,\"weight\":41},{\"tag\":\"家电数码/电脑\",\"tagid\":29070000,\"weight\":45},{\"tag\":\"求职创业\",\"tagid\":30000000,\"weight\":66},{\"tag\":\"游戏\",\"tagid\":31000000,\"weight\":67},{\"tag\":\"游戏/PC游戏\",\"tagid\":31030000,\"weight\":22},{\"tag\":\"游戏/网页游戏\",\"tagid\":31040000,\"weight\":67},{\"tag\":\"服饰鞋包\",\"tagid\":32000000,\"weight\":50},{\"tag\":\"个护美容\",\"tagid\":33000000,\"weight\":65},{\"tag\":\"房产\",\"tagid\":34000000,\"weight\":73},{\"tag\":\"建材家居\",\"tagid\":35000000,\"weight\":35},{\"tag\":\"休闲爱好\",\"tagid\":36000000,\"weight\":36},{\"tag\":\"休闲爱好/收藏\",\"tagid\":36050000,\"weight\":34},{\"tag\":\"年龄/25-34\",\"tagid\":0,\"weight\":72},{\"tag\":\"性别/女\",\"tagid\":0,\"weight\":90},{\"tag\":\"旅游酒店/国内游/中国省级行政单位名称/北京\",\"tagid\":0,\"weight\":50},{\"tag\":\"旅游酒店/国内游/中国省级行政单位名称/四川\",\"tagid\":0,\"weight\":21},{\"tag\":\"旅游酒店/国内游/中国省级行政单位名称/重庆\",\"tagid\":0,\"weight\":27}]";
//        System.out.println(filter(tmp,0));
        String tmp = "[{\"tag\":\"母婴亲子\",\"tagid\":12000000,\"weight\":43},{\"tag\":\"母婴亲子/孕婴保健\",\"tagid\":12140000,\"weight\":43},{\"tag\":\"金融财经\",\"tagid\":19000000,\"weight\":43},{\"tag\":\"金融财经/贵金属\",\"tagid\":19030000,\"weight\":43},{\"tag\":\"旅游酒店\",\"tagid\":28000000,\"weight\":43},{\"tag\":\"\",\"tagid\":28030800,\"weight\":43},{\"tag\":\"旅游酒店/自助游\",\"tagid\":28050100,\"weight\":43},{\"tag\":\"旅游酒店/跟团游\",\"tagid\":28050200,\"weight\":43},{\"tag\":\"\",\"tagid\":28050300,\"weight\":43},{\"tag\":\"旅游酒店/出国游\",\"tagid\":28090000,\"weight\":43},{\"tag\":\"\",\"tagid\":28090200,\"weight\":43},{\"tag\":\"\",\"tagid\":28090600,\"weight\":43},{\"tag\":\"年龄/45-54\",\"tagid\":0,\"weight\":53},{\"tag\":\"性别/女\",\"tagid\":0,\"weight\":77}]";
        System.out.println(match(tmp,0,"年龄/45-54"));
        System.out.println(match(tmp,0,"年龄/20-25"));
        System.out.println(match(tmp,0,"性别/女"));
        System.out.println(match(tmp,0,"性别"));
        System.out.println(match(tmp,0,"性别/男"));
        System.out.println(match(tmp,0, null));
    }

}
