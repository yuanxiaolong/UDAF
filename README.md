包含了UDF 和 UDAF 的例子

用于 统计分析 百度深度学习 tag标签

---

## UDF

1. 把data导入到 hive表中，根据关键词解析 json，并把指定 tagid 的「标签」提取出来，用空格分隔


```
insert overwrite local directory '/usr/local/datacenter/hive/exportData/' select f_tag(tag,0) from site_uuid2tag where  tag like '%"tagid":0%' and dt=20160109;
```

2.执行spark-shell，用于执行wordcount，并把结果保存到文本文件里

```
val textFile = sc.textFile("file:///root/app20160110.txt")
val lengthCounts = textFile.flatMap(_.split(" ").map(word => (word,1))).reduceByKey(_ + _)
lengthCounts.repartition(1).saveAsTextFile("/test/tag-app-20160110")
```

3.人工把文件解析，去掉左右括号，并导入到 hive 里以便 根据 sql 获取指定子集

4.导入到 tableau 进行绘图


---

## UDAF

1.先把data文件导入到hive表里，把打包后的jar 加入到hive里，并创建函数

```
add jar /root/xiaolong.udf-0.0.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION f_tag_sum AS 'com.yxl.UDAFUuid2TagSum';
```

2.统计结果，其中 第一个参数为 原始json列，第二个参数为需要查找的tagid，第三个参数（可选）为，tagid相同时，匹配指定的tagname

```
select f_tag_sum(tag,0,'性别/女') from tag.src_tag where dt=20160109;
select f_tag_sum(tag,0) from tag.src_tag where dt=20160109;
```


