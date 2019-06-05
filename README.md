# flink-sql-demo

[官方文档:https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/table/common.html](https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/table/common.html)


https://yq.aliyun.com/articles/457438


创建Java应用:

```
mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-quickstart-java \
    -DarchetypeVersion=1.8.0 \
    -DgroupId=cn.dubby \
    -DartifactId=flink-sql-demo \
    -Dversion=0.1 \
    -Dpackage=cn.dubby.flink.sql.demo \
    -DinteractiveMode=false
```