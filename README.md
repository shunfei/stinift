Stinift
====
## 介绍
​​
stinift是[舜飞科技](http://www.sunteng.com)开发的并且内部广泛使用的一款异构数据传输工具

**设计动机**

* [舜飞科技](http://www.sunteng.com)内部业务元数据存放在mongo，统计数据存放在mysql，标签数据存放在redis，还有其他的数据库共同支撑起公司业务。这些数据为了分析方便，需要建设一个数据仓库集中存放各类数据，这些数据来自不同的数据库。这些数据同步的特点是数据量小，镜像同步。
* 报表类数据需要从数据仓库中导出到查询数据库，比如mysql，mongo。
* json配置、csv数据文件，存放到数据库，经常需要写存放代码

**特点**

* 简单的配置
* 插件化工具开发
* 不同数据库之间互相导入导出


## 下载、编译

```
git clone git@github.com:shunfei/stinift.git
cd stinift
mvn clean package
ls distribution/target/distribution-*.zip
```

生成的 `distribution/target/distribution-*.zip` 就是可以部署的文件

## 配置

查看 `conf` 目录，`env.sh` 设置 jvm 运行环境，比如设置hadoop 用户
`stinift.properties` 设置 stinift 参数，比如 `stinift.plugin.classes` 添加插件
`log4j.xml` log 设置

## 运行

```
bin/stinift.sh example/test.json
```

会生成两个文件 `file.out_0`, `file.out_0`，则部署成功

## 插件

目前支持 file, hbase, hive, mongodb, mysql. 具体使用配置参考 `example/`

* 使用 hive, hbase 等插件需要把 hadoop 的 core-site.xml, hdfs-site.xml 放到 `conf` 目录
* 使用 hive writer，stinift 会先创建一个名为 `stinift.<randomstr>` 的临时表，数据目录位于 `/user/stinift/<randomstr>`，数据先以 csv 格式写入这个目录，然后再导入真正的目标表。
* 具体细节请查看代码，很简单。