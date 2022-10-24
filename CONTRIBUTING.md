## 环境

1. Scala 2.13.x
2. Kotlin 1.7.x
3. Java 11+
4. Mac、Linux、Windows（需要使用profile）

## 快速开始

1. 安装IDEA插件（可选） [IDEA Plugin Scala-Macro-Tools](https://github.com/bitlap/scala-macro-tools)
2. 下载源码 `git clone https://github.com/bitlap/bitlap.git`
3. 找到类 `bitlap-server/src/main/scala/../.../BitlapServer.scala`，然后在IDEA中运行该main方法。（Java 9以上需要JVM参数：`--add-exports
   java.base/jdk.internal.ref=ALL-UNNAMED`）
4. 浏览器中请求 `http://localhost:8080/init` ，以初始化数据。（使用的数据在`bitlap-server/src/main/resources/simple_data.csv`）
5. 浏览器中请求：`http://localhost:8080/sql` ，该接口使用固定的SQL查数并返回。


## 修改默认数据
- 目前仅能通过classpath导入csv，所以csv在server中，想修改csv得这样操作：
  1. 使用`bitlap-testkit/src/test/scala/.../FakeDataUtil.scala`
     工具生成csv文件，生成的文件在`bitlap-testkit/src/test/resources/.../simple_data.csv`
  2. 将生成的csv拷贝到`bitlap-server/src/main/resources/simple_data.csv`
  3. `http://localhost:8080/init` ，再次初始化数据
