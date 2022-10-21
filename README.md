# bitlap

> 龟速开发中，感兴趣的可以star一下

| Project Stage | CI              | Codecov                                   |
|---------------|-----------------|-------------------------------------------|
| ![Stage]      | ![CI][Badge-CI] | [![codecov][Badge-Codecov]][Link-Codecov] |

| Maven Central                               | Nexus Snapshots                                                  | Discord                                   |
|---------------------------------------------|------------------------------------------------------------------|-------------------------------------------|
| [![Maven Central][Badge-Maven]][Link-Maven] | [![Sonatype Nexus (Snapshots)][Badge-Snapshots]][Link-Snapshots] | [![Discord][Badge-Discord]][Link-Discord] |

## 架构

![](http://ice-img.flutterdart.cn/2021-08-01-165808.png)

## 环境

1. Scala 2.13.x
2. Kotlin 1.7.x
3. Java 11+
4. Mac或Linux

## 快速开始

1. 安装IDEA插件（可选） [IDEA Plugin Scala-Macro-Tools](https://github.com/bitlap/scala-macro-tools)
2. 下载源码 `git clone https://github.com/bitlap/bitlap.git`
3. 找到类 `bitlap-server/src/main/scala/../.../BitlapServer.scala`，然后在IDEA中运行该main方法。（Java 9以上需要JVM参数：`--add-exports
   java.base/jdk.internal.ref=ALL-UNNAMED`）
4. 浏览器中请求 `http://localhost:8080/init` ，以初始化数据。（使用的数据在`bitlap-server/src/main/resources/simple_data.csv`）
   - 目前仅能通过classpath导入csv，所以csv在server中，想修改csv得这样操作：
        1. 使用`bitlap-testkit/src/test/scala/.../FakeDataUtil.scala`
           工具生成csv文件，生成的文件在`bitlap-testkit/src/test/resources/.../simple_data.csv`
        2. 将生成的csv拷贝到`bitlap-server/src/main/resources/simple_data.csv`
        3. `http://localhost:8080/init` ，再次初始化数据
5. 浏览器中请求：`http://localhost:8080/sql` ，该接口使用固定的SQL查数并返回。

## 通过client查询

添加依赖

```scala
libraryDependencies += (
  "org.bitlap" % "bitlap-client" % <version>,
  "org.bitlap" % "smt-common" % <version>
)

// 若运行时报错 找不到grpc某个类的话，加这几行试试，保证io.grpc的所有组件都是一个版本！
dependencyOverrides ++= Seq(
  "io.grpc" % "grpc-core" % "1.46.0",
  "io.grpc" % "grpc-netty" % "1.46.0"
)
```

使用JDBC

```scala
Class.forName(classOf[org.bitlap.Driver].getName)
val statement = DriverManager.getConnection("jdbc:bitlap://localhost:23333/default").createStatement()
statement.execute(
  s"""select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
     |from table_test
     |where _time >= 0
     |group by _time""".stripMargin)

val rowSet: ResultSet = statement.getResultSet

// ResultSetTransformer是个工具类，会把ResultSet提取为Seq，其中GenericRow4表示结果是四列，每个类型需要指定，五列就是GenericRow5，以此类推。
val ret1: Seq[GenericRow4[Long, Double, Double, Long]] = ResultSetTransformer[GenericRow4[Long, Double, Double, Long]].toResults(rowSet)
println(ret1)
```

## 许可

```
MIT License

Copyright (c) 2022 bitlap.org

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

[Stage]: https://img.shields.io/badge/Project%20Stage-Development-yellowgreen.svg

[Badge-CI]: https://github.com/bitlap/bitlap/actions/workflows/ci.yml/badge.svg

[Badge-Maven]: https://img.shields.io/maven-central/v/org.bitlap/bitlap

[Badge-Discord]: https://img.shields.io/discord/968687999862841384

[Badge-Codecov]: https://codecov.io/gh/bitlap/bitlap/branch/main/graph/badge.svg?token=9XJ2LC2K8M

[Badge-Snapshots]: https://img.shields.io/nexus/s/org.bitlap/bitlap-core?server=https%3A%2F%2Fs01.oss.sonatype.org

[Link-Discord]: https://discord.com/invite/vp5stpz6eU

[Link-Codecov]: https://codecov.io/gh/bitlap/bitlap

[Link-Maven]: https://search.maven.org/search?q=g:%22org.bitlap%22%20AND%20a:%22bitlap%22

[Link-Snapshots]: https://s01.oss.sonatype.org/content/repositories/snapshots/org/bitlap/
