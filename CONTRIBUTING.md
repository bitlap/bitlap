## 构建环境

1. Scala 3.x、Scala 2.13.x
2. Kotlin 1.8.x
3. Java 11+ （Amazon Corretto JDK 11 或 Open JDK 17）
4. Mac、Linux、Windows（不支持在Windows上读写数据）
5. Docker

## 模块概述

***核心模块依赖关系***

![](./bitlap-structure.png)

| 模块             | 技术栈                               | 说明                                          |
| ---------------- | ------------------------------------ | --------------------------------------------- |
| bitlap-cli       | scala 3.x、zio-cli、 sqlline         | 交互式命令行实现                              |
| bitlap-client    | scala 3.x、zio-grpc                  | JDBC 和 Client 实现                           |
| bitlap-network   | scala 3.x、zio 2.x                   | 网络 IO 抽象                                  |
| bitlap-server    | scala 3.x、jraft、zio-grpc、zio-http | RPC server 实现、raft 集成实现、HTTP API 实现 |
| bitlap-core      | kotlin、calcite、parquet             | SQL解析、优化、任务、存储实现                 |
| bitlap-spark3    | scala 2.13.x、spark 3.x              | 与 spark3 集成                                |
| bitlap-common    | kotlin、 RoaringBitmap               | 公共模块、bitmap 封装                         |
| bitlap-testkit   | scala 3.x、javafaker、rolls          | 测试工具和集成测试模块                        |
| bitlap-server-ui | scala 3.x、javafaker、rolls          | 可视化 SQL 执行页面 UI                        |

## docker运行

> tag就是版本号，如：0.4.0-SNAPSHOT
```
# 打包
cd docker;sh docker.sh 0.4.0-SNAPSHOT
# 运行 
docker run --name bitlap:0.4.0-SNAPSHOT -dit -p 23333:23333 -p 24333:24333 -p 22333:22333  bitlap:0.4.0-SNAPSHOT
```
- 访问 `http://localhost:22333`
