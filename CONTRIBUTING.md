## 构建环境

1. Scala 2.13.x
2. Kotlin 1.7.x
3. Java 11+
4. Mac、Linux、Windows（需要使用profile）

## 模块概述

***核心模块依赖关系***

![](./bitlap-structure-2.png)

- `bitlap-cli`       交互式命令行实现。技术栈：scala、zio-cli
- `bitlap-client`    JDBC和RPC client实现。技术栈：scala、zio-grpc
- `bitlap-network`   RPC client和server的抽象定义。技术栈：scala、zio
- `bitlap-server`    RPC server实现、raft server实现、HTTP server实现。技术栈：scala、zio-raft、zio-grpc、zio-http(zhttp)
- `bitlap-core`      SQL解析、优化、任务、存储。技术栈：kotlin
- `bitlap-spark3`    与spark3集成。技术栈：spark3、scala
- `bitlap-common`    公共模块。技术栈：kotlin
- `bitlap-testkit`   测试工具和集成测试模块。技术栈：scala、javafaker
- `bitlap-server-ui` 可视化SQL执行页面的UI。


## 快速开始

> windows上无法运行

1. 安装IDEA插件（可选） [IDEA Plugin Scala-Macro-Tools](https://github.com/bitlap/scala-macro-tools)
2. 下载源码 `git clone https://github.com/bitlap/bitlap.git`
3. `mvn package -Pwebapp`
4. 运行 `org.bitlap.server.BitlapServer`，Java9以上需要参数 `--add-exports java.base/jdk.internal.ref=ALL-UNNAMED`
5. 浏览器中访问首页 `http://localhost:18081`，会基于`bitlap-server/src/main/resources/simple_data.csv` 创建一个 `bitlap_test_table`
   1. 每次访问首页都会重新初始化数据！
6. 以 `bitlap_test_table` 开始查询：
```sql
select _time, sum(vv) as vv, sum(pv) as pv, count(distinct pv) as uv
       from bitlap_test_table
       where _time >= 0
       group by _time
```

## 打包

1. 打包脚本：`dev/make-tarball.sh` （以Java11为准）
2. 在Java11上使用 `/bin/bitlap` 运行bitlap，需要添加虚拟机参数：`--add-exports xx`、`--add-opens xx`，请参考`bin/bitlap-env.sh`中的`# JDK11="......"`（在Java8上请删掉`JDK11`这个参数 ）。