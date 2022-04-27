| Project Stage | CI              | Codecov                                   |
| ------------- | --------------- | ----------------------------------------- |
| ![Stage]      | ![CI][Badge-CI] | [![codecov][Badge-Codecov]][Link-Codecov] |

| Maven Central    | Nexus Snapshots    |     Chat  |
| ---------------- | ------------------ | --------- |
| [![Maven Central][Badge-Maven]][Link-Maven] | [![Sonatype Nexus (Snapshots)][Badge-Snapshots]][Link-Snapshots] | [![Chat][Badge-Chat]][Link-Chat]  |


# bitlap

## Architecture

![](http://ice-img.flutterdart.cn/2021-08-01-165808.png)

## Quick Start

* [Test Data File Example](https://docs.google.com/spreadsheets/d/13KNvNTGYRjdPSsp9PSrIMU_2uKqKkJa-BgL4Q236Ky0/edit?usp=sharing)

## How to contribute

***Environment***

* [IDEA Plugin Scala-Macro-Tools](https://github.com/bitlap/scala-macro-tools)

***Build***

```sh
./mvnw clean package -DskipTests
```

* make tarball

```sh
./dev/make-tarball.sh
```

## LICENSE

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
[Badge-CI]: https://github.com/bitlap/bitlap/actions/workflows/java8.yml/badge.svg
[Badge-Maven]: https://img.shields.io/maven-central/v/org.bitlap/bitlap
[Badge-Chat]: https://badges.gitter.im/bitlap-org/bitlap.svg
[Badge-Codecov]: https://codecov.io/gh/bitlap/bitlap/branch/main/graph/badge.svg?token=9XJ2LC2K8M
[Badge-Snapshots]: https://img.shields.io/nexus/s/org.bitlap/bitlap-core?server=https%3A%2F%2Fs01.oss.sonatype.org

[Link-Chat]: https://gitter.im/bitlap-org/bitlap?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge
[Link-Codecov]: https://codecov.io/gh/bitlap/bitlap
[Link-Maven]: https://search.maven.org/search?q=g:%22org.bitlap%22%20AND%20a:%22bitlap%22
[Link-Snapshots]: https://s01.oss.sonatype.org/content/repositories/snapshots/org/bitlap/
