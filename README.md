FlinkStreamSQL
============
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

## 技术交流
- 招聘**Flink开发工程师**，如果有兴趣，请联系思枢【微信号ysqwhiletrue】，注明招聘<BR>
  Flink开发工程师JD要求：<BR>
  1.负责袋鼠云基于Flink的衍生框架数据同步flinkx和实时计算flinkstreamsql框架的开发；<BR>
  2.调研和把握当前最新大数据实时计算技术，将其中的合适技术引入到平台中，改善产品，提升竞争力；<BR>
  职位要求：<BR>
  1、本科及以上学历，3年及以上的Flink开发经验，精通Java，熟悉Scala、Python优先考虑；<BR>
  2、熟悉Flink原理，有基于Flink做过二次源码的开发，在github上贡献者Flink源码者优先；<BR>
  3、有机器学习、数据挖掘相关经验者优先；<BR>
  4、对新技术有快速学习和上手能力，对代码有一定的洁癖；<BR>
  加分项：<BR>
  1.在GitHub或其他平台上有过开源项目;<BR>
  可以添加本人微信号ysqwhiletrue，注明招聘，如有意者发送简历至sishu@dtstack.com<BR>
- 我们使用[钉钉](https://www.dingtalk.com/)沟通交流，可以搜索群号[**30537511**]或者扫描下面的二维码进入钉钉群
<div align=center>
     <img src=docs/images/streamsql_dd.jpg width=300 />
</div>

## 介绍
* 基于开源的flink，对其实时sql进行扩展
   * 自定义create table 语法（包括源表,输出表,维表）
   * 自定义create view 语法
   * 自定义create function 语法
   * 实现了流与维表的join
   * 支持原生FlinkSQL所有的语法
   * 扩展了输入和输出的性能指标到Task metrics

## 目录

[ 1.1 demo](docs/demo.md)  
[ 1.2 快速开始](docs/quickStart.md)  
[ 1.3 参数配置](docs/config.md)  
[ 1.4 支持的插件介绍和demo](docs/pluginsInfo.md)     
[ 1.5 指标参数](docs/newMetric.md)  
[ 1.6 自定义函数](docs/function.md)  
[ 1.7 自定义视图](docs/createView.md)

## 如何贡献FlinkStreamSQL

[pr规范](docs/pr.md)

## License
FlinkStreamSQL is under the Apache 2.0 license. See the [LICENSE](http://www.apache.org/licenses/LICENSE-2.0) file for details.    