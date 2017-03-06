# 百度Kafka服务Go样例

百度Kafka是托管的Kafka消息服务。完全兼容开源Kafka。本样例展示如何使用[sarama](https://github.com/Shopify/sarama)客户端访问百度Kafka服务。

## 环境要求

- [Go 1.8, 1.7](https://golang.org/)
- [sarama 1.10以上(支持kafka 0.10)](https://github.com/tcnksm-sample/sarama)

## 准备工作

准备工作的细节请参考[BCE官网帮助文档](https://cloud.baidu.com/doc/Kafka/QuickGuide.html)

1. 在管理控制台中创建好主题，并获取主题名称topic_name。
2. 在管理控制台中下载您的kafka-key.zip，包含Go程序使用的`client.pem`，`client.key`，`ca.pem`。
3. 用上一步的文件替换样例代码中的`client.pem`、`client.key`以及`ca.pem`。

## 运行样例代码

### 安装sarama及其依赖

    go get github.com/Shopify/sarama

### 构建与执行

将samples加入`GOPATH`，分别在consumer、producer目录下执行：

    go build

## 运行样例代码

在consumer目录下

    consumer -topic=topic_name

在producer目录下

    producer -topic=topic_name

## 参考链接

- [百度Kafka产品介绍](https://cloud.baidu.com/product/kafka.html)
- [Kafka](http://kafka.apache.org/)
- [sarama](https://github.com/tcnksm-sample/sarama)
- [Go Getting Started](https://golang.org/doc/install)