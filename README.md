# 《云计算技术》实验五：Chubby 类开源项目分析与 Paxos 协议实现

## 一、 实验概述
本实验参考 **etcd** (https://github.com/etcd-io/etcd) 的架构思想，使用 Python 从零手写实现一套轻量级的 Paxos 共识协议原型。系统支持 Prepare/Promise、Accept/Accepted、多数派确认、超时重试以及节点崩溃恢复机制，并通过自定义 TCP 报文在多节点间进行通信。

## 二、 项目目录结构
- 
ode/：Paxos 节点核心逻辑、状态机及网络服务
- client/：客户端请求发送与集群交互模块
- common/：自定义 TCP 报文编码/解码、日志及配置管理
- 	ests/：单元测试与集成测试脚本
- screenshots/：实验过程与结果截图
- docker-compose.yml：多节点本地容器编排配置
- equirements.txt：项目依赖声明

## 三、 运行与验证
详见后续各阶段更新。

## 四、 实验截图
| 截图说明 | 文件路径 |
| :--- | :--- |
| Docker 集群启动状态 | screenshots/docker_cluster_running.png |
| Paxos 共识达成与日志复制 | screenshots/paxos_consensus.png |
| 节点故障与崩溃恢复 | screenshots/node_recovery.png |
