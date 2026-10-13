# Cloud Computing Exp05: Paxos Distributed Consensus Implementation & Docker Deployment

本项目是《云计算技术》课程实验五的实现，旨在通过 Python 从零实现一个基础的 **Paxos 分布式共识协议**，并通过 **Docker 容器化** 模拟分布式集群环境，验证分布式系统中的安全性（Safety）与共识达成机制。

---

## 📂 项目目录结构

* `client/` - 客户端脚本，用于向集群节点发起提案（Propose）。
* `node/` - Paxos 节点核心逻辑（包含 Prepare、Promise、Accept、Accepted 两阶段共识算法与 TCP 网络通信）。
* `common/` - 公共工具与消息定义。
* `Dockerfile` - 容器镜像构建文件。
* `docker-compose.yml` - 3 节点分布式集群编排配置（支持网络互联与端口映射 8001-8003）。

*(注：本地运行时会自动生成 `data/` 目录用于持久化存储节点状态与 JSON 实验证据，该目录已通过 `.gitignore` 忽略，不纳入版本控制)*

---

## 🚀 快速开始 / 运行指南

### 1. Docker 容器化集群部署
在项目根目录下执行以下命令，启动 3 个 Paxos 节点：
```bash
docker compose up