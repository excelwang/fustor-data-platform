# Fustor datacast 服务

Fustor datacast 是一款轻量、可扩展的数据采集与推送工具。它负责监听数据源变更，并将其实时推送到 Fustor fustord 服务。

## 安装

```bash
pip install datacast
# 安装文件系统源驱动
pip install fustor-source-fs
```

### 1. 配置

Fustor datacast 使用一个主目录来存放配置和状态。
*   **默认路径**: `~/.fustor`
*   **自定义路径**: 设置 `FUSTOR_HOME` 环境变量。

datacast 的核心配置文件位于 Fustor 主目录下的 `datacast-config.yaml`。你需要定义 `sources` (数据源)、`senders` (推送目标) 和 `pipes` (同步任务)。

### 1. 配置 Source (数据源)

以文件系统 (FS) 为例：

```yaml
sources:
  - id: "my-local-files"       # 唯一 ID
    type: "fs"                 # 驱动类型
    config:
      uri: "/data/research"    # 监控的绝对路径
      driver_params:
        # 可选：文件过滤模式
        file_pattern: "*"      
```

### 2. 配置 Sender (推送目标)

通常推送到 fustord 服务：

```yaml
senders:
  - id: "to-fustord"            # 唯一 ID
    type: "fustord"             # 驱动类型
    config:
      # fustord 服务的 Ingest API 基准地址
      endpoint: "http://localhost:8102"
      # 从 fustord 管理员处获取的 API Key，用于鉴权
      credential: "YOUR_API_KEY_HERE"
```

### 3. 配置 Pipe (数据管道任务)

将 Source 和 Sender 绑定：

```yaml
pipes:
  - id: "phase-files-to-fustord"
    source_id: "my-local-files"
    sender_id: "to-fustord"
    enabled: true              # 设置为 true 以自动启动
```

## 命令指南

*   **启动服务**: `datacast start -D` (后台运行) 或 `datacast start` (前台运行)
*   **停止服务**: `datacast stop`
*   **查看状态**: 访问 `http://localhost:8100` 查看 Web 控制台。

## 数据可靠性保证 (Data Reliability)

datacast 遵循 **"瘦 datacast 感知 + 胖 fustord 裁决"** 架构。

### Leader/Follower 模式

| 角色 | Realtime Sync Phase | Snapshot Sync Phase | Audit Sync Phase | Sentinel Sweep |
|------|---------------|---------------|------------|----------------|
| **Leader** | ✅ | ✅ | ✅ | ✅ |
| **Follower** | ✅ | ❌ | ❌ | ❌ |

- **先到先得**：第一个建立 Session 的 datacast 成为 Leader
- **故障转移**：仅当 Leader 心跳超时后，fustord 才释放 Leader 锁

### 消息类型 (`message_source`)

| 类型 | 说明 |
|------|------|
| `realtime` | inotify 事件，优先级最高 |
| `snapshot` | datacast 启动时全量扫描 |
| `audit` | 定时审计，发现盲区变更 |

详见 `docs/CONSISTENCY_DESIGN.md`。

## 更多文档

*   **驱动开发**: 详见 `docs/driver_design.md`
*   **一致性设计**: 详见 `docs/CONSISTENCY_DESIGN.md`
