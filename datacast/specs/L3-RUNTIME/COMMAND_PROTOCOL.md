---
version: 1.0.0
---

# L3: [protocol] Datacast Command Execution Protocol

> Type: protocol | interface
> Layer: Stability Layer (Command Handling)

## [overview] Command_Execution_Overview

Datacast 作为独立自治的传感器，通过 **Heartbeat Response** 通道接收并执行标准化指令。本协议定义了 Datacast 支持的指令集、Payload 格式及预期的执行行为。

> **原则**: Datacast **不关心** 指令是谁发出的（Aggregator, Admin CLI, or other Consumers），只关心 **指令格式** 和 **执行逻辑**。

---

## [mechanism] Command_Transport_Mechanism

- **Channel**: Heartbeat Response
- **Format**: JSON List (Batch Commands)
- **Priority**: High (Immediate Dispatch)

```json
// Heartbeat Response Body
{
  "commands": [
    {
      "type": "scan",
      "id": "cmd_12345",
      "payload": { ... }
    }
  ]
}
```

---

## [interface] Supported_Command_Set

### 3.1 Data Complement (`scan`)

**Rationale**: Provide a mechanism for consumers to explicitly trigger a scan of a specific directory to ensure data completeness on-demand.

```python
# Command format
{
    "command": "scan",
    "params": {
        "path": "/absolute/path/to/scan",
        "recursive": true
    }
}
```
用于填补数据盲区或强制刷新特定路径的状态。

- **Input Payload**:
  ```json
  {
    "type": "scan",
    "path": "/data/logs",
    "recursive": true,
    "depth": 3  // optional
  }
  ```

- **Execution Behavior**:
  1. **Bypass Check**: 忽略 `is_realtime_ready` 状态检查（即使连接未就绪也强制执行）。
  2. **Direct IO**: 立即触发 `Scanner` 对指定 `path` 进行遍历。
  3. **Event Generation**: 生成 `ON_DEMAND_JOB` 类型的事件。
  4. **Direct Push**: 结果通常绕过 Batch Buffer，直接推送到 Sender 队列以保证低延迟（取决于实现配置）。

### 3.2 Process Control (`reload`)

用于热重载配置。

- **Input Payload**:
  ```json
  {
    "type": "reload",
    "config_version": "v2026.02.16" // optional verification
  }
  ```

- **Execution Behavior**:
  1. 触发 `SIGHUP` 信号模拟或直接调用 `ConfigLoader.reload()`。
  2. **Atomic Diff**: 计算配置差异 (Diff)。
  3. **Apply**: 停止移除的 Pipe，启动新增的 Pipe。
  4. **Feedback**: 在下一次 Heartbeat 中报告新的 `config_hash`。

### 3.3 Lifecycle Management (`upgrade` / `restart`)

用于软件升级或进程重启。

- **Input Payload**:
  ```json
  {
    "type": "upgrade",
    "image": "Datacast:v2.0.0", // container image or binary url
    "signature": "sha256:..."
  }
  ```

- **Execution Behavior**:
  1. **Self-Replacement**: 下载新二进制/镜像。
  2. **Hot-Swap**: 使用 `execve` (Linux) 或替换容器镜像重启。
  3. **Recovery**: 重启后自动读取持久化配置并重连。

---

## [error_handling] Protocol_Error_Handling

**Rationale**: Ensure the "umbilical cord" remains intact even when malformed or failed commands are encountered.

- **Unknown Command**: Log warning, ignore, and continue processing other commands.
- **Execution Failure**: Log error (local), report via `system_error` stats in next heartbeat.
- **Timeout**: Command execution must be non-blocking (async). Long-running tasks (`scan`) must yield execution.
