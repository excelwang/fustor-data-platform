# L3: [Sensord] Package Structure

> **Status**: Active
> **Layer**: Implementation (L3)
> **Parent**: L2-ARCHITECTURE.COMPONENTS.CORE

---

## [definition] Core_Sensord_Packages

**Component**: sensord engine package organization.

```
sensord/                             # sensord 守护进程主包
├── src/sensord/
114: │   ├── boot/                        # 引导与环境初始化
115: │   ├── core/                        # 运行时调度 (PipeManager)
116: │   └── cmd/                         # CLI 指令 (reload, start, version)

sensord-core/                         # 核心抽象层 (SDK)
├── src/sensord_core/
120: │   ├── common/                      # 通用工具 (logging, daemon, paths)
121: │   ├── event/                       # 统一事件模型 (EventBase, EventType)
122: │   ├── pipe/                        # 管道与 Handler ABC (含 BasePipeManager)
123: │   ├── stability/                   # 共享稳定性 Mixins
124: │   ├── transport/                   # Sender ABC
125: │   ├── clock/                       # 影子参考系时钟算法
126: │   └── config/                      # Pydantic 配置模型
```

## [definition] Source_and_Sender_Drivers

**Component**: Data extraction implementation packages.

```
sensord-source-fs/                   # 文件系统 Source Driver
sensord-source-sql/                  # 数据库 Source Driver (待选)
sensord-sender-http/                 # HTTP 协议发送驱动
sensord-sender-grpc/                 # gRPC 协议发送驱动
```
