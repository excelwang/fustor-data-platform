# L3: [Datacast] Package Structure

> **Status**: Active
> **Layer**: Implementation (L3)
> **Parent**: L2-ARCHITECTURE.COMPONENTS.CORE

---

## [definition] Core_Datacast_Packages

**Component**: Datacast engine package organization.

```
datacast/                             # Datacast 守护进程主包 (Repo Member)
├── src/datacast/
│   ├── boot/                        # 引导与环境初始化
│   ├── domain/                      # 运行时调度 (PipeManager)
│   └── cli.py                       # CLI 指令 (reload, start, version)

core/                                # 核心抽象层 (datacast-core)
├── src/datacast_core/
│   ├── common/                      # 通用工具 (logging, daemon, paths)
│   ├── event/                       # 统一事件模型 (EventBase, EventType)
│   ├── pipe/                        # 管道与 Handler ABC (含 BasePipeManager)
│   ├── stability/                   # 共享稳定性 Mixins
│   ├── transport/                   # Sender ABC
│   ├── clock/                       # 影子参考系时钟算法
│   └── models/                      # Pydantic 配置模型
```

## [definition] Source_and_Sender_Drivers

**Component**: Data extraction implementation packages.

```
extensions/
├── source-fs/                      # 文件系统 Source Driver
├── source-mysql/                   # 数据库 Source Driver
├── sender-http/                    # HTTP 协议发送驱动
├── sender-echo/                    # Debug 回环驱动
└── fustor-mgmt/                    # 统一控制平面 (Agent 端)
```
