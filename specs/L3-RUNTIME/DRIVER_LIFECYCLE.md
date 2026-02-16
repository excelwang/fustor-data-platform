---
version: 1.0.0
---

# L3: [algorithm] Driver Lifecycle & Instantiation

> Type: algorithm | pattern
> Layer: Domain Layer

## 1. 概述

本文档定义了 Driver 实例的生命周期管理，包括单例模式、引用计数和销毁策略。

## 2. FSDriver Singleton Pattern

为节省系统资源（如 inotify watch 描述符），FSDriver 实现了 **Per-URI Singleton** 模式。

### 2.1 唯一标识

```python
signature = f"{uri}#{hash(credential)}"
```

### 2.2 行为特征

-   **共享实例**: 不同 AgentPipe 配置若指向同一 URI 且凭证相同，将共享同一个 Driver 实例。
-   **资源互斥**: 共享实例意味着共享底层的 WatchManager 和 EventQueue (EventBus)。

### 2.3 生命周期约束

-   **引用计数**: Driver 内部**不**维护引用计数（简化设计）。
-   **显式销毁**: 必须调用 `driver.close()` 或 `FSDriver.invalidate(uri, cred)` 才能从缓存中移除。
-   **热重载**: 修改配置（如排除列表）但 URI 不变时，ConfigReloader 必须显式 `invalidate` 旧实例。
