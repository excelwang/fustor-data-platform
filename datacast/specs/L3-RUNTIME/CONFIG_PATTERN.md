# L3: [decision] [Datacast] Configuration Pattern

> Type: decision | pattern
> Layer: Domain Layer

## [pattern] Global_Singleton_for_Mutations

为了简化服务层（Service Layer）的方法签名并确保配置变更的原子性，**Datacast** 采用 **Global Singleton Pattern** 进行配置的增删改操作。

### 1.1 The `Datacast_config` Loader
- **Location**: `Datacast.config.unified.Datacast_config`
- **Role**: 它是进程内唯一的配置真值来源（Source of Truth）。
- **Responsibility**:
    - **Load**: 启动时从文件系统加载 YAML。
    - **Mutate**: 提供 `add_source`, `delete_sender` 等原子方法修改内存状态。
    - **Persist**: 负责将内存状态回写到磁盘（可选，或由 CLI 触发）。

### 1.2 Service Layer Usage
服务层（如 `SourceConfigService`）**不再**持有 `AppConfig` 的副本，而是直接调用全局加载器：

```python
# GOOD
from Datacast.config.unified import Datacast_config

class SourceConfigService:
    def add_source(self, id, config):
        # 直接操作全局单例
        Datacast_config.add_source(id, config)

# BAD
class SourceConfigService:
    def __init__(self, app_config):
        self.config = app_config  # 容易造成状态分裂
```

### 1.3 Testing Implication
由于使用了全局单例，测试必须显式 mock `Datacast_config`：

```python
with patch("Datacast.config.unified.Datacast_config") as mock_conf:
    mock_conf.get_source.return_value = ...
    service.do_something()
```

## [model] Immutable_Configuration_Objects
配置对象本身（`SourceConfig`, `PipeConfig`）一旦被加载，应视为**不可变对象**（Immutable）。任何修改都应通过 `Datacast_config.update_*` 接口替换整个对象或更新其字段，而不是直接修改对象的属性。
