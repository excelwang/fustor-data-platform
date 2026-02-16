# L3: [fustord] Configuration Structure

> Type: pattern
> Layer: Implementation Layer (Configuration)

---

## [strategy] Config_Loader_Strategy

Settings are loaded from `$FUSTORD_HOME`.

## [definition] YAML_Configuration_Structure_Definition

```yaml
# receivers.yaml - 定义监听端口
http-main:
  driver: http
  port: 18881
  api_keys: ["key-1", "key-2"]

# views/main-view.yaml - 定义业务视图
main-fs-view:
  driver: fs-view
  driver_params:
      hot_file_threshold: 60.0

# pipes/pipe-1.yaml - 定义绑定关系
pipe-1:
  receiver: http-main
  views: [main-fs-view]
```
