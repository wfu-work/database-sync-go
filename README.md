# DataSync Go

DataSync Go 是一个基于 `github.com/wfu-work/nav-common-go-lib` 的数据库同步服务，目标是在不同数据库之间做可配置、可追踪、可恢复的数据同步。

当前项目先实现 MySQL 和 TDengine 之间的数据同步，支持源表和目标表字段不完全一致、字段名称不同、字段类型需要转换等场景，并提供同步进度、同步历史、成功失败结果和错误原因记录。

## 当前进度

后台第一版已实现：

- 基于 `nav-common-go-lib` 的服务初始化、路由注册和管理表自动迁移。
- 数据源管理 API，支持 MySQL 和 TDengine。
- 同步任务管理 API，支持字段映射、默认值、简单转换、过滤条件、批量大小和写入模式。
- 数据源元数据 API，支持查询表列表、字段列表和表数据预览。
- 同步任务预检 API，支持校验字段映射并预览源数据和映射后数据。
- 同步运行记录、进度查询、历史查询和失败明细查询。
- MySQL 连接器，基于 `database/sql` 和 `github.com/go-sql-driver/mysql`。
- TDengine 连接器，基于 REST API，默认端口 `6041`。
- 全量同步和基于游标字段的增量同步。
- 运行中任务防重复启动、并发上限、超时取消和手动停止。
- Cron 定时调度，支持服务启动自动加载和手动重载。
- 失败明细重试，会基于失败时保存的源数据快照重新写入目标库。

暂未实现：前端管理页面、密码加密保存、分布式多实例任务锁。

## 项目目标

- 使用 `nav-common-go-lib` 作为基础框架，复用统一的配置、日志、数据库初始化、Gin 路由和 Web 静态资源能力。
- 支持 MySQL、TDengine 作为同步源或同步目标。
- 支持不同数据库、不同表之间的数据同步。
- 支持字段映射，例如源表 `device_id` 同步到目标表 `dev_id`。
- 支持字段过滤、默认值、简单类型转换和时间字段转换。
- 支持全量同步和增量同步。
- 支持任务进度展示，包括总数、已处理数、成功数、失败数、当前状态和耗时。
- 支持同步历史记录，包括每次执行的开始时间、结束时间、结果、影响行数和失败原因。
- 支持失败记录追踪，后续可扩展为失败重试和断点续传。

## 第一版范围

第一版优先做“稳定可用的单任务同步”，暂不追求复杂调度和分布式执行。

### 数据库类型

| 类型 | 作为源库 | 作为目标库 | 说明 |
| --- | --- | --- | --- |
| MySQL | 支持 | 支持 | 通过主键、时间字段或自增字段做分页和增量 |
| TDengine | 支持 | 支持 | 适合时序数据同步，重点处理时间戳和标签字段 |

### 同步模式

| 模式 | 说明 |
| --- | --- |
| 全量同步 | 从源表按批次读取全部数据，写入目标表 |
| 增量同步 | 根据游标字段同步新增或变更数据，例如 `id`、`updated_at`、`ts` |
| 手动触发 | 通过 API 或页面手动启动任务 |
| 定时触发 | 通过 `cronExpr` 和 `scheduleOn` 开启定时调度 |

### 字段映射

支持源字段和目标字段一对一映射：

```yaml
fields:
  - source: device_id
    target: dev_id
  - source: temperature
    target: temp
  - source: created_at
    target: ts
```

支持目标字段默认值：

```yaml
fields:
  - target: source_type
    default: mysql
```

支持简单转换：

```yaml
fields:
  - source: created_at
    target: ts
    transform: time_to_millis
```

第一版内置转换函数建议：

| 转换函数 | 说明 |
| --- | --- |
| `string` | 转成字符串 |
| `int` | 转成整数 |
| `float` | 转成浮点数 |
| `bool` | 转成布尔值 |
| `time_to_millis` | 时间转毫秒时间戳 |
| `millis_to_time` | 毫秒时间戳转时间 |

## 系统架构

```text
+---------------------+
| Web / API           |
| - 任务配置           |
| - 手动执行           |
| - 进度查询           |
| - 历史查询           |
+----------+----------+
           |
           v
+---------------------+
| Sync Service        |
| - 校验任务配置       |
| - 创建执行记录       |
| - 更新进度           |
| - 写入历史           |
+----------+----------+
           |
           v
+---------------------+
| Sync Worker         |
| - 批量读取           |
| - 字段映射           |
| - 类型转换           |
| - 批量写入           |
| - 错误收集           |
+-----+-----------+---+
      |           |
      v           v
+----------+  +-----------+
| MySQL    |  | TDengine  |
+----------+  +-----------+
```

## 目录规划

参考 `navmesh-go` 的组织方式，后续建议按下面结构演进：

```text
database-sync-go/
  apis/                 # HTTP handler，负责参数绑定和响应
  domains/              # GORM 管理表模型
  routers/              # 路由注册
  services/             # 业务服务
  syncer/               # 同步核心逻辑
    connector/          # MySQL、TDengine 连接器
    mapper/             # 字段映射和类型转换
    worker/             # 任务执行器
    cursor/             # 增量游标管理
  inits/                # nav-common-go-lib 初始化入口
  webs/                 # 前端静态资源
  config.yaml           # 服务配置
```

## 核心概念

### 数据源

数据源保存数据库连接信息，不直接写在同步任务里，便于复用和统一测试连接。

建议字段：

| 字段 | 说明 |
| --- | --- |
| `id` | 数据源 ID |
| `name` | 数据源名称 |
| `type` | `mysql` 或 `tdengine` |
| `host` | 地址 |
| `port` | 端口 |
| `username` | 用户名 |
| `password` | 密码，后续可加密保存 |
| `database` | 数据库名 |
| `params` | 额外连接参数 |
| `status` | 启用、禁用 |

### 同步任务

同步任务描述“从哪里读、写到哪里、如何映射、如何增量”。

建议字段：

| 字段 | 说明 |
| --- | --- |
| `id` | 任务 ID |
| `name` | 任务名称 |
| `source_id` | 源数据源 |
| `target_id` | 目标数据源 |
| `source_table` | 源表 |
| `target_table` | 目标表 |
| `mode` | `full` 或 `incremental` |
| `cursor_field` | 增量游标字段 |
| `cursor_value` | 当前游标值 |
| `batch_size` | 每批读取数量 |
| `field_mapping` | 字段映射 JSON |
| `write_mode` | `insert`、`upsert` 或 `replace` |
| `cron_expr` | Cron 表达式，支持 5 位或带秒 6 位 |
| `schedule_on` | 是否启用定时 |
| `status` | 启用、禁用 |

### 同步执行记录

每次运行同步任务都创建一条执行记录。

建议字段：

| 字段 | 说明 |
| --- | --- |
| `id` | 执行 ID |
| `task_id` | 任务 ID |
| `status` | `pending`、`running`、`success`、`failed`、`canceled` |
| `total_count` | 预计总数 |
| `processed_count` | 已处理数量 |
| `success_count` | 成功数量 |
| `failed_count` | 失败数量 |
| `start_time` | 开始时间 |
| `end_time` | 结束时间 |
| `duration_ms` | 耗时 |
| `last_error` | 最后一条错误 |

### 失败明细

失败明细用于定位某一行数据为什么失败。

建议字段：

| 字段 | 说明 |
| --- | --- |
| `id` | 失败记录 ID |
| `run_id` | 执行 ID |
| `task_id` | 任务 ID |
| `source_pk` | 源数据主键或唯一标识 |
| `source_data` | 源数据快照 |
| `error_message` | 错误信息 |
| `create_time` | 创建时间 |

## 同步流程

```text
1. 读取任务配置
2. 校验源数据源和目标数据源
3. 创建同步执行记录，状态为 running
4. 根据同步模式计算总数和起始游标
5. 按 batch_size 从源表分页读取
6. 对每行数据执行字段映射、默认值填充和类型转换
7. 批量写入目标表
8. 更新进度和当前游标
9. 记录失败明细
10. 任务完成后写入 success 或 failed 状态
```

## API 规划

统一使用 `config.yaml` 中的 `system.router-prefix`，默认前缀为 `/api`。

### 数据源

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/datasources/list` | 数据源列表 |
| `POST` | `/datasources` | 创建数据源 |
| `PUT` | `/datasources/:id` | 更新数据源 |
| `DELETE` | `/datasources/:id` | 删除数据源 |
| `POST` | `/datasources/:id/test` | 测试连接 |
| `GET` | `/datasources/:id/tables` | 表列表 |
| `GET` | `/datasources/:id/columns?table=xxx` | 字段列表 |
| `POST` | `/datasources/:id/preview` | 表数据预览 |

### 同步任务

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/sync/tasks/list` | 同步任务列表 |
| `POST` | `/sync/tasks` | 创建同步任务 |
| `PUT` | `/sync/tasks/:id` | 更新同步任务 |
| `DELETE` | `/sync/tasks/:id` | 删除同步任务 |
| `POST` | `/sync/tasks/validate` | 校验未保存任务配置 |
| `GET` | `/sync/tasks/:id/validate` | 校验已保存任务 |
| `POST` | `/sync/tasks/:id/preview` | 预览任务源数据和映射结果 |
| `POST` | `/sync/tasks/:id/run` | 手动执行任务 |
| `POST` | `/sync/tasks/:id/stop` | 停止执行任务 |
| `GET` | `/sync/tasks/schedules` | 当前调度列表 |
| `POST` | `/sync/tasks/schedules/reload` | 重载调度 |

### 进度和历史

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/sync/runs/list` | 同步历史 |
| `GET` | `/sync/runs/:id` | 执行详情 |
| `GET` | `/sync/runs/:id/progress` | 同步进度 |
| `GET` | `/sync/runs/:id/errors` | 失败明细 |
| `POST` | `/sync/runs/:id/retry-errors` | 重试失败明细 |

## 配置规划

服务基础配置沿用 `nav-common-go-lib`：

```yaml
system:
  app-name: "datasync"
  addr: 3010
  db-type: sqlite
  router-prefix: /api

jwt:
  issuer: datasync
  signing-key: "datasync"
  expires-time: 24h
  buffer-time: 1h

sqlite:
  db-name: datasync
  path: ./data/

zap:
  director: logback
  level: info
  prefix: '[datasync-server]'
  retention-day: 3
```

同步运行配置建议：

```yaml
sync:
  default-batch-size: 1000
  max-workers: 4
  run-timeout: 2h
  error-sample-limit: 1000
  history-retention-days: 90
```

## TDengine 注意事项

- TDengine 的时间字段通常是第一列，第一版建议将时间字段显式配置在字段映射中。
- 超级表和子表的标签字段需要单独处理，第一版可先支持普通表和固定目标表。
- 写入 TDengine 时需要注意时间戳精度，配置中应明确使用毫秒、微秒或纳秒。
- MySQL 到 TDengine 的同步建议优先使用 `insert` 写入模式。

## MySQL 注意事项

- 增量同步建议使用自增主键或更新时间字段。
- `upsert` 模式需要配置目标表唯一键或主键。
- 大表全量同步需要按主键或游标字段分页，避免一次性加载全部数据。
- 字符集建议统一使用 `utf8mb4`。

## 实施计划

### 阶段 1：基础框架

- 接入 `nav-common-go-lib` 初始化流程。
- 建立 `domains`、`services`、`routers`、`apis` 基础结构。
- 创建数据源、同步任务、执行记录、失败明细等管理表。
- 提供健康检查和基础 API。

### 阶段 2：连接器

- 实现 MySQL connector。
- 实现 TDengine connector。
- 统一 `QueryBatch`、`Count`、`WriteBatch`、`TestConnection` 等接口。
- 支持连接池和连接失败日志。

### 阶段 3：同步执行

- 实现全量同步。
- 实现字段映射、默认值和基础类型转换。
- 实现批量写入。
- 实现任务运行状态和进度更新。
- 实现同步历史和失败明细记录。

### 阶段 4：增量同步

- 支持游标字段配置。
- 每批成功后更新游标。
- 支持任务失败后从最近游标继续执行。
- 支持手动重置游标。

### 阶段 5：Web 页面

- 数据源管理页面。
- 同步任务配置页面。
- 执行进度页面。
- 同步历史页面。
- 失败明细页面。

## 后续扩展

- 支持 PostgreSQL、SQL Server、SQLite 等更多数据库。
- 支持复杂表达式字段转换。
- 支持行级过滤条件。
- 支持定时任务和任务依赖。
- 支持失败数据重试。
- 支持多任务并发和限速。
- 支持同步前后 SQL hook。
- 支持 Prometheus 指标和告警。

## 开发启动

```bash
go mod tidy
go run .
```

默认 API 地址：

```text
http://127.0.0.1:3010/api
```

后续实现完成后可通过下面接口检查服务：

```bash
curl http://127.0.0.1:3010/api/health
```

## API 示例

创建 MySQL 数据源：

```bash
curl -X POST http://127.0.0.1:3010/api/datasources \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "mysql-source",
    "type": "mysql",
    "host": "127.0.0.1",
    "port": 3306,
    "username": "root",
    "password": "password",
    "database": "demo"
  }'
```

创建 TDengine 数据源：

```bash
curl -X POST http://127.0.0.1:3010/api/datasources \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "td-target",
    "type": "tdengine",
    "host": "127.0.0.1",
    "port": 6041,
    "username": "root",
    "password": "taosdata",
    "database": "demo"
  }'
```

查看数据源表列表：

```bash
curl http://127.0.0.1:3010/api/datasources/SOURCE_GUID/tables
```

查看表字段：

```bash
curl 'http://127.0.0.1:3010/api/datasources/SOURCE_GUID/columns?table=sensor_data'
```

预览数据源表数据：

```bash
curl -X POST http://127.0.0.1:3010/api/datasources/SOURCE_GUID/preview \
  -H 'Content-Type: application/json' \
  -d '{
    "table": "sensor_data",
    "whereClause": "temperature > 0",
    "limit": 10
  }'
```

创建同步任务：

```bash
curl -X POST http://127.0.0.1:3010/api/sync/tasks \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "mysql-to-tdengine",
    "sourceGuid": "SOURCE_GUID",
    "targetGuid": "TARGET_GUID",
    "sourceTable": "sensor_data",
    "targetTable": "sensor_data",
    "mode": "incremental",
    "cursorField": "id",
    "batchSize": 1000,
    "writeMode": "insert",
    "scheduleOn": 1,
    "cronExpr": "0 */5 * * * *",
    "fields": [
      {"source": "created_at", "target": "ts", "transform": "time_to_millis"},
      {"source": "device_id", "target": "device_id"},
      {"source": "temperature", "target": "temperature", "transform": "float"},
      {"target": "source_type", "default": "mysql"}
    ]
  }'
```

校验未保存的同步任务配置：

```bash
curl -X POST http://127.0.0.1:3010/api/sync/tasks/validate \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "mysql-to-tdengine",
    "sourceGuid": "SOURCE_GUID",
    "targetGuid": "TARGET_GUID",
    "sourceTable": "sensor_data",
    "targetTable": "sensor_data",
    "mode": "incremental",
    "cursorField": "id",
    "fields": [
      {"source": "created_at", "target": "ts", "transform": "time_to_millis"},
      {"source": "device_id", "target": "device_id"}
    ]
  }'
```

预览同步任务映射结果：

```bash
curl -X POST http://127.0.0.1:3010/api/sync/tasks/TASK_GUID/preview \
  -H 'Content-Type: application/json' \
  -d '{"limit": 10}'
```

手动执行任务：

```bash
curl -X POST http://127.0.0.1:3010/api/sync/tasks/TASK_GUID/run
```

停止任务：

```bash
curl -X POST http://127.0.0.1:3010/api/sync/tasks/TASK_GUID/stop
```

查询进度：

```bash
curl http://127.0.0.1:3010/api/sync/runs/RUN_GUID/progress
```

查看当前定时调度：

```bash
curl http://127.0.0.1:3010/api/sync/tasks/schedules
```

重载定时调度：

```bash
curl -X POST http://127.0.0.1:3010/api/sync/tasks/schedules/reload
```

重试某次运行的失败明细：

```bash
curl -X POST http://127.0.0.1:3010/api/sync/runs/RUN_GUID/retry-errors
```
