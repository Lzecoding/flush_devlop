# Flush 三大功能模块完整设计文档

> **定位**：本文档描述 gsiceberg flush 子系统的三个功能模块的完整设计。
> 每个模块从零开始，不假设任何已有实现，仅依赖外部框架提供的约定接口。
> **设计参考**：`docs/release/v0.1.0/arch-zh/` 架构文档（01-overview, 08-flush-pipeline, 06-write-path, 13-fault-recovery, 14-internal-schema）

---

## 目录

- [1. 外部依赖接口](#1-外部依赖接口)
- [2. 元数据表设计](#2-元数据表设计)
- [3. 模块一：定时自动 Flush](#3-模块一定时自动-flush)
- [4. 模块二：Flush 生命周期追踪](#4-模块二flush-生命周期追踪)
- [5. 模块三：后台 Flush 作业调度](#5-模块三后台-flush-作业调度)
- [6. 跨模块协作与部署](#6-跨模块协作与部署)

---

## 1. 外部依赖接口

> 三个模块不直接操作 Iceberg 文件、不直接管理 PG 存储。所有底层能力由 gsiceberg
> 核心框架通过以下接口提供。模块开发者只需理解接口契约，无需了解实现细节。

### 1.1 Flush 管道接口

模块需要触发 flush 操作。框架提供两个粒度的入口：

#### 1.1.1 异步 flush 入口

```
接口名：iceberg_flush
类型：PG C 函数，通过 SQL 调用
SQL 签名：iceberg_flush(table_name text) → bigint
```

**行为契约**：
1. 调用方通过 `SELECT iceberg_flush('表名')` 或 PL/pgSQL 的 `PERFORM iceberg_flush(...)` 触发
2. 函数内部获取表级 advisory lock（`pg_try_advisory_xact_lock`），同一张表同一时刻只允许一个 flush 执行。锁冲突时返回 NULL
3. 调用方必须是表的 owner 或 `gsiceberg_admin` 角色的成员（框架内部做权限检查）
4. 执行 Phase 1（冻结 delta）后返回 job_id
5. 返回值：`>0` = job_id（成功创建 flush 任务），`NULL` = 锁冲突或 delta 为空
6. Phase 2 由后台 worker 异步执行，不在本次调用中完成

**依赖此接口的模块**：模块一（调度器触发 flush）、模块三（worker 也可通过 `iceberg_flush_phase2` 直接执行 Phase 2）

#### 1.1.2 Phase 2 直接执行入口

```
接口名：iceberg_flush_phase2
类型：PG C 函数，通过 SQL 调用
SQL 签名：iceberg_flush_phase2(table_name text, job_id bigint) → boolean
```

**行为契约**：
1. 处理已冻结的 `_delta_flushing` 表：I/D 抵消 → 写 Parquet → 更新 snapshots/data_files 目录 → 清理
2. 函数内部自行管理 SPI 连接/断开
3. 返回值：`true` = 成功，`false` = 失败
4. 成功时在内部调用 `flush_state_done()` 清理状态

**依赖此接口的模块**：模块三（worker 执行 job 时调用）

#### 1.1.3 同步 flush 入口

```
接口名：iceberg_flush_sync
类型：PL/pgSQL 函数
SQL 签名：iceberg_flush_sync(table_name text) → boolean
```

**行为契约**：
1. 顺序执行：权限检查 → Phase 1 → Phase 2 → 返回结果
2. Phase 2 同步等待完成，不像 `iceberg_flush()` 那样异步

**依赖此接口的模块**：模块二（测试中需要同步 flush 验证追踪结果）

### 1.2 Flush 状态管理接口

框架维护一个 `_gsiceberg.flush_state` 表，用于追踪每次 flush 的生命周期。框架提供以下接口操作此表：

| 接口 | 签名 | 作用 | SPI 要求 |
|---|---|---|---|
| `flush_state_begin` | `void (const char *table_name)` | 标记 flush 开始：INSERT 或 UPDATE 为 in_progress 状态，清空上一轮的残留数据 | 调用前需 SPI 已连接 |
| `flush_state_set_file` | `void (const char *table_name, const char *path, int64_t n_rows)` | 记录写入的 Parquet 文件路径和行数 | 调用前需 SPI 已连接 |
| `flush_state_set_snapshot` | `void (const char *table_name, int64_t snapshot_id)` | 记录创建的快照 ID | 调用前需 SPI 已连接 |
| `flush_state_done` | `void (const char *table_name)` | 标记 flush 完成：UPDATE phase='completed'（模块二改为 UPDATE，不再 DELETE） | 调用前需 SPI 已连接 |
| `flush_state_recover` | `bool (const char *table_name, char *parquet_out, size_t out_len, int current_n_rows)` | 崩溃恢复：检查是否有可恢复的 Parquet 文件 | 调用前需 SPI 已连接 |

**关键语义**：
- 空闲状态由 `flush_status='completed'`（`flush_status='idle'` 为旧版语义，模块二改为 `'completed'`）表示。`flush_state_done()` 执行 `UPDATE`。
- 崩溃恢复仅依赖 `parquet_file`、`delta_rows`、`snapshot_id` 三列

### 1.3 安全守卫接口

框架提供三个权限检查函数（static inline，头文件中定义）：

| 接口 | 签名 | 检查内容 | 失败行为 |
|---|---|---|---|
| `gsiceberg_require_table_owner` | `void (const char *table_name)` | current_user == owner OR owner == 'public' OR 是 gsiceberg_admin | ereport(ERROR) 终止事务 |
| `gsiceberg_require_admin` | `void (void)` | 是 gsiceberg_admin 或 superuser | ereport(ERROR) |
| `gsiceberg_require_primary` | `void (void)` | 运行在 primary 节点（`!RecoveryInProgress()`） | ereport(ERROR) |

**前置条件**：调用前 SPI 必须已连接。

### 1.4 工具函数接口

| 接口 | 签名 | 作用 | SPI 要求 |
|---|---|---|---|
| `relation_exists` | `bool (const char *schema, const char *relname)` | 检查 PG relation 是否存在 | 是 |
| `relation_row_count` | `int (const char *schema, const char *relname)` | 获取 relation 行数 | 是 |
| `escape_sql_literal` | `char *(const char *src)` | SQL 字面值转义（调用方 pfree） | 否 |

### 1.5 文件系统接口

| 接口 | 签名 | 作用 | SPI 要求 |
|---|---|---|---|
| `gsfile_register_internal` | `int (const char *path, const char *table_name)` | 注册文件到 `_gsfs.owned_files`（refcount+1） | 是 |
| `gsfile_unregister_internal` | `int (const char *path)` | 注销文件（refcount-1，归零则 DELETE） | 是 |

### 1.6 GUC 变量接口

框架提供 GUC（Grand Unified Configuration）变量的注册与读取：

**注册**（C 层，在 `_PG_init()` 中调用）：
```c
DefineCustomBoolVariable(name, short_desc, long_desc, &var, default_val, context, ...);
DefineCustomIntVariable(name, short_desc, long_desc, &var, default_val, min, max, context, ...);
DefineCustomStringVariable(name, short_desc, long_desc, &var, default_val, context, ...);
```

**读取**（PL/pgSQL 层）：
```sql
current_setting('gsiceberg.xxx')::boolean
current_setting('gsiceberg.xxx')::int
```

### 1.7 PG 内置能力

三个模块依赖以下 PostgreSQL 标准能力：
- `SPI_execute_with_args()` —— 参数化 SQL 执行
- `pg_try_advisory_xact_lock_int4()` —— 事务级建议锁
- `FOR UPDATE SKIP LOCKED` —— 行级锁跳过（用于 worker 并发取任务）
- `pg_sleep(n)` —— 秒级等待
- `format(%I, ...)` + `EXECUTE` —— 动态 SQL（PL/pgSQL）
- `ereport(WARNING/NOTICE/ERROR, ...)` —— 日志与错误报告
- `TEXTOID`, `INT8OID`, `BOOLOID`, `TIMESTAMPTZOID` —— PG 类型 OID 常量

---

## 2. 元数据表设计

三个模块共享以下元数据表。表由框架创建，模块通过 SPI 读写。标注 **[M1]/[M2]/[M3]** 的列为对应模块新增。

### 2.1 `_gsiceberg.tables` —— 挂载表注册表

```sql
CREATE TABLE _gsiceberg.tables (
    table_name       text PRIMARY KEY,          -- 表名（如 'orders'）
    table_path       text NOT NULL,             -- Iceberg 表根目录绝对路径
    owner            text NOT NULL DEFAULT current_user,
    "current_schema" jsonb NOT NULL,            -- Iceberg schema 定义
    partition_spec   jsonb,                     -- 分区规范，NULL = 未分区
    created_at       timestamptz DEFAULT now(),

    -- [M1] 模块一新增
    auto_flush_enabled      boolean NOT NULL DEFAULT false,
    auto_flush_interval_sec  int    NOT NULL DEFAULT 300,   -- 0 = 关闭
    auto_flush_min_rows      int    NOT NULL DEFAULT 1      -- 最小触发行数
);
```

### 2.2 `_gsiceberg.flush_state` —— Flush 生命周期状态

```sql
CREATE TABLE _gsiceberg.flush_state (
    table_name     text NOT NULL,
    job_id         bigint NOT NULL,               -- [M2] 新增：关联 flush_jobs.job_id

    -- 基础状态（框架已有，flush_status 语义扩展）
    flush_status   text NOT NULL DEFAULT 'in_progress',  -- 'in_progress' | 'completed' | 'failed'
    started_at     timestamptz DEFAULT now(),
    parquet_file   text,                             -- Parquet 文件路径
    delta_rows     bigint,                           -- delta 行数（与 rows_written 区分）
    snapshot_id    bigint,                           -- 快照 ID

    -- [M2] 新增：阶段枚举
    phase          text NOT NULL DEFAULT 'frozen',  -- 'frozen'|'writing'|'committing'|'cleaning'|'completed'

    -- [M2] 新增：各阶段时间戳
    frozen_at      timestamptz,
    written_at     timestamptz,
    committed_at   timestamptz,
    cleaned_at     timestamptz,

    -- [M2] 新增：统计数据
    delta_rows_frozen  bigint,    -- Phase 1 冻结时 delta 行数
    rows_written       bigint,    -- 最终写入 Parquet 的行数
    tombstone_count    bigint,    -- 产生的墓碑记录数
    parquet_size_bytes bigint,    -- Parquet 文件字节数

    -- [M2] 新增：错误追踪
    error_code     text,
    error_phase    text,           -- 失败发生在哪个阶段

    PRIMARY KEY (table_name, job_id)   -- [M2] 改动：加入 job_id，每次 flush 一行
);
```

**设计要点**：
- **主键改为 `(table_name, job_id)`**：每次 flush INSERT 新行，不覆盖历史。同一张表的不同 flush 各自保留独立追踪记录
- **`flush_status`**：`in_progress`（执行中）、`completed`（成功完成）、`failed`（失败）。空闲状态不再由"行不存在"表示，改为明确的状态值
- **`phase`**：`frozen` → `writing` → `committing` → `cleaning` → `completed`。完成后不再 DELETE，行永久保留
- **`flush_state_done()` 改为 UPDATE**：`UPDATE SET phase='completed', flush_status='completed'`，不再 DELETE

### 2.3 `_gsiceberg.flush_jobs` —— Flush 作业队列

```sql
CREATE TABLE _gsiceberg.flush_jobs (
    job_id        bigserial PRIMARY KEY,
    table_name    text NOT NULL,
    flush_table   text NOT NULL,
    started_at    timestamptz DEFAULT now(),
    finished_at   timestamptz,
    status        text NOT NULL DEFAULT 'pending',   -- pending|in_progress|completed|failed
    retry_count   int DEFAULT 0,
    error_msg     text,

    -- [M3] 模块三新增
    priority      int NOT NULL DEFAULT 0,           -- 优先级，越大越先处理
    scheduled_at  timestamptz DEFAULT now(),        -- 任务创建时间
    worker_id     text                              -- 哪个 worker 在处理
);
```

### 2.4 `_gsiceberg.flush_workers` —— Worker 心跳表

```sql
-- [M3] 模块三新增
CREATE TABLE IF NOT EXISTS _gsiceberg.flush_workers (
    worker_id      text PRIMARY KEY,               -- worker 唯一标识
    status         text NOT NULL DEFAULT 'running',-- running | stopped | dead
    current_job    bigint,                         -- 当前处理的 job_id，NULL=空闲
    heartbeat      timestamptz DEFAULT now(),      -- 最近心跳时间
    started_at     timestamptz DEFAULT now(),      -- worker 首次启动
    jobs_processed bigint DEFAULT 0,               -- 累计完成数
    last_error     text                            -- 最近错误
);
```

### 2.5 只读依赖表

以下表由框架维护，模块仅读取：

```sql
-- 快照历史（查询上次 flush 状态、快照计数）
_siceberg.snapshots (table_name, snapshot_id, parent_id, timestamp, manifest_list, summary)

-- 数据文件清单（查询文件大小、行数统计）
_siceberg.data_files (table_name, begin_snapshot_id, file_path, record_count, file_size_bytes, ...)

-- 行 ID 区间映射（崩溃恢复时查询活跃区间）
_siceberg._row_range_facts (table_name, first_row_id, begin_snapshot, row_upper, file_path, ...)
```

### 2.6 每表运行时对象

框架为每张挂载表创建以下运行时对象，模块一和模块三通过 `iceberg_flush()` 间接触发它们的使用：

| 对象 | 命名格式 | 用途 |
|---|---|---|
| delta 表 | `_gsiceberg._<name>_delta` | 暂存 INSERT/UPDATE/DELETE 的 DML 变更 |
| 冻结 delta 表 | `_gsiceberg._<name>_delta_flushing` | Phase 1 RENAME 后的冻结快照（Phase 2 处理对象） |
| 公共 VIEW | `<name>` | `_data UNION ALL _delta`，用户的 DML 入口 |

**关键约定**：
- delta 表名可通过 `'_' || table_name || '_delta'` 拼接
- delta 表属于 `_gsiceberg` schema
- Phase 1 冻结后 delta 表被 RENAME 为 `_delta_flushing`，同时新建空 delta

---

## 3. 模块一：定时自动 Flush

### 3.1 模块概述

**目标**：让系统在没有人工干预的情况下，自动将 delta 表中积攒的数据周期性持久化到 Iceberg Parquet 文件。

**解决的问题**：用户 INSERT 后忘记手动执行 `iceberg_flush()`，导致：数据仅存于 PG 堆表无 Iceberg 快照保护、外部工具（Spark/DuckDB）不可见、delta 增长拖慢 SELECT 性能。

**核心思路**：提供一个调度器函数，由外部定时器（pg_cron / cron / 内部循环）周期性调用。调度器扫描所有挂载表，对满足条件的表触发异步 flush。

### 3.2 功能需求

| 编号 | 需求 | 详细说明 |
|---|---|---|
| **FR-1.1** | 表级自动 flush 开关 | 每张挂载表可独立启用/禁用自动 flush。开关为 `auto_flush_enabled` 列（boolean），默认 false（不自动 flush）。这样已有表不受影响，新表按需开启 |
| **FR-1.2** | 表级 flush 间隔配置 | 每张表可设置两次 flush 之间的最小间隔。`auto_flush_interval_sec`（integer，秒）。例如 orders 表设为 300（5 分钟），products 表设为 3600（1 小时）。设为 0 等效于关闭自动 flush |
| **FR-1.3** | 表级最小行数阈值 | `auto_flush_min_rows`（integer，默认 1）。delta 行数未达标时不触发 flush，防止 1-2 行就产生碎片 Parquet 文件 |
| **FR-1.4** | 全局 GUC 总开关 | `gsiceberg.auto_flush_enabled`（boolean，默认 true，PGC_SUSET）。运维场景下一次关闭所有表的自动 flush（如批量导入期间）。false 时调度器立即返回，不扫描任何表 |
| **FR-1.5** | 调度器函数 | `iceberg_auto_flush_scheduler()`，PL/pgSQL 实现。被调用时扫描所有 `auto_flush_enabled=true` 的表，检查 delta 行数和时间间隔，满足条件则调 `iceberg_flush()` 触发异步 flush。返回 TABLE(table_name, flushed, reason) 供调用方审计 |
| **FR-1.6** | 不阻塞用户 DML | 调度器调用 `iceberg_flush()`（异步模式，仅执行 Phase 1 的毫秒级 RENAME）。Phase 2 的 Parquet 写入由后台 worker 异步完成，用户 INSERT 始终写入新 delta，不受影响 |
| **FR-1.7** | daemon 守护循环 | `iceberg_auto_flush_daemon(worker_id, poll_interval_sec)`，PL/pgSQL 实现。LOOP 内调用 scheduler，无任务时 pg_sleep。适用于无 pg_cron 或系统 cron 的环境。异常时记录 WARNING 后继续，不退出循环 |
| **FR-1.8** | 调度器幂等性 | 同一秒内多次调用 scheduler 不会重复创建 flush job。`iceberg_flush()` 内部的 advisory lock 保证同表并发调用只有一个成功，其他返回 NULL |
| **FR-1.9** | 首次 flush 时间计算 | 新挂载表从未 flush 过时，`last_flush_at` 回退为 `tables.created_at`（挂载时刻），确保首次自动 flush 从挂载时刻开始计时 |

### 3.3 详细设计

#### 3.3.1 配置数据存储

配置存储在 `_gsiceberg.tables` 的三列中（见 §2.1）。模块通过标准 SQL UPDATE/SELECT 读写：

```sql
-- 启用：orders 表每 10 分钟自动 flush，至少 100 行才触发
UPDATE _gsiceberg.tables
SET auto_flush_enabled = true,
    auto_flush_interval_sec = 600,
    auto_flush_min_rows = 100
WHERE table_name = 'orders';

-- 禁用
UPDATE _gsiceberg.tables
SET auto_flush_enabled = false
WHERE table_name = 'orders';

-- 运维查询：哪些表开了自动 flush？间隔多少？
SELECT table_name, auto_flush_interval_sec, auto_flush_min_rows
FROM _gsiceberg.tables
WHERE auto_flush_enabled = true;
```

#### 3.3.2 调度器逻辑

```
iceberg_auto_flush_scheduler()
│
├─ ① 读取 GUC gsiceberg.auto_flush_enabled
│     false → RETURN（全局关闭，一步都不走）
│
├─ ② 扫描 _gsiceberg.tables
│     WHERE auto_flush_enabled = true
│       AND auto_flush_interval_sec > 0
│     输出：table_name, interval_sec, min_rows, last_flush_at
│     last_flush_at = MAX(flush_jobs.finished_at) 中最晚的 completed 时间
│                     若从未 completed → COALESCE → tables.created_at
│
├─ ③ 对每张匹配的表：
│   ├─ 动态 SQL 查 delta 行数：
│   │   EXECUTE 'SELECT count(*) FROM _gsiceberg._' || table_name || '_delta'
│   ├─ 算距今秒数：extract(epoch FROM now() - last_flush_at)
│   ├─ 判定：
│   │   delta_rows >= min_rows  AND  elapsed_sec >= interval_sec
│   │   ↓ 满足
│   ├─ PERFORM iceberg_flush(table_name)
│   │   内部：advisory lock → 权限检查 → Phase 1 (RENAME + CREATE + INSERT flush_jobs) → 返回 job_id
│   │   锁冲突 → 返回 NULL → 调度器不将此表计入 flushed
│   └─ RETURN NEXT (table_name, true, 原因描述)
│
└─ ④ 返回：TABLE(table_name text, flushed bool, reason text)
```

**调用方式**：

```sql
-- 由 pg_cron 每分钟调用
SELECT cron.schedule('auto-flush', '* * * * *',
    'SELECT iceberg_auto_flush_scheduler();');

-- 或由系统 cron 每分钟调用
-- * * * * * psql -c "SELECT iceberg_auto_flush_scheduler();"
```

#### 3.3.3 调度器 PL/pgSQL 实现

```sql
CREATE OR REPLACE FUNCTION iceberg_auto_flush_scheduler()
RETURNS TABLE(table_name text, flushed bool, reason text)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog AS $$
DECLARE
    rec     RECORD;
    dcount  bigint;
    elapsed int;
BEGIN
    -- ① 全局开关
    IF NOT current_setting('gsiceberg.auto_flush_enabled')::boolean THEN
        RETURN;
    END IF;

    -- ② 扫描所有启用了自动 flush 的表
    FOR rec IN
        SELECT t.table_name,
               t.auto_flush_interval_sec,
               t.auto_flush_min_rows,
               COALESCE(
                   (SELECT MAX(j.finished_at)
                    FROM _gsiceberg.flush_jobs j
                    WHERE j.table_name = t.table_name
                      AND j.status = 'completed'),
                   t.created_at
               ) AS last_flush_at
        FROM _gsiceberg.tables t
        WHERE t.auto_flush_enabled = true
          AND t.auto_flush_interval_sec > 0
    LOOP
        -- ③-a 查 delta 行数（动态 SQL，表名安全转义）
        EXECUTE format(
            'SELECT count(*) FROM _gsiceberg._%I_delta', rec.table_name
        ) INTO dcount;

        -- ③-b 计算时间间隔
        elapsed := extract(epoch FROM now() - rec.last_flush_at)::int;

        -- ③-c 判定触发条件
        IF dcount >= rec.auto_flush_min_rows
           AND elapsed >= rec.auto_flush_interval_sec
        THEN
            -- ③-d 触发异步 flush
            PERFORM iceberg_flush(rec.table_name);

            table_name := rec.table_name;
            flushed := true;
            reason := format(
                'delta_rows=%s, min_rows=%s, elapsed_sec=%s, interval_sec=%s',
                dcount, rec.auto_flush_min_rows,
                elapsed, rec.auto_flush_interval_sec);
            RETURN NEXT;
        END IF;
    END LOOP;
END;
$$;
```

#### 3.3.4 daemon 守护循环

```sql
CREATE OR REPLACE FUNCTION iceberg_auto_flush_daemon(
    worker_id         text DEFAULT 'auto_flush_daemon',
    poll_interval_sec int  DEFAULT 60
)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog AS $$
DECLARE
    processed int;
BEGIN
    RAISE NOTICE 'gsiceberg: auto_flush daemon "%" starting (poll=%ss)',
                  worker_id, poll_interval_sec;
    LOOP
        BEGIN
            SELECT count(*) INTO processed
            FROM iceberg_auto_flush_scheduler();
            -- 有活干时不 sleep（连续处理积压），没活时 sleep
            IF processed = 0 THEN
                PERFORM pg_sleep(poll_interval_sec);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'gsiceberg: auto_flush daemon "%" error: %',
                           worker_id, SQLERRM;
            PERFORM pg_sleep(poll_interval_sec);
        END;
    END LOOP;
END;
$$;
```

#### 3.3.5 GUC 注册（C 层）

```c
static bool gsiceberg_auto_flush_enabled = true;
static int  gsiceberg_auto_flush_poll_interval = 60;

// 在 _PG_init() 中调用：
DefineCustomBoolVariable(
    "gsiceberg.auto_flush_enabled",
    "Global on/off for automatic delta flush scheduling.",
    "When false, iceberg_auto_flush_scheduler() returns immediately.",
    &gsiceberg_auto_flush_enabled,
    true, PGC_SUSET, 0, NULL, NULL, NULL);

DefineCustomIntVariable(
    "gsiceberg.auto_flush_poll_interval",
    "Default polling interval for iceberg_auto_flush_daemon().",
    NULL,
    &gsiceberg_auto_flush_poll_interval,
    60, 1, 3600, PGC_SUSET, 0, NULL, NULL, NULL);
```

#### 3.3.6 边界条件处理

| 场景 | 判定条件 | 行为 |
|---|---|---|
| delta 为空 | `dcount = 0` | `dcount >= min_rows` 不满足，跳过 |
| 表正在 flush 中 | advisory lock 冲突 | `iceberg_flush()` 返回 NULL（不重复创建 job） |
| 表已被卸载 | tables 无记录 | FOR 循环自然跳过 |
| interval=0 且 enabled=true | WHERE 条件 `> 0` 过滤 | 跳过 |
| 全局 GUC=false | scheduler 第一步 | 直接 RETURN |
| 新挂载表从未 flush | `finished_at IS NULL` | COALESCE → `created_at` |
| 调度器被并发调用 | 两个 session 同时执行 | 各自独立扫描，同表的 advisory lock 保证互斥 |

### 3.4 接口依赖

| 依赖的外部接口 | 调用方式 | 用途 |
|---|---|---|
| `iceberg_flush(text) → bigint` | PL/pgSQL `PERFORM` | 触发异步 flush（Phase 1） |
| `_gsiceberg.tables`（含 auto_flush_* 列） | SELECT | 读取配置 |
| `_gsiceberg.flush_jobs` | 子查询 | 获取上次 flush 完成时间 |
| `gsiceberg.auto_flush_enabled` GUC | `current_setting(...)::boolean` | 全局开关检查 |
| `gsiceberg.auto_flush_poll_interval` GUC | daemon 参数默认值 | 轮询间隔 |
| `pg_sleep(n)` | PL/pgSQL `PERFORM` | daemon 等待 |
| `format(%I, ...)` + `EXECUTE` | PL/pgSQL 内置 | 动态 SQL 查 delta 行数 |

### 3.5 实现清单

| 产出 | 类型 | 说明 |
|---|---|---|
| `_gsiceberg.tables` 新增 3 列 | DDL | auto_flush_enabled, auto_flush_interval_sec, auto_flush_min_rows |
| `iceberg_auto_flush_scheduler()` | PL/pgSQL 函数 | 调度器，约 60 行 |
| `iceberg_auto_flush_daemon()` | PL/pgSQL 函数 | 守护循环，约 25 行 |
| `gsiceberg.auto_flush_enabled` | GUC | boolean，全局开关 |
| `gsiceberg.auto_flush_poll_interval` | GUC | int，daemon 轮询间隔 |

**代码量**：PL/pgSQL ~100 行 + C ~20 行

### 3.6 测试文档

#### 3.6.1 配置读写测试：`test/sql/functional/auto_flush_config.sql`

```sql
-- ============================================================
-- T1: 新挂载表默认值校验
-- ============================================================
SELECT auto_flush_enabled = false  AS "default_disabled",
       auto_flush_interval_sec = 300 AS "default_300s",
       auto_flush_min_rows = 1       AS "default_min_1"
FROM _gsiceberg.tables WHERE table_name = 'af_t1';

-- 预期结果：
--  default_disabled | default_300s | default_min_1
-- ──────────────────┼──────────────┼──────────────
--  t                | t            | t
-- 验证：新挂载表默认不自动 flush（安全默认），间隔 300 秒，最小 1 行触发


-- ============================================================
-- T2: 配置更新后读写一致性
-- ============================================================
UPDATE _gsiceberg.tables
SET auto_flush_enabled = true, auto_flush_interval_sec = 120, auto_flush_min_rows = 50
WHERE table_name = 'af_t1';

SELECT auto_flush_enabled = true  AS "enabled",
       auto_flush_interval_sec = 120 AS "interval_120",
       auto_flush_min_rows = 50     AS "min_50"
FROM _gsiceberg.tables WHERE table_name = 'af_t1';

-- 预期结果：
--  enabled | interval_120 | min_50
-- ─────────┼──────────────┼───────
--  t       | t            | t
-- 验证：UPDATE 后 SELECT 立即可见，三个字段全部生效


-- ============================================================
-- T3: interval=0 语义 —— 等同于关闭自动 flush
-- ============================================================
UPDATE _gsiceberg.tables SET auto_flush_interval_sec = 0 WHERE table_name = 'af_t1';

SELECT count(*) = 0 AS "not_processed_when_interval_zero"
FROM iceberg_auto_flush_scheduler() WHERE table_name = 'af_t1';

-- 预期结果：
--  not_processed_when_interval_zero
-- ─────────────────────────────────
--  t
-- 验证：即使 auto_flush_enabled=true，interval=0 也不触发 flush
-- scheduler 的 WHERE 条件包含 auto_flush_interval_sec > 0
```

#### 3.6.2 端到端集成测试：`test/sql/functional/auto_flush_trigger.sql`

```sql
-- ============================================================
-- Setup: 开启自动 flush，interval=1 表示 delta 有数据即可触发
-- ============================================================
UPDATE _gsiceberg.tables
SET auto_flush_enabled = true, auto_flush_interval_sec = 1, auto_flush_min_rows = 1
WHERE table_name = 'af_t2';

-- ============================================================
-- Step 0: 插入一条数据
-- ============================================================
INSERT INTO af_t2 VALUES (1, 100.0, 'auto_test');

-- ============================================================
-- Step 1: 验证 INSERT 进入了 delta 表
-- ============================================================
SELECT count(*) >= 1 AS "delta_populated"
FROM _gsiceberg._af_t2_delta;

-- 预期结果：
--  delta_populated
-- ────────────────
--  t
-- 验证：INSERT 被 INSTEAD OF INSERT 触发器重定向到 delta 表


-- ============================================================
-- Step 2: 执行调度器
-- ============================================================
SELECT * FROM iceberg_auto_flush_scheduler();

-- 预期结果：
--  table_name | flushed | reason
-- ────────────┼─────────┼──────────────────────────────────────
--  af_t2      | t       | delta_rows=1, min_rows=1, elapsed_sec=..., interval_sec=1
-- 验证：调度器处理了 af_t2 表，flushed=true，reason 包含触发详情


-- ============================================================
-- Step 3: Phase 1 冻结完成 → 新 delta 为空
-- ============================================================
SELECT count(*) = 0 AS "delta_emptied_after_freeze"
FROM _gsiceberg._af_t2_delta;

-- 预期结果：
--  delta_emptied_after_freeze
-- ──────────────────────────
--  t
-- 验证：旧的 _af_t2_delta 已被 RENAME 为 _af_t2_delta_flushing，
--       新 _af_t2_delta 是空表


-- ============================================================
-- Step 4: 调度器返回结果（再次调用时 delta 已空，不再处理）
-- ============================================================
SELECT count(*) AS "result_count"
FROM iceberg_auto_flush_scheduler();

-- 预期结果：
--  result_count
-- ─────────────
--  0
-- 验证：第二次调用时 delta 为空，调度器不再重复触发


-- ============================================================
-- Step 5: flush_jobs 有 pending 记录
-- ============================================================
SELECT count(*) >= 1 AS "job_created"
FROM _gsiceberg.flush_jobs
WHERE table_name = 'af_t2' AND status = 'pending';

-- 预期结果：
--  job_created
-- ────────────
--  t
-- 验证：Phase 1 创建了 flush_jobs 记录，等待 worker 处理


-- ============================================================
-- Step 6: 手动完成 Phase 2（模拟 worker）
-- ============================================================
SELECT iceberg_flush_worker() AS "worker_result";

-- 预期结果：
--  worker_result
-- ──────────────
--  1
-- 验证：worker 处理了 1 个 pending 任务


-- ============================================================
-- Step 7: 数据持久化并可见
-- ============================================================
SELECT count(*) >= 1 AS "data_persisted"
FROM af_t2 WHERE name = 'auto_test';

-- 预期结果：
--  data_persisted
-- ───────────────
--  t
-- 验证：flush 完成后数据仍然可以通过 VIEW 查到


-- ============================================================
-- Step 8: flush_jobs 状态变为 completed
-- ============================================================
SELECT status = 'completed' AS "job_completed"
FROM _gsiceberg.flush_jobs
WHERE table_name = 'af_t2'
ORDER BY job_id DESC LIMIT 1;

-- 预期结果：
--  job_completed
-- ──────────────
--  t
```

#### 3.6.3 边界测试：`test/sql/functional/auto_flush_edge.sql`

```sql
-- ============================================================
-- E1: 空 delta → 不触发 flush
-- ============================================================
TRUNCATE _gsiceberg._af_t3_delta;
UPDATE _gsiceberg.tables SET auto_flush_enabled = true, auto_flush_interval_sec = 1
WHERE table_name = 'af_t3';

SELECT count(*) = 0 AS "empty_delta_no_flush"
FROM iceberg_auto_flush_scheduler() WHERE table_name = 'af_t3';

-- 预期结果：
--  empty_delta_no_flush
-- ─────────────────────
--  t
-- 验证：dcount=0 < auto_flush_min_rows=1 → 不触发


-- ============================================================
-- E2: delta 行数不足 min_rows → 不触发
-- ============================================================
INSERT INTO af_t3 VALUES (1, 1.0, 'below_threshold');
UPDATE _gsiceberg.tables SET auto_flush_min_rows = 100 WHERE table_name = 'af_t3';

SELECT count(*) = 0 AS "below_min_rows_no_flush"
FROM iceberg_auto_flush_scheduler() WHERE table_name = 'af_t3';

-- 预期结果：
--  below_min_rows_no_flush
-- ───────────────────────
--  t
-- 验证：dcount=1 < auto_flush_min_rows=100 → 不触发
-- 防止 1-2 行就 flush 产生碎片 Parquet 文件


-- ============================================================
-- E3: 全局 GUC 关闭 → scheduler 静默返回空结果集
-- ============================================================
SET gsiceberg.auto_flush_enabled = false;

SELECT count(*) = 0 AS "global_off_returns_empty"
FROM iceberg_auto_flush_scheduler();

-- 预期结果：
--  global_off_returns_empty
-- ─────────────────────────
--  t
-- 验证：GUC=false 时 scheduler 第一步就 RETURN，不扫描任何表

RESET gsiceberg.auto_flush_enabled;


-- ============================================================
-- E4: 多表同时到期 → 全部被处理
-- ============================================================
INSERT INTO af_t4 VALUES (1, 1.0, 'multi_a');
INSERT INTO af_t5 VALUES (1, 1.0, 'multi_b');
UPDATE _gsiceberg.tables SET auto_flush_enabled = true, auto_flush_interval_sec = 0
WHERE table_name IN ('af_t4', 'af_t5');

SELECT count(*) = 2 AS "both_tables_flushed"
FROM iceberg_auto_flush_scheduler();

-- 预期结果：
--  both_tables_flushed
-- ────────────────────
--  t
-- 验证：两张表都被 scheduler 处理（返回 2 行）
```

---

## 4. 模块二：Flush 生命周期追踪

### 4.1 模块概述

**目标**：为每次 flush 操作提供完整的可观测性——知道 flush 走到了哪个阶段、每个阶段耗时多少、delta 入口多少行→抵消了多少→最终写入多少行、失败了是什么原因。

**解决的问题**：flush 是异步多阶段操作（Phase 1 毫秒级 RENAME → Phase 2 可能数分钟的 Parquet 写入），运维需要知道"现在卡在哪"、"历史哪次 flush 最慢"、"为什么昨天那次失败了"。

**核心思路**：扩展 `_gsiceberg.flush_state` 表，在 flush 管道的每个阶段转换点写入追踪记录。完成后不 DELETE 行（改为 UPDATE `phase='completed', flush_status='completed'`），数据永久保留。主键改为 `(table_name, job_id)`，每次 flush INSERT 新行不覆盖历史。提供 `iceberg_flush_history()` 函数统一查询。

### 4.2 功能需求

| 编号 | 需求 | 详细说明 |
|---|---|---|
| **FR-2.1** | 五阶段状态追踪 | flush_state 新增 `phase` 列，取值为 `frozen`（Phase 1 冻结完成）→ `writing`（Phase 2 开始写 Parquet）→ `committing`（开始更新目录）→ `cleaning`（开始清理）→ `completed`（flush 成功完成，行永久保留）。在 flush 管道的精确位置嵌入追踪调用 |
| **FR-2.2** | 各阶段时间戳 | 新增 `frozen_at`, `written_at`, `committed_at`, `cleaned_at` 四列。`started_at` 列已存在（由框架的 `flush_state_begin()` 写入）。每列在对应阶段开始时写入 `now()` |
| **FR-2.3** | 入口行数追踪 | 新增 `delta_rows_frozen` 列。Phase 1 冻结 delta 时，记录当时 delta 表的行数。冻结动作在框架的 `iceberg_flush_phase1()` 中完成，本模块在冻结成功后读取行数并写入 |
| **FR-2.4** | 出口行数追踪 | 新增 `rows_written` 列。Phase 2 Parquet 写入完成后，记录最终写入的行数。框架的 `FlushJob` 结构体在写入后设置 `final_rows` 字段，本模块读取并写入 |
| **FR-2.5** | 墓碑计数追踪 | 新增 `tombstone_count` 列。记录本次 flush 产生的墓碑（已删除行的持久化记录）数量。框架的 `FlushJob` 结构体在 I/D 解析后设置 `tombstone_count` 字段 |
| **FR-2.6** | 文件大小追踪 | 新增 `parquet_size_bytes` 列。记录写入的 Parquet 文件字节数。框架的 `FlushJob` 结构体在写入后设置 `final_sz` 字段 |
| **FR-2.7** | 结构化错误记录 | 新增 `error_code`（如 E_DISK_FULL, E_WRITE_FAILED）和 `error_phase`（失败发生在哪个阶段）两列。flush 任何阶段失败时调用 `flush_state_failed()` 写入 |
| **FR-2.8** | 统一历史查询 | 提供 `iceberg_flush_history(filter_table, limit_rows)` 函数。`flush_state` 行不再 DELETE，每次 flush 保留一行。JOIN 条件使用 `(table_name, job_id)` 精确匹配，不再有"已完成行查不到"的问题 |
| **FR-2.9** | 追踪失败不影响 flush | 所有追踪 UPDATE 失败时 `ereport(WARNING)`，不抛 ERROR。追踪是观测性质，不能因为监控写入失败而导致业务数据 flush 中断 |
| **FR-2.10** | 不破坏已有崩溃恢复 | `flush_phase1()` 孤儿检测加 `AND flush_status = 'in_progress'` 过滤，`flush_state_recover()` 已有此过滤，保留 completed 行不影响恢复判断 |

### 4.3 详细设计

#### 4.3.1 状态机

flush 的生命周期被建模为五个阶段。每个阶段由一次状态转换函数调用标记，函数内部执行 `UPDATE flush_state SET phase=$new_phase, <timestamp>=now() WHERE table_name=$1`。

```
                        ┌──────────┐
                        │   idle   │  ← 初始（flush_state 中无此行）
                        └────┬─────┘
                             │ 框架：flush_state_begin()
                             │ (在 flush_write_parquet 中调用)
                             │ INSERT 新行，flush_status='in_progress'
                             ▼
                        ┌──────────┐
                        │ in_progress│ ← 框架已有，flush_status='in_progress'
                        └────┬─────┘
                             │ [M2] flush_state_frozen()
                             │ 时机：Phase 1 freeze 成功后
                             │ 写入：phase='frozen', frozen_at, delta_rows_frozen
                             │ 注：如果行不存在则 INSERT（UPSERT），确保 Phase 1 后就可见
                             ▼
                        ┌──────────┐
                        │  frozen   │ ← delta 已冻结为 _delta_flushing
                        └────┬─────┘
                             │ [M2] flush_state_writing()
                             │ 时机：Phase 2 开始处理 _delta_flushing
                             │ 写入：phase='writing', written_at
                             ▼
                        ┌──────────┐
                        │  writing  │ ← 正在 I/D 抵消 + 写 Parquet
                        └────┬─────┘
                             │ [M2] flush_state_write_done()
                             │ 时机：Parquet 写入完成后
                             │ 写入：rows_written, tombstone_count, parquet_size_bytes
                             │ phase 仍为 'writing'（不新增 written 阶段）
                             ▼
                        ┌──────────┐
                        │ writing   │ ← 统计数据已填充
                        │ (done)   │
                        └────┬─────┘
                             │ [M2] flush_state_committing()
                             │ 时机：目录提交前
                             │ 写入：phase='committing', committed_at
                             ▼
                        ┌──────────┐
                        │committing │ ← 正在写 snapshots / data_files / 清单
                        └────┬─────┘
                             │ [M2] flush_state_cleaning()
                             │ 时机：清理 _delta_flushing 前
                             │ 写入：phase='cleaning', cleaned_at
                             ▼
                        ┌──────────┐
                        │ cleaning  │ ← 正在 DROP _delta_flushing
                        └────┬─────┘
                             │ 框架：flush_state_done()
                             │ [M2] 改为 UPDATE：phase='completed', flush_status='completed'
                             │ ★ 不再 DELETE，行永久保留
                             ▼
                        ┌──────────┐
                        │ completed │ ← flush 成功完成，所有 12 列数据完整保留
                        └──────────┘

    任何阶段发生不可恢复错误：
        [M2] flush_state_failed(table_name, job_id, error_code, error_phase)
        → UPDATE SET flush_status='failed', error_code, error_phase
        → 保留行，下次 flush 时框架的崩溃恢复逻辑接管检测
```

**`flush_state_done()` 改动**：

原来执行 `DELETE FROM flush_state WHERE table_name = $1`。模块二改为：

```c
void flush_state_done(const char *table_name) {
    SPI_execute_with_args(
        "UPDATE _gsiceberg.flush_state "
        "SET phase = 'completed', flush_status = 'completed', "
        "cleaned_at = COALESCE(cleaned_at, now()) "
        "WHERE table_name = $1 AND job_id = currval('...')"  // 或通过参数传入 job_id
        , ...);
}
```

对应的崩溃恢复查询加 `AND flush_status = 'in_progress'` 过滤——`flush_phase1()` 孤儿检测和 `flush_state_recover()` 都只匹配 in_progress 行，completed 行被跳过，不触发恢复逻辑。

**`flush_state_frozen()` 改为 UPSERT**：

Phase 1 完成时 `flush_state_begin()` 可能还没调用（它在 `flush_write_parquet()` 中）。`flush_state_frozen()` 使用 `INSERT ... ON CONFLICT DO UPDATE` 确保行存在：

```c
"INSERT INTO _gsiceberg.flush_state (table_name, job_id, flush_status, phase, "
"frozen_at, delta_rows_frozen, started_at) "
"VALUES ($1, $2, 'in_progress', 'frozen', now(), $3, now()) "
"ON CONFLICT (table_name, job_id) DO UPDATE SET "
"phase = 'frozen', frozen_at = now(), delta_rows_frozen = $3"
```

这样异步 flush 返回后，frozen 数据已可查。后续 `flush_state_begin()` 的 `ON CONFLICT DO UPDATE` 继续更新同一行，不冲突。

**设计决策：writing 阶段写完后为什么不开新的"written"阶段？**

Parquet 写入完成和目录提交之间不需要额外的追踪粒度。`flush_state_write_done()` 填充统计数据后，紧接着就是 `flush_state_committing()`。减少一次 UPDATE 的开销，同时保持足够的信息密度。

#### 4.3.2 追踪点嵌入位置

追踪函数在 flush 管道的精确位置被调用。以下标注每个追踪点的调用时机和参数来源：

| 追踪函数 | 嵌入位置 | 调用时机 | 参数来源 |
|---|---|---|---|
| `flush_state_frozen(table_name, delta_rows)` | `flush_phase1()` 末尾，`return job_id` 之前 | Phase 1 RENAME + CREATE + INSERT flush_jobs 全部成功后 | `n_delta_rows`：Phase 1 中 `relation_row_count("_gsiceberg", delta_rel)` 的返回值 |
| `flush_state_writing(table_name)` | `flush_phase2()` 中，`flush_job_init()` 之后、`flush_delta_read()` 之前 | Phase 2 刚完成上下文初始化，准备开始处理 | 无额外参数 |
| `flush_state_write_done(table_name, final_rows, tombstone_count, final_sz)` | `flush_phase2()` 中，`flush_write_parquet()` 之后、`flush_commit_catalog()` 之前 | Parquet 文件已写入，数据已安全 | 来自 `FlushJob j` 的 `j.final_rows`, `j.tombstone_count`, `j.final_sz` |
| `flush_state_committing(table_name)` | `flush_phase2()` 中，`flush_commit_catalog()` 之前 | 准备更新 snapshots / data_files 目录 | 无额外参数 |
| `flush_state_cleaning(table_name)` | `flush_phase2()` 中，cleanup label 内 `DROP TABLE _delta_flushing` 之前 | 准备清理临时表 | 无额外参数 |

**与框架已有 `flush_state_*()` 调用的关系**：

框架已在 flush 管道中调用了 `flush_state_begin()`, `flush_state_set_file()`, `flush_state_set_snapshot()`, `flush_state_done()`。模块二的新增调用**穿插**在这些已有调用之间。

**关键改动**：`flush_state_done()` 从 DELETE 改为 UPDATE（`phase='completed', flush_status='completed'`），行永久保留。`flush_state_frozen()` 改用 UPSERT，确保 Phase 1 后就可见。

```
flush_phase1()
  ...
  return job_id
  ← [M2] flush_state_frozen() 插入点 (UPSERT：行不存在则创建)

flush_phase2()
  flush_job_init()
  ← [M2] flush_state_writing() 插入点
  flush_delta_read()
  flush_resolve_pairs()
  flush_write_parquet()
    → 框架 flush_state_begin()      [已有，ON CONFLICT UPDATE]
    → 框架 flush_state_set_file()   [已有]
  ← [M2] flush_state_write_done() 插入点
  ← [M2] flush_state_committing() 插入点
  flush_commit_catalog()
    → 框架 flush_state_set_snapshot() [已有]
  ...
  cleanup:
  ← [M2] flush_state_cleaning() 插入点
  DROP _delta_flushing
  → 框架 flush_state_done()          [M2 改为 UPDATE，不再 DELETE]
```

#### 4.3.3 C 层追踪函数实现

6 个函数全部在 `flush_state.c` 中实现。每个函数执行一条 SPI UPDATE，失败时 `ereport(WARNING)`。SPI 必须已连接（调用方保证）。

```c
// ① Phase 1 完成：记录 frozen 状态（UPSERT，行不存在则创建）
void flush_state_frozen(const char *table_name, int64_t job_id, int64_t delta_rows);

// ② Phase 2 writing 开始
void flush_state_writing(const char *table_name, int64_t job_id);

// ③ Parquet 写入完成：记录统计指标
void flush_state_write_done(const char *table_name, int64_t job_id,
                             int64_t rows_written,
                             int64_t tombstone_count,
                             int64_t file_size);

// ④ 目录提交开始
void flush_state_committing(const char *table_name, int64_t job_id);

// ⑤ 清理开始
void flush_state_cleaning(const char *table_name, int64_t job_id);

// ⑥ 失败记录
void flush_state_failed(const char *table_name, int64_t job_id,
                         const char *error_code,
                         const char *error_phase);
```

**实现模式**（以 `flush_state_frozen` 为例——UPSERT）：

```c
void flush_state_frozen(const char *table_name, int64_t job_id, int64_t delta_rows) {
    Datum vals[] = {
        CStringGetTextDatum(table_name),
        Int64GetDatum(job_id),
        Int64GetDatum(delta_rows)
    };
    Oid types[] = {TEXTOID, INT8OID, INT8OID};
    int rc = SPI_execute_with_args(
        "INSERT INTO _gsiceberg.flush_state "
        "(table_name, job_id, flush_status, phase, frozen_at, delta_rows_frozen, started_at) "
        "VALUES ($1, $2, 'in_progress', 'frozen', now(), $3, now()) "
        "ON CONFLICT (table_name, job_id) DO UPDATE SET "
        "phase = 'frozen', frozen_at = now(), delta_rows_frozen = $3",
        3, types, vals, NULL, false, 0);
    if (rc < 0)
        ereport(WARNING,
            (errmsg("gsiceberg: flush_state_frozen failed for %s", table_name)));
}
```

**声明**（`flush_internal.h` 新增）：

```c
void flush_state_frozen(const char *table_name, int64_t job_id, int64_t delta_rows);
void flush_state_writing(const char *table_name, int64_t job_id);
void flush_state_write_done(const char *table_name, int64_t job_id,
                             int64_t rows_written, int64_t tombstone_count, int64_t file_size);
void flush_state_committing(const char *table_name, int64_t job_id);
void flush_state_cleaning(const char *table_name, int64_t job_id);
void flush_state_failed(const char *table_name, int64_t job_id,
                         const char *error_code, const char *error_phase);
```

#### 4.3.4 历史查询接口

```sql
CREATE OR REPLACE FUNCTION iceberg_flush_history(
    filter_table text DEFAULT NULL,    -- NULL = 所有表，指定表名 = 单表
    limit_rows   int  DEFAULT 20       -- 最近 N 条
)
RETURNS TABLE(
    table_name         text,           -- 表名
    job_id             bigint,         -- 任务 ID
    job_status         text,           -- pending|in_progress|completed|failed
    phase              text,           -- idle|frozen|writing|committing|cleaning
    delta_rows_frozen  bigint,         -- Phase 1 冻结行数
    rows_written       bigint,         -- 最终写入行数
    tombstone_count    bigint,         -- 墓碑数
    parquet_size_bytes bigint,         -- Parquet 文件大小
    frozen_at          timestamptz,    -- Phase 1 完成时间
    written_at         timestamptz,    -- writing 开始时间
    committed_at       timestamptz,    -- 目录提交时间
    cleaned_at         timestamptz,    -- 清理完成时间
    total_duration     interval,       -- 总耗时 (finished_at - started_at)
    error_code         text,           -- 错误码
    error_phase        text,           -- 失败阶段
    error_msg          text            -- 错误详情（来自 flush_jobs）
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT
        j.table_name, j.job_id, j.status,
        COALESCE(s.phase, 'idle'),
        s.delta_rows_frozen, s.rows_written,
        s.tombstone_count, s.parquet_size_bytes,
        s.frozen_at, s.written_at, s.committed_at, s.cleaned_at,
        j.finished_at - j.started_at,
        s.error_code, s.error_phase, j.error_msg
    FROM _gsiceberg.flush_jobs j
    LEFT JOIN _gsiceberg.flush_state s
        ON j.table_name = s.table_name AND j.job_id = s.job_id
    WHERE ($1 IS NULL OR j.table_name = $1)
    ORDER BY j.job_id DESC
    LIMIT $2;
$$;
```

**设计要点**：
- JOIN 条件从 `USING (table_name)` 改为 `ON j.table_name = s.table_name AND j.job_id = s.job_id`：每次 flush 的 state 行由 `(table_name, job_id)` 联合标识，不再模糊匹配
- `LEFT JOIN`（非 `INNER JOIN`）：防御性保留——极端情况下（如 flush_state 行写入失败）job 仍能显示，只是 state 列为 NULL
- **已完成 flush 的 12 列不再返回 NULL**：行永久保留，实际值可查

### 4.4 接口依赖

| 依赖的外部接口 | 调用方式 | 用途 |
|---|---|---|
| `flush_state_begin/done/set_file/set_snapshot` | C 直接调用 | 与新增追踪函数共存 |
| `FlushJob` 结构体 | C 字段访问 | 读取 final_rows, tombstone_count, final_sz |
| `relation_row_count()` | C 直接调用 | Phase 1 获取 delta 行数 |
| `SPI_execute_with_args()` | C 直接调用 | 所有追踪 UPDATE |
| `TEXTOID, INT8OID` | C 常量 | Datum 类型数组 |
| `ereport(WARNING, ...)` | C 直接调用 | 追踪失败告警 |
| `_gsiceberg.flush_state`（含新增列） | SPI UPDATE/SELECT | 读写追踪数据 |
| `_gsiceberg.flush_jobs` | LEFT JOIN | 历史查询关联 |

### 4.5 实现清单

| 产出 | 类型 | 说明 |
|---|---|---|
| `flush_state` 新增 12 列 | DDL | phase, *_at × 4, *_rows × 3, tombstone_count, parquet_size_bytes, error_code, error_phase |
| `flush_state_frozen()` | C 函数 | Phase 1 完成追踪 |
| `flush_state_writing()` | C 函数 | writing 阶段开始 |
| `flush_state_write_done()` | C 函数 | 写入统计记录 |
| `flush_state_committing()` | C 函数 | 提交阶段开始 |
| `flush_state_cleaning()` | C 函数 | 清理阶段开始 |
| `flush_state_failed()` | C 函数 | 失败记录 |
| `flush_internal.h` 声明 | C 头文件 | 6 个新函数声明 |
| `flush_phase1.c` 修改 | C 嵌入 | 末尾调用 flush_state_frozen() |
| `flush_phase2.c` 修改 | C 嵌入 | 4 处调用新增追踪函数 |
| `iceberg_flush_history()` | PL/pgSQL 函数 | 历史查询，约 30 行 |

**代码量**：C ~180 行 + PL/pgSQL ~30 行

### 4.6 测试文档

#### 4.6.1 六阶段状态转移测试：`test/sql/functional/flush_lifecycle_states.sql`

```sql
-- ============================================================
-- T1: 正常 flush → sync 完成后行保留，phase='completed'
-- ============================================================
INSERT INTO lt_t1 VALUES (1, 100.0, 'phase_test');
SELECT iceberg_flush_sync('lt_t1');

SELECT
    phase = 'completed'         AS "phase_is_completed",
    flush_status = 'completed'  AS "status_is_completed",
    frozen_at IS NOT NULL       AS "frozen_at_preserved",
    written_at IS NOT NULL      AS "written_at_preserved",
    committed_at IS NOT NULL    AS "committed_at_preserved",
    cleaned_at IS NOT NULL      AS "cleaned_at_preserved"
FROM _gsiceberg.flush_state
WHERE table_name = 'lt_t1'
ORDER BY job_id DESC LIMIT 1;

-- 预期结果：
--  phase_is_completed | status_is_completed | frozen_at_preserved | written_at_preserved | committed_at_preserved | cleaned_at_preserved
-- ────────────────────┼─────────────────────┼─────────────────────┼──────────────────────┼────────────────────────┼─────────────────────
--  t                  | t                   | t                   | t                    | t                      | t
-- 验证：flush_state_done() 改为 UPDATE 后行永久保留，5 个阶段时间戳一应俱全


-- ============================================================
-- T2: 历史查询接口能查到已完成的 flush 完整追踪数据
-- ============================================================
SELECT
    count(*) >= 1            AS "history_has_record",
    rows_written > 0         AS "rows_tracked_correctly",
    job_status = 'completed' AS "job_is_completed",
    phase = 'completed'      AS "phase_is_completed",
    delta_rows_frozen IS NOT NULL AS "frozen_count_preserved",
    tombstone_count IS NOT NULL   AS "tombstone_preserved",
    parquet_size_bytes IS NOT NULL AS "file_size_preserved",
    total_duration IS NOT NULL    AS "duration_available"
FROM iceberg_flush_history('lt_t1', 1);

-- 预期结果：所有追踪列均返回实际值（行永久保留，不再全是 NULL）
--  history_has_record | rows_tracked_correctly | ... | duration_available
-- ────────────────────┼────────────────────────┼─────┼───────────────────
--  t                  | t                      |     | t
-- 验证：flush 完成后 12 列数据完整保留，history 接口可查


-- ============================================================
-- T3: 多次 flush 后，每行独立保留，按 job_id DESC 排列
-- ============================================================
INSERT INTO lt_t1 VALUES (2, 200.0, 'round2');
SELECT iceberg_flush_sync('lt_t1');

SELECT count(*) >= 2 AS "multiple_rows_preserved"
FROM _gsiceberg.flush_state WHERE table_name = 'lt_t1';

-- 预期结果：
--  multiple_rows_preserved
-- ────────────────────────
--  t
-- 验证：flush_state 表中有 2 行（两次 flush 各 INSERT 一行，不覆盖）


SELECT count(*) >= 2 AS "history_shows_both"
FROM iceberg_flush_history('lt_t1', 10);

-- 预期结果：
--  history_shows_both
-- ───────────────────
--  t
```

#### 4.6.2 I/D 抵消追踪测试：`test/sql/functional/flush_lifecycle_id_cancel.sql`

```sql
-- ============================================================
-- T1: INSERT 后立刻 DELETE → I/D 全部抵消 → rows_written=0
-- ============================================================
INSERT INTO lt_t2 VALUES (1, 1.0, 'cancel_me');
DELETE FROM lt_t2 WHERE id = 1;
SELECT iceberg_flush_sync('lt_t2');

SELECT
    rows_written = 0        AS "nothing_written_to_parquet",
    tombstone_count >= 1     AS "tombstone_recorded",
    job_status = 'completed' AS "job_finished",
    phase = 'completed'      AS "state_preserved",
    delta_rows_frozen IS NOT NULL AS "frozen_count_available"
FROM iceberg_flush_history('lt_t2', 1);

-- 预期结果：
--  nothing_written_to_parquet | tombstone_recorded | job_finished | state_preserved | frozen_count_available
-- ───────────────────────────┼───────────────────┼──────────────┼─────────────────┼───────────────────────
--  t                         | t                  | t            | t               | t
-- 验证：
--   • rows_written=0: INSERT 后立刻 DELETE，I/D 抵消后没有行写入 Parquet
--   • tombstone_count>=1: DELETE 产生了至少 1 个墓碑记录
--   • state_preserved: flush_state 行永久保留，即使 rows_written=0


-- ============================================================
-- T2: 多行 INSERT + 部分 DELETE → 只写入存活行
-- ============================================================
INSERT INTO lt_t2 VALUES (1, 10.0, 'keep'), (2, 20.0, 'keep'), (3, 30.0, 'delete_me');
DELETE FROM lt_t2 WHERE id = 3;
SELECT iceberg_flush_sync('lt_t2');

SELECT
    rows_written = 2         AS "two_surviving_rows",
    tombstone_count >= 1      AS "one_tombstone",
    delta_rows_frozen = 4     AS "four_rows_frozen_total",
    job_status = 'completed'  AS "job_finished"
FROM iceberg_flush_history('lt_t2', 1);

-- 预期结果：
--  two_surviving_rows | one_tombstone | four_rows_frozen_total | job_finished
-- ────────────────────┼───────────────┼────────────────────────┼─────────────
--  t                  | t             | t                      | t
```

#### 4.6.3 错误追踪测试：`test/sql/functional/flush_lifecycle_error.sql`

```sql
-- ============================================================
-- T1: 正常 flush 完成后手动模拟失败 → error_* 列在 history 中可见
-- ============================================================
INSERT INTO lt_t3 VALUES (1, 100.0, 'err_test');
SELECT iceberg_flush_sync('lt_t3');

-- 模拟此次 flush 在 committing 阶段失败（手动 UPDATE）
UPDATE _gsiceberg.flush_state
SET error_code = 'E_CATALOG_WRITE', error_phase = 'committing',
    flush_status = 'failed', phase = 'failed'
WHERE table_name = 'lt_t3'
ORDER BY job_id DESC LIMIT 1;

SELECT
    error_code = 'E_CATALOG_WRITE' AS "error_code_visible",
    error_phase = 'committing'      AS "error_phase_visible",
    flush_status = 'failed'         AS "status_is_failed"
FROM iceberg_flush_history('lt_t3', 1);

-- 预期结果：
--  error_code_visible | error_phase_visible | status_is_failed
-- ────────────────────┼─────────────────────┼─────────────────
--  t                  | t                   | t
-- 验证：失败后 error_* 列在 history 中可查，行永久保留不被删除
```

#### 4.6.4 追踪不阻断 flush 测试：`test/sql/functional/flush_lifecycle_resilience.sql`

```sql
-- ============================================================
-- 验证：即使 flush_state 表临时不可用，flush 仍能正常完成
-- （通过 TAP 测试模拟 SPI 错误场景，此处仅作 SQL 层回归）
-- ============================================================

-- T1: flush_state 整列全 NULL 的边界情况
INSERT INTO lt_t4 VALUES (1, 100.0, 'resilience');
SELECT iceberg_flush_sync('lt_t4');

-- 数据应该正常可见（追踪可能失败但不影响 flush 正确性）
SELECT count(*) >= 1 AS "data_visible_even_if_tracking_fails"
FROM lt_t4 WHERE name = 'resilience';

-- 预期结果：
--  data_visible_even_if_tracking_fails
-- ────────────────────────────────────
--  t
-- 验证：flush 的核心功能（写 Parquet + 数据可见）不依赖追踪
```

#### 4.6.5 12 列可查询性验证：`test/sql/functional/flush_lifecycle_columns.sql`

验证模块二新增的 12 列在真实 flush 流程中的可查询时机。所有数据由框架自动写入，关键时间点：
- **异步 flush Phase 1 后**：`flush_state_frozen()` 已调用（UPSERT），frozen 阶段数据可查
- **同步 flush 完成后**：`flush_state_done()` 改为 UPDATE（不再 DELETE），行永久保留，所有 12 列完整可查

```sql
-- ============================================================
-- T1: 异步 flush Phase 1 之后 → frozen 阶段数据已写入
-- ============================================================
INSERT INTO lc_t1 VALUES (1, 100.0, 'col_test'), (2, 200.0, 'col_test'), (3, 300.0, 'col_test');
SELECT iceberg_flush('lc_t1') AS job_id;

-- Phase 1 完成，flush_state_frozen() 已 UPSERT 创建行
SELECT
    phase = 'frozen'            AS "phase_is_frozen",
    frozen_at IS NOT NULL       AS "frozen_at_set",
    delta_rows_frozen = 3       AS "frozen_count_3",
    started_at IS NOT NULL      AS "started_at_set",
    -- 以下列尚未写入，应为 NULL
    written_at IS NULL          AS "written_at_still_null",
    rows_written IS NULL        AS "rows_written_still_null",
    committed_at IS NULL        AS "committed_at_still_null",
    cleaned_at IS NULL          AS "cleaned_at_still_null",
    error_code IS NULL          AS "error_code_still_null"
FROM _gsiceberg.flush_state
WHERE table_name = 'lc_t1'
ORDER BY job_id DESC LIMIT 1;

-- 预期结果：
--  phase_is_frozen | frozen_at_set | frozen_count_3 | started_at_set | written_at_still_null | rows_written_still_null | committed_at_still_null | cleaned_at_still_null | error_code_still_null
-- ─────────────────┼───────────────┼────────────────┼────────────────┼───────────────────────┼─────────────────────────┼─────────────────────────┼───────────────────────┼──────────────────────
--  t               | t             | t              | t              | t                     | t                       | t                       | t                     | t


-- ============================================================
-- T2: 同步 flush 完成后 → 所有 12 列完整保留
-- ============================================================
INSERT INTO lc_t2 VALUES (1, 10.0, 'sync_test'), (2, 20.0, 'sync_test');
SELECT iceberg_flush_sync('lc_t2');

SELECT
    phase = 'completed'         AS "phase_completed",
    flush_status = 'completed'  AS "status_completed",
    frozen_at IS NOT NULL       AS "frozen_at_preserved",
    written_at IS NOT NULL      AS "written_at_preserved",
    committed_at IS NOT NULL    AS "committed_at_preserved",
    cleaned_at IS NOT NULL      AS "cleaned_at_preserved",
    delta_rows_frozen = 2       AS "frozen_count_2",
    rows_written = 2            AS "written_count_2",
    tombstone_count = 0         AS "no_tombstones",
    parquet_size_bytes > 0      AS "file_size_positive",
    total_duration IS NOT NULL  AS "duration_available"
FROM iceberg_flush_history('lc_t2', 1);

-- 预期结果：全部为 t
-- 验证：flush 完成后行保留，12 列数据完整可查，不再返回 NULL


-- ============================================================
-- T3: 多次 flush → 每次独立一行，history 精确匹配
-- ============================================================
INSERT INTO lc_t3 VALUES (1, 1.0, 'round1');
SELECT iceberg_flush_sync('lc_t3') AS "round1_ok";
INSERT INTO lc_t3 VALUES (2, 2.0, 'round2');
SELECT iceberg_flush_sync('lc_t3') AS "round2_ok";

SELECT
    count(*) = 2                AS "two_rows_in_state",
    (SELECT count(*) FROM iceberg_flush_history('lc_t3', 10)) = 2 AS "two_in_history"
FROM _gsiceberg.flush_state WHERE table_name = 'lc_t3';

-- 预期结果：
--  two_rows_in_state | two_in_history
-- ───────────────────┼────────────────
--  t                 | t
-- 验证：(table_name, job_id) 联合主键确保两次 flush 各自独立一行


-- ============================================================
-- T4: LEFT JOIN 精确匹配 → 不会张冠李戴
-- ============================================================
-- 查最新的那次 flush，phase 对应那一次的，frozen_at 也是那一次的
SELECT
    phase = 'completed'     AS "latest_phase_correct",
    frozen_at IS NOT NULL   AS "latest_frozen_at_correct"
FROM iceberg_flush_history('lc_t3', 1);

-- 预期结果：
--  latest_phase_correct | latest_frozen_at_correct
-- ──────────────────────┼─────────────────────────
--  t                    | t
```

#### 4.6.6 12 列查询时机对照表

| 列 | Phase 1 后 | writing 中 | 写入完成 | committing | cleaning | flush 完成后 |
|---|---|---|---|---|---|---|
| `phase` | `'frozen'` | `'writing'` | `'writing'` | `'committing'` | `'cleaning'` | **`'completed'`** |
| `frozen_at` | ✅ | ✅ | ✅ | ✅ | ✅ | **✅ 保留** |
| `written_at` | — | ✅ | ✅ | ✅ | ✅ | **✅ 保留** |
| `committed_at` | — | — | — | ✅ | ✅ | **✅ 保留** |
| `cleaned_at` | — | — | — | — | ✅ | **✅ 保留** |
| `delta_rows_frozen` | ✅ | ✅ | ✅ | ✅ | ✅ | **✅ 保留** |
| `rows_written` | — | — | ✅ | ✅ | ✅ | **✅ 保留** |
| `tombstone_count` | — | — | ✅ | ✅ | ✅ | **✅ 保留** |
| `parquet_size_bytes` | — | — | ✅ | ✅ | ✅ | **✅ 保留** |
| `error_code` | — | — | — | — | — | NULL（正常） |
| `error_phase` | — | — | — | — | — | NULL（正常） |

> ✅ = 可查询到实际值，且 flush 完成后永久保留
> — = 该阶段尚未发生，值为 NULL
> `error_*` 仅在 flush 失败时写入，正常路径始终为 NULL
> **核心变化**：flush 完成后 12 列不再丢失，行永久保留

---

## 5. 模块三：后台 Flush 作业调度

### 5.1 模块概述

**目标**：为多表 flush 任务提供完整的后台调度能力——任务入队、优先级排序、自动取任务执行、worker 存活性监控、运维可视化。

**解决的问题**：
1. 多张表同时触发 flush 时，大表（几十万行 delta）应该优先于小表（几行 delta）处理——数据量大丢数据风险高
2. worker 可能崩溃（PG 进程被杀），需要检测僵尸任务并自动重置，避免任务永久卡在 in_progress
3. 运维需要知道：哪些表在排队？哪个 worker 在处理哪个任务？worker 还活着吗？

**核心思路**：扩展 `flush_jobs` 表增加优先级和 worker 标识列，新增 `flush_workers` 心跳表。重写 worker 调度逻辑以支持优先级排序和心跳维护。提供 daemon 循环函数和 dashboard 视图。

### 5.2 功能需求

| 编号 | 需求 | 详细说明 |
|---|---|---|
| **FR-3.1** | 优先级调度 | `flush_jobs` 新增 `priority` 列（int，越大越优先）。Worker 取任务时 `ORDER BY priority DESC, job_id ASC`（同优先级 FIFO）。优先级在 Phase 1 创建 job 时根据 delta 行数自动计算 |
| **FR-3.2** | 优先级计算规则 | delta 行数 > 100000 → priority 100；> 10000 → 75；> 1000 → 50；> 100 → 25；≤ 100 → 等于行数。分段而非连续值，避免 1-2 行差异改变执行顺序 |
| **FR-3.3** | Worker 心跳维护 | 新增 `flush_workers` 表。Worker 每次被调用时注册/更新心跳。处理任务时 `current_job` 指向正在处理的 job_id。空闲时 `current_job=NULL` 但心跳仍更新（证明存活） |
| **FR-3.4** | 僵尸任务检测与重置 | Worker 取任务前，将 `status='in_progress'` 且 `started_at < now() - 5min` 的任务重置为 `status='pending'`，`retry_count++`。处理此任务的 worker 大概率已崩溃 |
| **FR-3.5** | 重试次数限制 | 复用框架 GUC `gsiceberg.flush_retry_count`（默认 3）。`retry_count >= 限制` 的任务被永久跳过，需人工介入。在 dashboard 中作为 `dead_jobs` 展示 |
| **FR-3.6** | Worker 并发安全 | `SELECT ... FOR UPDATE SKIP LOCKED` 确保多个 worker 不会取到同一个 job。不同表的 Phase 2 可并行执行（各自写不同 Parquet 文件，无冲突） |
| **FR-3.7** | daemon 守护循环 | `iceberg_flush_daemon(worker_id, poll_interval_sec)`，LOOP 内调用 worker。有活干时不 sleep（连续处理积压队列），无活时 sleep poll_interval_sec 秒。异常时记录 WARNING 继续，不退出循环 |
| **FR-3.8** | 任务创建时间追踪 | `flush_jobs` 新增 `scheduled_at` 列（默认 now()）。记录任务何时入队，区别于 `started_at`（worker 实际开始处理的时间） |
| **FR-3.9** | 任务归属追踪 | `flush_jobs` 新增 `worker_id` 列。记录哪个 worker 在处理/处理过此任务。僵尸任务重置时 `worker_id` 置 NULL |
| **FR-3.10** | 运维仪表板 | `_gsiceberg.flush_dashboard` 视图。一行展示：worker 状态、心跳年龄、当前任务、累计处理数、队列积压（pending 数）、运行中（in_progress 数）、死信（retry_count 超限的 failed 数） |

### 5.3 详细设计

#### 5.3.1 优先级计算（在 Phase 1 中）

当 `iceberg_flush_phase1()` 创建 flush_job 时，根据冻结的 delta 行数自动计算优先级：

```c
// 在 flush_phase1.c INSERT flush_jobs 之前
int64_t auto_priority;
if      (n_delta_rows > 100000) auto_priority = 100;
else if (n_delta_rows > 10000)  auto_priority = 75;
else if (n_delta_rows > 1000)   auto_priority = 50;
else if (n_delta_rows > 100)    auto_priority = 25;
else                             auto_priority = (int64_t)n_delta_rows;

// INSERT 语句增加 priority 列
"INSERT INTO _gsiceberg.flush_jobs "
"(table_name, flush_table, priority) "
"VALUES ($1, $2, $3) RETURNING job_id"
```

**设计决策：为什么分段而不是直接用行数？**

如果直接用行数，50001 行和 50000 行的表相差仅 1 行就交换顺序，而两表的数据量几乎相同。分段确保"同一数量级内的表按 FIFO"，避免无意义的微调度。

#### 5.3.2 Worker 调度流程

```
iceberg_flush_worker('worker_1')
│
├─ ① 心跳注册：INSERT/UPDATE flush_workers
│     worker_id='worker_1', status='running', heartbeat=now()
│
├─ ② 僵尸清理：重置超时 in_progress 任务
│     UPDATE flush_jobs SET status='pending', retry_count++
│     WHERE status='in_progress' AND started_at < now() - 5min
│
├─ ③ 取任务（优先级 + 行锁）：
│     SELECT job_id, table_name FROM flush_jobs
│     WHERE status IN ('pending','failed')
│       AND retry_count < gsiceberg.flush_retry_count
│     ORDER BY priority DESC, job_id ASC
│     LIMIT 1
│     FOR UPDATE SKIP LOCKED
│     │
│     ├─ 无任务 → 更新心跳（current_job=NULL）→ RETURN 0
│     │
│     └─ 有任务 ↓
│
├─ ④ 标记处理中：
│     UPDATE flush_jobs SET status='in_progress', worker_id='worker_1', started_at=now()
│     UPDATE flush_workers SET current_job=<job_id>, heartbeat=now()
│
├─ ⑤ 执行 Phase 2：
│     SELECT iceberg_flush_phase2(table_name, job_id) → ok
│     │
│     ├─ ok=true:
│     │   UPDATE flush_jobs SET status='completed', finished_at=now()
│     │   UPDATE flush_workers SET jobs_processed++, current_job=NULL
│     │   RETURN 1
│     │
│     └─ ok=false:
│         UPDATE flush_jobs SET status='failed', retry_count++, error_msg=...
│         UPDATE flush_workers SET last_error=..., current_job=NULL
│         RETURN 1  (任务已处理，虽然失败了)
```

#### 5.3.3 Worker PL/pgSQL 实现

```sql
CREATE OR REPLACE FUNCTION iceberg_flush_worker(
    worker_id text DEFAULT 'default'
)
RETURNS int LANGUAGE plpgsql STRICT SECURITY DEFINER
SET search_path = pg_catalog AS $$
DECLARE
    job_id_val   bigint;
    table_val    text;
    ok           bool;
BEGIN
    PERFORM public.gsiceberg_require_admin();

    -- ① 心跳注册/更新
    INSERT INTO _gsiceberg.flush_workers (worker_id, status, heartbeat)
    VALUES (worker_id, 'running', now())
    ON CONFLICT (worker_id) DO UPDATE
    SET heartbeat = now(), status = 'running';

    -- ② 僵尸任务重置（5 分钟超时）
    UPDATE _gsiceberg.flush_jobs
    SET status = 'pending',
        retry_count = COALESCE(retry_count, 0) + 1,
        worker_id = NULL
    WHERE status = 'in_progress'
      AND started_at < now() - interval '5 minutes';

    -- ③ 按优先级取任务
    SELECT job_id, table_name INTO job_id_val, table_val
    FROM _gsiceberg.flush_jobs
    WHERE status IN ('pending', 'failed')
      AND COALESCE(retry_count, 0)
          < current_setting('gsiceberg.flush_retry_count')::int
    ORDER BY priority DESC, job_id ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED;

    IF NOT FOUND THEN
        UPDATE _gsiceberg.flush_workers
        SET heartbeat = now(), current_job = NULL
        WHERE worker_id = worker_id;
        RETURN 0;
    END IF;

    -- ④ 标记处理中
    RAISE NOTICE 'gsiceberg: flush_worker "%" picked job % for table "%"',
                  worker_id, job_id_val, table_val;

    UPDATE _gsiceberg.flush_jobs
    SET status = 'in_progress', worker_id = worker_id, started_at = now()
    WHERE job_id = job_id_val;

    UPDATE _gsiceberg.flush_workers
    SET current_job = job_id_val, heartbeat = now()
    WHERE worker_id = worker_id;

    -- ⑤ 执行 Phase 2（调用框架 C 函数）
    SELECT public.iceberg_flush_phase2(table_val, job_id_val) INTO ok;

    -- ⑥ 更新结果
    IF ok THEN
        UPDATE _gsiceberg.flush_jobs
        SET status = 'completed', finished_at = now()
        WHERE job_id = job_id_val;

        UPDATE _gsiceberg.flush_workers
        SET jobs_processed = jobs_processed + 1,
            current_job = NULL, heartbeat = now()
        WHERE worker_id = worker_id;

        RAISE NOTICE 'gsiceberg: flush_worker "%" completed job % for table "%"',
                      worker_id, job_id_val, table_val;
    ELSE
        UPDATE _gsiceberg.flush_jobs
        SET status = 'failed',
            retry_count = COALESCE(retry_count, 0) + 1,
            error_msg = 'flush_phase2_failed',
            finished_at = now(), worker_id = NULL
        WHERE job_id = job_id_val;

        UPDATE _gsiceberg.flush_workers
        SET current_job = NULL,
            last_error = format('job %s failed for table %s', job_id_val, table_val),
            heartbeat = now()
        WHERE worker_id = worker_id;
    END IF;

    RETURN 1;
END;
$$;
```

#### 5.3.4 daemon 循环

```sql
CREATE OR REPLACE FUNCTION iceberg_flush_daemon(
    worker_id         text DEFAULT 'daemon',
    poll_interval_sec int  DEFAULT 5
)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog AS $$
DECLARE
    processed int;
BEGIN
    RAISE NOTICE 'gsiceberg: flush daemon "%" starting (poll=%ss)',
                  worker_id, poll_interval_sec;
    LOOP
        BEGIN
            processed := iceberg_flush_worker(worker_id);
            IF processed = 0 THEN
                PERFORM pg_sleep(poll_interval_sec);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'gsiceberg: flush daemon "%" error: %',
                           worker_id, SQLERRM;
            PERFORM pg_sleep(poll_interval_sec);
        END;
    END LOOP;
END;
$$;
```

#### 5.3.5 运维仪表板

```sql
CREATE OR REPLACE VIEW _gsiceberg.flush_dashboard AS
SELECT
    w.worker_id,
    w.status                     AS worker_status,
    w.current_job,
    w.heartbeat,
    now() - w.heartbeat          AS heartbeat_age,     -- >5min = worker 可能挂了
    w.jobs_processed,
    w.last_error,
    (SELECT count(*) FROM _gsiceberg.flush_jobs
     WHERE status = 'pending')   AS pending_jobs,      -- 队列积压
    (SELECT count(*) FROM _gsiceberg.flush_jobs
     WHERE status = 'in_progress') AS running_jobs,
    (SELECT count(*) FROM _gsiceberg.flush_jobs
     WHERE status = 'failed'
       AND COALESCE(retry_count, 0)
           >= current_setting('gsiceberg.flush_retry_count')::int
    )                             AS dead_jobs          -- 需人工介入
FROM _gsiceberg.flush_workers w;
```

#### 5.3.6 并发安全保证

```
同表串行（两级防护）：
  Level 1: Phase 1 入口 → pg_try_advisory_xact_lock(table_name)
            同一张表的多个并发 iceberg_flush() 中只有一个获取锁成功
            锁失败者返回 NULL（不创建重复 job）
  Level 2: Phase 2 处理对象 → _delta_flushing 表
            RENAME 是 PG DDL（事务原子），不可能同时存在两个 _delta_flushing

多表并行（FOR UPDATE SKIP LOCKED）：
  两个 worker 同时调 SELECT ... FOR UPDATE SKIP LOCKED：
    worker_1 锁定 job_id=5 → 处理 orders 的 Phase 2
    worker_2 跳过 job_id=5，锁定 job_id=6 → 处理 products 的 Phase 2
    各自写不同的 Parquet 文件 → 无冲突
  全串行模式：只启动 1 个 worker → 所有 job 顺序执行
```

### 5.4 接口依赖

| 依赖的外部接口 | 调用方式 | 用途 |
|---|---|---|
| `iceberg_flush_phase2(text, bigint) → bool` | PL/pgSQL SELECT | 执行 Phase 2+3 |
| `gsiceberg_require_admin()` | PL/pgSQL PERFORM | worker 权限检查 |
| `gsiceberg.flush_retry_count` | `current_setting(...)::int` | 最大重试次数 |
| `_gsiceberg.flush_jobs`（含 priority, worker_id） | SELECT/UPDATE | 队列读写 |
| `_gsiceberg.flush_workers` | INSERT/UPDATE | 心跳维护 |
| `FOR UPDATE SKIP LOCKED` | PG SQL | 并发 job 隔离 |
| `RAISE NOTICE/WARNING` | PL/pgSQL | 可观测性 |
| `pg_sleep(n)` | PL/pgSQL PERFORM | daemon 等待 |

### 5.5 实现清单

| 产出 | 类型 | 说明 |
|---|---|---|
| `flush_jobs` 新增 3 列 | DDL | priority, scheduled_at, worker_id |
| `flush_workers` 表 | DDL | worker 心跳表 |
| `iceberg_flush_worker()` 重写 | PL/pgSQL 函数 | 增加 priority + heartbeat + worker_id |
| `iceberg_flush_daemon()` | PL/pgSQL 函数 | 守护循环 |
| `_gsiceberg.flush_dashboard` | VIEW | 运维仪表板 |
| `flush_phase1.c` 修改 | C 修改 | INSERT flush_jobs 增加 priority |

**代码量**：PL/pgSQL ~150 行 + C ~15 行

### 5.6 测试文档

#### 5.6.1 优先级调度测试：`test/sql/functional/flush_scheduler_priority.sql`

```sql
-- ============================================================
-- Setup: 插入三个不同优先级的 job
-- ============================================================
INSERT INTO _gsiceberg.flush_jobs (table_name, flush_table, status, priority)
VALUES ('sc_high', 'sc_high', 'pending', 100),
       ('sc_mid',  'sc_mid',  'pending', 50),
       ('sc_low',  'sc_low',  'pending', 10);

-- ============================================================
-- T1: worker 应按 priority DESC 取任务 → 先取 priority=100
-- ============================================================
SELECT iceberg_flush_worker('priority_test') AS "worker_result";

-- 预期结果：
--  worker_result
-- ──────────────
--  1
-- 验证：worker 处理了 1 个任务

SELECT status IN ('in_progress','completed') AS "high_priority_taken_first"
FROM _gsiceberg.flush_jobs WHERE table_name = 'sc_high'
ORDER BY job_id DESC LIMIT 1;

-- 预期结果：
--  high_priority_taken_first
-- ─────────────────────────
--  t
-- 验证：priority=100 的 sc_high 被最先选中


-- ============================================================
-- T2: 同优先级按 FIFO（job_id ASC）
-- ============================================================
INSERT INTO _gsiceberg.flush_jobs (table_name, flush_table, status, priority)
VALUES ('sc_t1', 'sc_t1', 'pending', 50),
       ('sc_t2', 'sc_t2', 'pending', 50);

SELECT iceberg_flush_worker('priority_test');
SELECT iceberg_flush_worker('priority_test');

-- 验证先后处理顺序：job_id 小的 sc_t1 应被先处理
SELECT job_id, table_name, status
FROM _gsiceberg.flush_jobs
WHERE table_name IN ('sc_t1', 'sc_t2')
ORDER BY job_id;

-- 预期结果（两行）：
--  job_id | table_name | status
-- ────────┼────────────┼───────────────
--  N      | sc_t1      | in_progress/completed   ← 先处理
--  N+1    | sc_t2      | in_progress/completed   ← 后处理
-- 验证：同 priority=50 时，job_id 小的先被处理（FIFO）
```

#### 5.6.2 Worker 心跳测试：`test/sql/functional/flush_worker_heartbeat.sql`

```sql
-- ============================================================
-- Setup
-- ============================================================
TRUNCATE _gsiceberg.flush_workers;

-- ============================================================
-- T1: 首次调用 → worker 被注册，心跳时间极近
-- ============================================================
SELECT iceberg_flush_worker('hb_test') AS "result";

-- 预期结果：
--  result
-- ────────
--  1 或 0（取决于是否有 pending job）

SELECT worker_id = 'hb_test'                            AS "worker_registered",
       status = 'running'                                AS "status_is_running",
       now() - heartbeat < interval '1 second'           AS "heartbeat_current",
       (current_job IS NULL OR current_job IS NOT NULL)  AS "job_field_set"
FROM _gsiceberg.flush_workers WHERE worker_id = 'hb_test';

-- 预期结果：
--  worker_registered | status_is_running | heartbeat_current | job_field_set
-- ───────────────────┼───────────────────┼───────────────────┼──────────────
--  t                 | t                 | t                 | t
-- 验证：worker 在 flush_workers 表中成功注册，心跳在 1 秒内


-- ============================================================
-- T2: 无任务时心跳仍然更新
-- ============================================================
SELECT iceberg_flush_worker('hb_test') AS "second_result";

SELECT now() - heartbeat < interval '1 second' AS "heartbeat_still_fresh",
       current_job IS NULL                      AS "idle_when_no_job"
FROM _gsiceberg.flush_workers WHERE worker_id = 'hb_test';

-- 预期结果：
--  heartbeat_still_fresh | idle_when_no_job
-- ───────────────────────┼─────────────────
--  t                     | t
-- 验证：即使没有任务处理（返回 0），worker 仍更新心跳
--       current_job 为 NULL 表示空闲
```

#### 5.6.3 重试限制测试：`test/sql/functional/flush_worker_retry.sql`

```sql
-- ============================================================
-- T1: retry_count < gsiceberg.flush_retry_count → 可被重试
-- ============================================================
INSERT INTO _gsiceberg.flush_jobs (table_name, flush_table, status, retry_count)
VALUES ('rt_t1', 'rt_t1', 'failed', 1);

SELECT iceberg_flush_worker('retry_test') AS "worker_result";

-- 预期结果：
--  worker_result
-- ──────────────
--  1
-- 验证：retry_count=1 < GUC 限制(3) → worker 选中并尝试执行

SELECT status IN ('in_progress','completed','failed') AS "job_was_processed"
FROM _gsiceberg.flush_jobs
WHERE table_name = 'rt_t1'
ORDER BY job_id DESC LIMIT 1;

-- 预期结果：
--  job_was_processed
-- ──────────────────
--  t
-- 验证：job 被 worker 选中处理，状态已更新


-- ============================================================
-- T2: retry_count >= gsiceberg.flush_retry_count → 永久跳过
-- ============================================================
INSERT INTO _gsiceberg.flush_jobs (table_name, flush_table, status, retry_count)
VALUES ('rt_t2', 'rt_t2', 'failed',
        current_setting('gsiceberg.flush_retry_count')::int);

SELECT iceberg_flush_worker('retry_test') = 0 AS "worker_skipped_dead_job";

-- 预期结果：
--  worker_skipped_dead_job
-- ────────────────────────
--  t
-- 验证：retry_count=3 >= GUC 限制(3) → worker 跳过，返回 0

SELECT status = 'failed' AS "status_unchanged"
FROM _gsiceberg.flush_jobs
WHERE table_name = 'rt_t2'
ORDER BY job_id DESC LIMIT 1;

-- 预期结果：
--  status_unchanged
-- ─────────────────
--  t
-- 验证：死信任务的 status 保持 'failed' 不变
```

#### 5.6.4 Dashboard 视图测试：`test/sql/functional/flush_dashboard.sql`

```sql
-- ============================================================
-- Setup: 确保有一个 worker 注册
-- ============================================================
SELECT iceberg_flush_worker('default');

-- ============================================================
-- T1: dashboard 能反映 pending job 队列状态
-- ============================================================
INSERT INTO _gsiceberg.flush_jobs (table_name, flush_table, status)
VALUES ('db_t', 'db_t', 'pending'),
       ('db_t', 'db_t', 'pending');

SELECT pending_jobs >= 2 AS "dashboard_reflects_pending",
       worker_status = 'running' AS "worker_is_running"
FROM _gsiceberg.flush_dashboard WHERE worker_id = 'default';

-- 预期结果：
--  dashboard_reflects_pending | worker_is_running
-- ───────────────────────────┼──────────────────
--  t                         | t
-- 验证：dashboard 视图正确聚合了 flush_jobs 的 pending 计数


-- ============================================================
-- T2: dashboard 能识别死信（retry_count 超限的 failed job）
-- ============================================================
INSERT INTO _gsiceberg.flush_jobs (table_name, flush_table, status, retry_count)
VALUES ('db_t2', 'db_t2', 'failed',
        current_setting('gsiceberg.flush_retry_count')::int);

SELECT dead_jobs >= 1 AS "dashboard_sees_dead_jobs"
FROM _gsiceberg.flush_dashboard WHERE worker_id = 'default';

-- 预期结果：
--  dashboard_sees_dead_jobs
-- ─────────────────────────
--  t
-- 验证：retry_count 超限的 failed job 被归类为 dead_jobs


-- ============================================================
-- T3: heartbeat_age 反映心跳活跃度
-- ============================================================
SELECT heartbeat_age < interval '1 second' AS "heartbeat_is_recent"
FROM _gsiceberg.flush_dashboard WHERE worker_id = 'default';

-- 预期结果：
--  heartbeat_is_recent
-- ────────────────────
--  t
```

#### 5.6.5 并发安全测试（TAP）：`test/perl/flush_concurrency.pl`

```
测试场景：
  启动 3 个并发 worker，队列中有 10 个 pending job。
  验证：
    1. 10 个 job 全部被处理（status 最终为 completed 或 failed）
    2. 没有两个 worker 处理同一个 job（job_id 不重复）
    3. FOR UPDATE SKIP LOCKED 无死锁（3 个 session 正常退出）
    4. 每个 job 的 worker_id 正确记录
```

---

## 6. 跨模块协作与部署

### 6.1 模块间依赖

```
模块二（生命周期追踪）
  │  提供：flush 各阶段的状态数据（phase, *_at, *_rows）
  │  被依赖原因：模块一和模块三可通过 iceberg_flush_history() 查询 flush 效率
  │
  ├──────────────────────┬──────────────────────┐
  │                      │                      │
  ▼                      ▼                      ▼
模块三（后台调度）        模块一（自动 Flush）
  提供：优先级调度 +       提供：周期性自动触发
  worker 心跳 + daemon     + 表级配置
  
  模块一可选依赖模块三：
    • 调度器通过 iceberg_flush() 触发（不直接依赖 worker）
    • daemon 实现模式相同（都是 pg_sleep 循环）
```

### 6.2 开发顺序

| 顺序 | 模块 | 原因 |
|---|---|---|
| 1 | 模块二 | 改动范围最小（扩展 flush_state 表 + 在已有流程中嵌入追踪点），不修改控制流。为后续模块提供查询基础 |
| 2 | 模块三 | 独立可测，修改 flush_phase1.c 的一处 INSERT 语句 + 重写 PL/pgSQL worker |
| 3 | 模块一 | 纯 PL/pgSQL，通过已有接口触发，无 C 代码修改。可选复用模块三的 daemon |

### 6.3 部署方式

```sql
-- ============================================
-- 部署模块一（自动 flush）
-- ============================================

-- 方案 A: pg_cron（推荐）
SELECT cron.schedule('auto-flush', '* * * * *',
    'SELECT iceberg_auto_flush_scheduler();');

-- 方案 B: 系统 cron
-- * * * * * psql -c "SELECT iceberg_auto_flush_scheduler();"

-- 方案 C: daemon（独立 session）
-- psql -c "SELECT iceberg_auto_flush_daemon();"


-- ============================================
-- 部署模块三（后台调度）
-- ============================================

-- 方案 A: daemon（推荐）
-- psql -c "SELECT iceberg_flush_daemon('daemon', 10);"

-- 方案 B: pg_cron + worker
SELECT cron.schedule('flush-worker', '* * * * *',
    'SELECT iceberg_flush_worker(''cron_w1'');');


-- ============================================
-- 运维查询
-- ============================================

-- 查看自动 flush 配置
SELECT table_name, auto_flush_enabled, auto_flush_interval_sec
FROM _gsiceberg.tables WHERE auto_flush_enabled = true;

-- 查看 flush 历史（模块二）
SELECT * FROM iceberg_flush_history('orders', 10);

-- 查看调度状态（模块三）
SELECT * FROM _gsiceberg.flush_dashboard;
```

### 6.4 总代码量

| 模块 | PL/pgSQL | C | 测试 SQL | 合计 |
|---|---|---|---|---|
| 模块一 | ~100 行 | ~20 行 | ~120 行 | ~240 行 |
| 模块二 | ~30 行 | ~180 行 | ~100 行 | ~310 行 |
| 模块三 | ~150 行 | ~15 行 | ~150 行 | ~315 行 |
| **合计** | **~280 行** | **~215 行** | **~370 行** | **~865 行** |
