# Flush 三大功能模块完整设计文档

> **定位**：本文档描述 gsiceberg flush 子系统的三个功能模块的完整设计与实现。
> **最后更新**：2026-07-21（适配当前代码架构）
> **实现文件**：
> - SQL 层：`gsiceberg/sql/02f-auto-flush.sql`、`gsiceberg/sql/02b-ddl.sql`
> - C 层：`gsiceberg/fdw/bgworker/auto_flush_worker.c`、`gsiceberg/fdw/hooks/fdw_hooks.c`
> **测试脚本**：`flush_devlop/sql/test_all_modules.sql`
> **前提**：`shared_preload_libraries = 'gsiceberg'` in postgresql.conf

---

## 目录

- [1. 外部依赖接口](#1-外部依赖接口)
- [2. 元数据表设计](#2-元数据表设计)
- [3. 模块一：定时自动 Flush](#3-模块一定时自动-flush)
- [4. 模块二：Flush 生命周期追踪](#4-模块二flush-生命周期追踪)
- [5. 模块三：后台 Flush 作业调度](#5-模块三后台-flush-作业调度)
- [6. 部署](#6-部署)
- [7. 测试用例](#7-测试用例)

---

## 1. 外部依赖接口

### 1.1 异步 flush 入口 (Phase 1)

```
接口名：iceberg_flush
类型：PG C 函数
SQL 签名：iceberg_flush(table_name text) → bigint
```

**行为契约**：
1. 获取表级 advisory lock（`pg_try_advisory_xact_lock`），锁冲突返回 0
2. 权限检查（table owner 或 gsiceberg_admin）
3. **仅执行 Phase 1**：冻结 delta + 注册 job → 返回 pending job_id
4. Phase 2 由 C bgworker 自动消费
5. 返回值：>0 = pending job_id，NULL = delta 为空

### 1.2 Worker 消费入口 (Phase 2)

```
接口名：iceberg_flush_worker
类型：PL/pgSQL 函数（STRICT）
SQL 签名：iceberg_flush_worker(table_name text) → int
```

**注意**：STRICT 修饰符，NULL 参数直接返回 NULL。传空字符串 `''` 绕过。

**行为契约**：
1. 扫描 `flush_jobs WHERE status IN ('pending','failed') FOR UPDATE SKIP LOCKED`
2. 执行 Stage Foreign → Stage Flush → Stage Train → Stage Cleanup
3. Drain 循环：一次调用处理所有 pending jobs

---

## 2. 元数据表设计

### 2.1 `_gsiceberg.tables`（已实现列）

```sql
-- [M1] 新增
auto_flush_enabled      boolean NOT NULL DEFAULT false,
auto_flush_interval_sec  int    NOT NULL DEFAULT 300,      -- 0=每次INSERT触发
auto_flush_mode          text   NOT NULL DEFAULT 'async',  -- async|sync
auto_flush_min_rows      int    NOT NULL DEFAULT 1,        -- 最小触发行数
last_flush_at            timestamptz                       -- 上次完成时间
```

### 2.2 `_gsiceberg.flush_jobs`

```sql
job_id        bigserial PRIMARY KEY,
table_name    text NOT NULL,
namespace     text NOT NULL DEFAULT 'default',
status        text NOT NULL DEFAULT 'pending',  -- pending|in_progress|completed|failed
started_at    timestamptz DEFAULT now(),
finished_at   timestamptz,
error_msg     text
```

### 2.3 `_gsiceberg.flush_state`（单行/表，UPDATE 覆盖）

```sql
table_name     text NOT NULL,
flush_status   text NOT NULL DEFAULT 'idle',    -- idle|in_progress|error
stage          text,                             -- foreign|flush|train|cleanup
stage_seq      int DEFAULT 0,
started_at     timestamptz DEFAULT now(),
finished_at    timestamptz,
PRIMARY KEY (namespace, table_name)
```

---

## 3. 模块一：定时自动 Flush

> 实现文件：`gsiceberg/sql/02f-auto-flush.sql`（核心 SQL）、`gsiceberg/fdw/bgworker/auto_flush_worker.c`（C bgworker 定时检查）

### 3.1 核心思路

定时由 **C bgworker 每 1 秒轮询** `iceberg_auto_flush_scheduler()` 驱动。
INSERT 触发器只做 Phase 1（冻结 delta），AFTER STATEMENT 触发器 + bgworker 消费 Phase 2。

### 3.2 计时机制

计时器存储在 `_gsiceberg.tables.last_flush_at`，三种来源：

```
last_flush_at 被更新:
  ├─ UPDATE SET auto_flush_enabled=true    → 设为 now()（trg: auto_flush_on_enable）
  ├─ UPDATE SET auto_flush_interval_sec=X  → 设为 now()（同上触发器）
  └─ flush_jobs.status='completed'        → trg_last_flush_at → 设为 finished_at
```

### 3.3 完整调用链路

```
═══════════════════════════════════════════════════════════════
触发路径 1 — INSERT（异步，interval>0 时毫秒级返回）
═══════════════════════════════════════════════════════════════
INSERT INTO af_test VALUES (...)
  │
  ├─ INSTEAD OF INSERT 触发器 (ROW LEVEL)
  │   ├─ INSERT INTO _gsiceberg._<stem>_delta  ← 写入 delta
  │   └─ PERFORM _gsiceberg.iceberg_auto_flush_trigger_check()
  │       │  [02f-auto-flush.sql:23]
  │       ├─ interval=0? → 每次 INSERT 都触发 Phase 1
  │       ├─ interval>0? → elapsed >= interval 才触发
  │       │   elapsed = now() - MAX(finished_at) WHERE status='completed'
  │       │   找不到 → 回退到 tables.created_at
  │       ├─ 有 pending/in_progress job? → 跳过（避免重复）
  │       ├─ delta 行数 < min_rows? → 跳过
  │       └─ 条件满足:
  │           async → PERFORM iceberg_flush(table)  ← 只做 Phase 1 freeze
  │           sync  → PERFORM iceberg_flush_sync(table)  ← Phase 1+2 一步
  │
  └─ AFTER INSERT STATEMENT 触发器
      └─ PERFORM iceberg_flush_worker('')  ← 消费 Phase 2

═══════════════════════════════════════════════════════════════
触发路径 2 — C bgworker 轮询（每 1 秒，Phase 1+2 分离）
═══════════════════════════════════════════════════════════════
bgworker: for(;;) {  [auto_flush_worker.c:22]
    SPI_execute("SELECT public.iceberg_auto_flush_scheduler()")
    SPI_execute("SELECT public.iceberg_flush_worker('')")
    pg_usleep(1s)
}

iceberg_auto_flush_scheduler()  [02f-auto-flush.sql:193]
  FOR 每张 auto_flush_enabled=true AND interval_sec > 0 的表:
    ① last_ts = tables.last_flush_at
    ② elapsed = now() - last_ts (fallback: created_at)
    ③ IF elapsed >= interval_sec:
         无 pending/in_progress job?
         delta 行数 >= min_rows?
         → PERFORM iceberg_flush(table)   ← Phase 1 freeze → job_id
    ④ RETURN NEXT (返回触发记录给调用方)

═══════════════════════════════════════════════════════════════
触发路径 3 — UPDATE 触发器（false→true 时立即 flush）
═══════════════════════════════════════════════════════════════
UPDATE _gsiceberg.tables SET auto_flush_enabled=true
  → auto_flush_on_enable_trg  [02f-auto-flush.sql:176]
    → iceberg_auto_flush_on_enable()  [02f-auto-flush.sql:102]
      ├─ false→true: 立即 Phase 1 + Phase 2 (不在 INSERT 上下文中，cleanup 安全)
      ├─ 已开启+改 interval: 只重置 last_flush_at=now()，不 flush
      └─ 关闭: 不处理
```

### 3.4 触发方式对比

| 触发方式 | 谁触发 | Phase 1 | Phase 2 | 适用场景 |
|---------|--------|---------|---------|---------|
| INSERT 触发器 | 每次 INSERT 检查条件 | async=freeze, sync=一步 | AFTER STATEMENT | interval=0 每次触发 |
| C bgworker | 每 1 秒轮询 | scheduler→freeze | worker→消费 | interval>0 周期触发 |
| UPDATE 触发器 | auto_flush_enabled 变化 | freeze+worker | 同步 | 首次开启 |

### 3.5 函数与触发器清单

| 名称 | 类型 | 文件:行 | 作用 |
|------|------|--------|------|
| `iceberg_auto_flush_trigger_check` | 函数 | 02f:23 | INSERT 行级：检查条件→Phase 1 |
| `iceberg_auto_flush_after_stmt` | 函数 | 02f:88 | AFTER STATEMENT：消费 Phase 2 |
| `iceberg_auto_flush_on_enable` | 函数 | 02f:102 | UPDATE 触发器：启用时立即 flush |
| `iceberg_auto_flush_scheduler` | 函数 | 02f:193 | bgworker 调用：扫描+触发 Phase 1 |
| `iceberg_auto_flush_next_sec` | 函数 | 02f:277 | 距下次 flush 秒数 |
| `_update_last_flush_at` | 函数 | 02f:161 | flush_jobs→completed 时同步 last_flush_at |
| `auto_flush_on_enable_trg` | 触发器 | 02f:176 | tables 上：AFTER UPDATE OF enabled/interval |
| `trg_last_flush_at` | 触发器 | 02f:169 | flush_jobs 上：job 完成时更新计时器 |
| `<table>_ins_trg` | 触发器 | 02b:185 | 每表：INSTEAD OF INSERT → trigger_check |
| `<table>_auto_ast` | 触发器 | 02b:208 | 每表：AFTER INSERT STATEMENT → worker |

### 3.6 时间线示例（interval=5s）

```
t=0s   UPDATE SET auto_flush_enabled=true, interval=5
       → last_flush_at = now()  (计时器归零)

t=1s   INSERT → delta=1行
       bgworker: elapsed=1s < 5s → scheduler 不触发

t=3s   INSERT → delta=2行
       bgworker: elapsed=3s < 5s → 不触发

t=6s   INSERT → delta=3行（或仅 bgworker 检查）
       bgworker: elapsed=6s >= 5s → scheduler 触发!
         → iceberg_flush() → freeze 3行 → job_id=X
         → iceberg_flush_worker('') → Phase 2 → completed
         → trg_last_flush_at → last_flush_at = now()

t=7s   INSERT → delta=1行
       bgworker: elapsed=1s < 5s → 不触发
       ...循环...
```

### 3.7 变量清单

| 变量 | 类型 | 所在函数 | 含义 |
|------|------|---------|------|
| `rec` | RECORD | trigger_check, scheduler | 当前表的 auto_flush 配置 |
| `elapsed` | int | trigger_check, scheduler | now() - last_flush_at（秒） |
| `last_ts` | timestamptz | trigger_check, scheduler | 上次 flush 完成时间 |
| `delta_n` | bigint | trigger_check, scheduler, on_enable | delta 表当前行数 |
| `obj_stem` | text | trigger_check, scheduler, on_enable | namespace_table |
| `delta_rel` | text | trigger_check, scheduler, on_enable | `_gsiceberg._<stem>_delta` |
| `jid` | bigint | trigger_check, on_enable | iceberg_flush() 返回的 job_id |
| `was_enabled` | bool | on_enable | OLD.auto_flush_enabled |
| `interval_changed` | bool | on_enable | interval 是否变化 |

---

## 4. 模块二：Flush 生命周期追踪

> 实现文件：
> - C 层：`fdw/flush/flush_state.c`（状态机核心）、`fdw/flush/flush_internal.h`（接口声明）
> - SQL 层：`sql/flush_state.sql`（PL/pgSQL 包装）、`sql/02f-auto-flush.sql:302`（history 函数）、`sql/01-schema.sql`（表结构）
> - 各 Stage 文件：`flush_stage_foreign.c`、`flush_stage_flush.c`、`flush_stage_train.c`、`flush_stage_cleanup.c`

### 4.1 两张追踪表

**`_gsiceberg.flush_jobs`** — job 历史（每次 flush 一行，永久保留）

```sql
job_id        bigserial PRIMARY KEY,
table_name    text NOT NULL,
status        text NOT NULL DEFAULT 'pending',  -- pending|in_progress|completed|failed
started_at    timestamptz DEFAULT now(),
finished_at   timestamptz,
error_msg     text
```

**`_gsiceberg.flush_state`** — 阶段状态（每表一行，UPDATE 覆盖）

```sql
table_name     text NOT NULL,
flush_status   text NOT NULL DEFAULT 'idle',    -- idle|in_progress|error
stage          text,                             -- foreign|flush|train|cleanup
stage_seq      int DEFAULT 0,
stage_detail   jsonb,
started_at     timestamptz DEFAULT now(),
finished_at    timestamptz,
PRIMARY KEY (namespace, table_name)
```

### 4.2 完整调用链路

```
═══════════════════════════════════════════════════════════════
Phase 1 — freeze（不写 flush_state）
═══════════════════════════════════════════════════════════════
iceberg_flush()                              [fdw/flush/flush_phase2.c:17]
  → iceberg_flush_stage_freeze()             [fdw/flush/flush_stage_freeze.c]
      → RENAME _delta → _delta_flushing
      → INSERT INTO flush_jobs (status='pending')   ← 仅写 flush_jobs
      → RETURN job_id
  → flush_state: (无变化，Phase 1 不写 flush_state)

═══════════════════════════════════════════════════════════════
Phase 2 — worker 消费各阶段
═══════════════════════════════════════════════════════════════
iceberg_flush_worker('')                     [sql/02a-lifecycle.sql:159]
  → UPDATE flush_jobs SET status='in_progress'

  Stage Foreign ─────────────────────────────────────────────
  → iceberg_flush_stage_foreign()           [fdw/flush/flush_stage_foreign.c:12]
      → flush_state_enter_stage("foreign")  [flush_internal.h:45 → flush_state.c]
          → INSERT/UPDATE flush_state: stage='foreign', flush_status='in_progress'
      → 处理外部导入文件（无则跳过）
      → flush_state_complete_stage("train") [flush_state.c]
          → UPDATE flush_state: stage='train'

  Stage Flush ───────────────────────────────────────────────
  → iceberg_flush_stage_flush()             [fdw/flush/flush_stage_flush.c:16]
      → flush_state_enter_stage("flush")    [flush_state.c]
          → UPDATE flush_state: stage='flush'
      → flush_state_recover()              [flush_state.c] — 崩溃恢复检查
      → 读 _delta_flushing → CoW 重写 → 写 Parquet
      → flush_state_set_file()             [flush_state.c] — 记录文件路径+行数
      → flush_state_set_snapshot()         [flush_state.c] — 记录快照 ID
      → flush_state_complete_stage("train") [flush_state.c]
          → UPDATE flush_state: stage='train'

  Stage Train ───────────────────────────────────────────────
  → iceberg_flush_stage_train()             [fdw/flush/flush_stage_train.c:18]
      → flush_state_enter_stage("train", seq) [flush_state.c]
          → UPDATE flush_state: stage='train', stage_seq=N
      → micro-stage 循环：训练 L0 索引
      → 无索引时：flush_state_complete_stage("cleanup")
          → UPDATE flush_state: stage='cleanup'

  Stage Cleanup ─────────────────────────────────────────────
  → iceberg_flush_stage_cleanup()           [fdw/flush/flush_stage_cleanup.c:8]
      → flush_state_enter_stage("cleanup")  [flush_state.c]
          → UPDATE flush_state: stage='cleanup'
      → DROP _delta_flushing + _foreign_delta_flushing
      → iceberg_build_object()              [sql/02b-ddl.sql:44]
          → 重建 public VIEW + 所有触发器
      → UPDATE flush_jobs SET status='completed', finished_at=now()
        → trg_last_flush_at                 [02f-auto-flush.sql:169]
            → UPDATE tables.last_flush_at = finished_at
      → flush_state_done()                  [flush_state.c]
          → UPDATE flush_state: flush_status='idle', finished_at=now()
```

### 4.3 阶段变化汇总

| 步骤 | 调用函数 | 文件:行 | `flush_status` | `stage` |
|------|---------|--------|---------------|---------|
| Phase 1 freeze | `iceberg_flush_stage_freeze` | `flush_stage_freeze.c` | (空) | (空) |
| Stage Foreign | `flush_state_enter_stage("foreign")` | `flush_stage_foreign.c:23` | `in_progress` | `foreign`→`train` |
| Stage Flush | `flush_state_enter_stage("flush")` | `flush_stage_flush.c:35` | `in_progress` | `flush`→`train` |
| Stage Train | `flush_state_enter_stage("train")` | `flush_stage_train.c:42` | `in_progress` | `train`→`cleanup` |
| Stage Cleanup | `flush_state_enter_stage("cleanup")` | `flush_stage_cleanup.c:18` | `in_progress` |
| 完成 | `flush_state_done` | `flush_state.c` | `idle` | `cleanup` |

### 4.4 涉及文件明细

| 文件 | 函数 | 作用 |
|------|------|------|
| `fdw/flush/flush_state.c` | `flush_state_enter_stage` | 写入当前阶段 |
| | `flush_state_complete_stage` | 推进到下一阶段 |
| | `flush_state_done` | 标记 flush 完成（idle） |
| | `flush_state_begin` | 旧接口：开始 flush |
| | `flush_state_set_file` | 记录 Parquet 文件路径+行数 |
| | `flush_state_set_snapshot` | 记录快照 ID |
| | `flush_state_recover` | 崩溃恢复：检查可恢复文件 |
| `fdw/flush/flush_internal.h` | 所有 flush_state_* 声明 | C 接口头文件 |
| `sql/flush_state.sql` | `_gsiceberg.flush_state_*` | PL/pgSQL 包装（SQL 层调用） |
| `sql/02f-auto-flush.sql:302` | `iceberg_flush_history` | JOIN flush_jobs+flush_state 统一查询 |
| `sql/02a-lifecycle.sql:159` | `iceberg_flush_worker` | Phase 2 消费入口 |
| `fdw/flush/flush_stage_foreign.c` | `iceberg_flush_stage_foreign` | Stage Foreign |
| `fdw/flush/flush_stage_flush.c` | `iceberg_flush_stage_flush` | Stage Flush（写 Parquet） |
| `fdw/flush/flush_stage_train.c` | `iceberg_flush_stage_train` | Stage Train（索引训练） |
| `fdw/flush/flush_stage_cleanup.c` | `iceberg_flush_stage_cleanup` | Stage Cleanup（清理+重建 VIEW） |
| `fdw/flush/flush_phase2.c` | `iceberg_flush` | Phase 1 入口 |
| `sql/02b-ddl.sql:44` | `iceberg_build_object` | 重建 VIEW + 触发器 |

### 4.5 查询接口

```sql
-- 当前阶段状态（每表一行）
SELECT table_name, flush_status, stage, stage_seq, started_at, finished_at
FROM _gsiceberg.flush_state WHERE table_name='af_test';

-- job 历史（每次 flush 一行，永久保留）
SELECT job_id, status, started_at, finished_at, error_msg
FROM _gsiceberg.flush_jobs WHERE table_name='af_test' ORDER BY job_id DESC;

-- 统一查询（JOIN 两表）
SELECT job_id, table_name, job_status, stage, stage_seq, job_created, job_finished, error_msg
FROM iceberg_flush_history('af_test');
```

### 4.6 分步阶段观察（手动调用各 Stage）

```sql
-- 可手动调用各 stage 函数观察 flush_state 变化
INSERT INTO af_test VALUES (1, 1.0, 'walk');
SELECT iceberg_flush('af_test') AS job_id;              -- Phase 1
-- flush_state: (空)

SELECT iceberg_flush_stage_foreign('af_test', <jid>);   -- Stage Foreign
-- flush_state: in_progress | train

SELECT iceberg_flush_stage_flush('af_test', <jid>);     -- Stage Flush
-- flush_state: in_progress | train

SELECT iceberg_flush_stage_train('af_test', <jid>);     -- Stage Train
-- flush_state: in_progress | cleanup

SELECT iceberg_flush_stage_cleanup('af_test', <jid>);   -- Stage Cleanup
-- flush_state: idle | cleanup
```

---

## 5. 模块三：后台 Flush 作业调度

> 实现文件：
> - `gsiceberg/fdw/bgworker/auto_flush_worker.c` — C bgworker 主循环
> - `gsiceberg/fdw/hooks/fdw_hooks.c` — `_PG_init()` 注册 bgworker
> - `gsiceberg/Makefile` — 编译链接

### 5.1 设计思路

`iceberg_flush()` 完成 Phase 1（冻结 delta）后立即返回 job_id 给用户。
**C background worker** 自动轮询 `flush_jobs`，消费 pending/failed job 执行 Phase 2。

```
用户: SELECT iceberg_flush('af_test')
  → [flush_phase2.c] Phase 1: freeze delta → INSERT job(pending)
  → PG_RETURN_INT64(job_id)  ← 立即返回给用户

C bgworker (随 PG 启动，1s 轮询):
  for(;;) {
    SPI_execute("SELECT public.iceberg_flush_worker('')")
    pg_usleep(1s)
  }
  → 扫描 flush_jobs WHERE status IN ('pending','failed')
  → foreign → flush → train → cleanup → completed
```

### 5.2 bgworker 实现

**文件**：`fdw/bgworker/auto_flush_worker.c`

```c
PGDLLEXPORT void auto_flush_main(Datum arg) {
    BackgroundWorkerInitializeConnection(dbname, NULL, 0);
    for (;;) {
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());
        SPI_execute("SELECT public.iceberg_flush_worker('')", false, 0);
        PopActiveSnapshot();
        SPI_finish();
        CommitTransactionCommand();
        pg_usleep(interval * 1000000L);
    }
}
```

**关键点**：`SPI_connect()` 后必须 `PushActiveSnapshot(GetTransactionSnapshot())`，否则报 `cannot execute SQL without an outer snapshot or portal`。

### 5.3 注册

**文件**：`fdw/hooks/fdw_hooks.c`，在 `_PG_init()` 中：

```c
if (!IsUnderPostmaster) {
    BackgroundWorker worker;
    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;
    snprintf(worker.bgw_library_name,  BGW_MAXLEN, "gsiceberg");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "auto_flush_main");
    snprintf(worker.bgw_name,          BGW_MAXLEN, "gsiceberg auto_flush");
    snprintf(worker.bgw_type,          BGW_MAXLEN, "gsiceberg auto_flush");
    RegisterBackgroundWorker(&worker);
}
```

### 5.4 调用链路

```
═══════════════════════════════════════════════════════════════
Phase 1 — 用户/触发器调用（毫秒级，立即返回 job_id）
═══════════════════════════════════════════════════════════════
SELECT iceberg_flush('af_test')
  → [fdw/flush/flush_phase2.c:17] iceberg_flush()
    ├─ advisory lock
    ├─ 权限检查
    ├─ iceberg_flush_stage_freeze()
    │   → RENAME _delta → _delta_flushing
    │   → CREATE new _delta
    │   → INSERT INTO flush_jobs (status='pending')
    │   → RETURN job_id
    └─ PG_RETURN_INT64(job_id)  ← pending!

═══════════════════════════════════════════════════════════════
Phase 2 — C bgworker 自动消费（独立事务，后台异步）
═══════════════════════════════════════════════════════════════
bgworker: SELECT public.iceberg_flush_worker('')
  → [sql/02a-lifecycle.sql:159]
    扫描 flush_jobs WHERE status IN ('pending','failed')
     ORDER BY job_id LIMIT 1 FOR UPDATE SKIP LOCKED
    ├─ UPDATE status='in_progress'
    ├─ Stage Foreign → stage='foreign'
    ├─ Stage Flush   → 写 Parquet → stage='train'
    ├─ Stage Train   → stage='cleanup'
    └─ Stage Cleanup
        → DROP _delta_flushing
        → iceberg_build_object() → 重建 VIEW+触发器
        → UPDATE status='completed', finished_at=now()
        → trg_last_flush_at → UPDATE tables.last_flush_at
```

### 5.5 涉及文件明细

| 文件 | 函数/代码 | 作用 |
|------|----------|------|
| `fdw/flush/flush_phase2.c:17` | `iceberg_flush()` | Phase 1 入口：锁+权限+freeze→返回 job_id |
| `fdw/flush/flush_stage_freeze.c` | `iceberg_flush_stage_freeze()` | Phase 1: 冻结 delta |
| `fdw/bgworker/auto_flush_worker.c:22` | `auto_flush_main()` | bgworker 主循环：SPI 调用 worker |
| `fdw/hooks/fdw_hooks.c:1167` | `_PG_init()` | 注册 bgworker + GUC |
| `sql/02f-auto-flush.sql:193` | `iceberg_auto_flush_scheduler()` | 周期检查 auto_flush 表 |
| `sql/02a-lifecycle.sql:159` | `iceberg_flush_worker()` | Phase 2：消费 pending jobs |
| `sql/02b-ddl.sql:44` | `iceberg_build_object()` | 重建 VIEW + 触发器 |
| `Makefile:151` | `OBJS += fdw/bgworker/auto_flush_worker.o` | 编译链接 |

### 5.6 变量清单

#### C 层（bgworker）

| 变量 | 类型 | 文件 | 含义 |
|------|------|------|------|
| `gsiceberg_bgworker_interval` | int | auto_flush_worker.c | 轮询间隔（秒），GUC 可配 |
| `gsiceberg_bgworker_dbname` | char* | auto_flush_worker.c | 连接数据库名，GUC 可配 |
| `table_name` | const char* | flush_phase2.c | 用户传入的表名 |
| `job_id` | int64_t | flush_phase2.c | Phase 1 返回的 job_id |

#### PL/pgSQL 层（scheduler/daemon）

| 变量 | 类型 | 所在函数 | 含义 |
|------|------|---------|------|
| `rec` | RECORD | scheduler | 当前表配置行 |
| `elapsed` | int | scheduler | 距上次 flush 秒数 |
| `last_ts` | timestamptz | scheduler | `tables.last_flush_at` |
| `delta_n` | bigint | scheduler | delta 行数 |
| `jid` | bigint | scheduler | `iceberg_flush()` 返回值 |

### 5.7 GUC 配置

| GUC | 类型 | 默认值 | 含义 |
|-----|------|--------|------|
| `gsiceberg.bgworker_interval` | int | 1 | 轮询间隔（秒） |
| `gsiceberg.bgworker_dbname` | string | gv | 连接数据库 |

---

## 6. 部署

### 6.1 编译

```bash
cd gsiceberg
source scripts/setup-env.sh
cat sql/01-schema.sql sql/02f-auto-flush.sql ... > sql/gsiceberg--0.1.0.sql
make all && make install
```

### 6.2 配置 PG

```ini
# postgresql.conf
shared_preload_libraries = 'gsiceberg'
```

### 6.3 使用

```bash
# PG 启动后，bgworker 自动运行
pg_ctl -D /home/gv/pgdata start

# 创建数据库 + 安装扩展
psql -p 5432 -d postgres -c "CREATE DATABASE gv;"
psql -p 5432 -d gv -c "CREATE EXTENSION gsiceberg;"
```

```sql
-- 挂载表 + 配置
SELECT iceberg_mount('public', 'af_test', '/path/to/table');
UPDATE _gsiceberg.tables SET
    auto_flush_enabled      = true,
    auto_flush_interval_sec = 10
WHERE table_name = 'af_test';

-- 正常使用，bgworker 自动 flush
INSERT INTO af_test VALUES (1, 99.9, 'test');

-- Phase 1 立即返回 job_id
SELECT iceberg_flush('af_test');  -- 返回 job_id
-- bgworker 1s 内自动完成 Phase 2
```

```sql
-- 运维查询
SELECT * FROM iceberg_auto_flush_scheduler();
SELECT * FROM iceberg_flush_history('af_test');
SELECT pid, backend_type FROM pg_stat_activity WHERE backend_type LIKE '%gsiceberg%';
```

---

## 7. 测试用例

> 前置：PG 已启动 + 扩展已安装 + `shared_preload_libraries = 'gsiceberg'`

### 7.1 模块一：定时自动 Flush

```sql
-- ===== M1-1: 基本周期触发 (interval=5s) =====
DELETE FROM _gsiceberg.flush_jobs; DELETE FROM _gsiceberg.flush_state;
SELECT iceberg_mount('public', 'af_test', '/tmp/test_iceberg_c1/basic');

UPDATE _gsiceberg.tables SET
    auto_flush_enabled=true, auto_flush_interval_sec=5,
    auto_flush_min_rows=1, last_flush_at=now()
WHERE table_name='af_test';

INSERT INTO af_test VALUES (11, 99.9, 'test');
SELECT count(*)=0 AS "不到5s" FROM _gsiceberg.flush_jobs WHERE table_name='af_test';
-- 预期: t

SELECT pg_sleep(7);
INSERT INTO af_test VALUES (12, 50.5, 'test2');
SELECT pg_sleep(2);
SELECT count(*)>=1 AS "daemon触发" FROM _gsiceberg.flush_jobs WHERE table_name='af_test';
-- 预期: t

SELECT count(*)>=11 AS "数据可见" FROM af_test;
-- 预期: t

-- ===== M1-2: UPDATE 开启立即 flush =====
SELECT iceberg_unmount('af_test');
DELETE FROM _gsiceberg.flush_jobs; DELETE FROM _gsiceberg.flush_state;
SELECT iceberg_mount('public', 'af_test', '/tmp/test_iceberg_c2/basic');

INSERT INTO af_test VALUES (21, 1.0, 'p1');
INSERT INTO af_test VALUES (22, 2.0, 'p2');
SELECT count(*)=0 AS "关闭" FROM _gsiceberg.flush_jobs WHERE table_name='af_test';
-- 预期: t

UPDATE _gsiceberg.tables SET auto_flush_enabled=true WHERE table_name='af_test';
SELECT count(*)>=1 AS "立即flush" FROM _gsiceberg.flush_jobs WHERE table_name='af_test';
-- 预期: t

-- ===== M1-3: 禁用 =====
UPDATE _gsiceberg.tables SET auto_flush_enabled=false WHERE table_name='af_test';
DELETE FROM _gsiceberg.flush_jobs;
INSERT INTO af_test VALUES (31, 1.0, 'disabled');
SELECT pg_sleep(3);
SELECT count(*)=0 AS "禁用" FROM _gsiceberg.flush_jobs WHERE table_name='af_test';
-- 预期: t

-- ===== M1-4: 改 interval 只重置计时 =====
UPDATE _gsiceberg.tables SET auto_flush_interval_sec=999 WHERE table_name='af_test';
SELECT count(*)=0 AS "不触发" FROM _gsiceberg.flush_jobs WHERE table_name='af_test';
SELECT extract(epoch FROM now()-last_flush_at)::int <= 10 AS "计时器重置"
FROM _gsiceberg.tables WHERE table_name='af_test';
-- 预期: t, t

-- ===== M1-5: sync 模式 =====
SELECT iceberg_unmount('af_test');
DELETE FROM _gsiceberg.flush_jobs; DELETE FROM _gsiceberg.flush_state;
SELECT iceberg_mount('public', 'af_test', '/tmp/test_iceberg_c5/basic');

UPDATE _gsiceberg.tables SET auto_flush_enabled=true, auto_flush_interval_sec=5,
    auto_flush_mode='sync', auto_flush_min_rows=1, last_flush_at=now()
WHERE table_name='af_test';

INSERT INTO af_test VALUES (41, 1.0, 'sync');
SELECT pg_sleep(7);
INSERT INTO af_test VALUES (42, 2.0, 'sync2');
SELECT pg_sleep(2);
SELECT count(*)>=1 AS "sync完成" FROM _gsiceberg.flush_jobs WHERE table_name='af_test' AND status='completed';
-- 预期: t

-- ===== M1-6: min_rows 阈值 =====
SELECT iceberg_unmount('af_test');
DELETE FROM _gsiceberg.flush_jobs; DELETE FROM _gsiceberg.flush_state;
SELECT iceberg_mount('public', 'af_test', '/tmp/test_iceberg_c6/basic');

UPDATE _gsiceberg.tables SET auto_flush_enabled=true, auto_flush_interval_sec=3,
    auto_flush_min_rows=3, last_flush_at=now()
WHERE table_name='af_test';

INSERT INTO af_test VALUES (51, 1.0, 'b1');
INSERT INTO af_test VALUES (52, 2.0, 'b2');
SELECT pg_sleep(5);
SELECT count(*)=0 AS "2行<min=3" FROM _gsiceberg.flush_jobs WHERE table_name='af_test';
-- 预期: t

INSERT INTO af_test VALUES (53, 3.0, 'a1');
SELECT pg_sleep(2);
SELECT count(*)>=1 AS "3行>=min=3" FROM _gsiceberg.flush_jobs WHERE table_name='af_test';
-- 预期: t

-- ===== M1-7: 多表 =====
SELECT iceberg_unmount('af_test'); SELECT iceberg_unmount('orders');
DELETE FROM _gsiceberg.flush_jobs; DELETE FROM _gsiceberg.flush_state;
SELECT iceberg_mount('public', 'af_test', '/tmp/test_iceberg_c1/basic');
SELECT iceberg_mount('public', 'orders', '/tmp/test_iceberg_c7/basic');
UPDATE _gsiceberg.tables SET auto_flush_enabled=true, auto_flush_interval_sec=5,
    auto_flush_min_rows=1, last_flush_at=now() WHERE table_name='af_test';
UPDATE _gsiceberg.tables SET auto_flush_enabled=true, auto_flush_interval_sec=3,
    auto_flush_min_rows=1, last_flush_at=now() WHERE table_name='orders';

INSERT INTO af_test VALUES (61, 1.0, 'm1');
INSERT INTO orders VALUES (71, 1.0, 'm2');
SELECT pg_sleep(6);
INSERT INTO af_test VALUES (62, 2.0, 'm1b');
INSERT INTO orders VALUES (72, 2.0, 'm2b');
SELECT pg_sleep(2);
SELECT table_name, count(*)>=1 AS pass FROM _gsiceberg.flush_jobs
WHERE table_name IN ('af_test','orders') GROUP BY table_name;
-- 预期: 两行都是 t

-- ===== M1-8: scheduler + next_sec =====
SELECT count(*)>=0 AS pass FROM iceberg_auto_flush_scheduler();
SELECT _gsiceberg.iceberg_auto_flush_next_sec() > 0 AS pass;
-- 预期: t, t
```

### 7.2 模块二：生命周期追踪

```sql
DELETE FROM _gsiceberg.flush_jobs; DELETE FROM _gsiceberg.flush_state;
SELECT iceberg_unmount('af_test');
SELECT iceberg_mount('public', 'af_test', '/tmp/test_iceberg_c_m2/basic');

-- M2-0: 分步阶段观察
INSERT INTO af_test VALUES (1, 1.0, 'stage_walk');
SELECT iceberg_flush('af_test') AS job_id;
SELECT count(*)=0 AS "Phase1后空" FROM _gsiceberg.flush_state WHERE table_name='af_test';
-- 预期: t

-- 手动执行各阶段（用 DO 块传 job_id）
DO $$ DECLARE jid bigint; fs record;
BEGIN
    SELECT job_id INTO jid FROM _gsiceberg.flush_jobs
    WHERE table_name='af_test' AND status='pending' ORDER BY job_id DESC LIMIT 1;

    PERFORM public.iceberg_flush_stage_foreign('af_test', jid);
    SELECT flush_status, stage INTO fs FROM _gsiceberg.flush_state WHERE table_name='af_test';
    RAISE NOTICE 'foreign: status=%, stage=%', fs.flush_status, fs.stage;

    PERFORM public.iceberg_flush_stage_flush('af_test', jid);
    SELECT flush_status, stage INTO fs FROM _gsiceberg.flush_state WHERE table_name='af_test';
    RAISE NOTICE 'flush: status=%, stage=%', fs.flush_status, fs.stage;

    PERFORM public.iceberg_flush_stage_train('af_test', jid);
    SELECT flush_status, stage INTO fs FROM _gsiceberg.flush_state WHERE table_name='af_test';
    RAISE NOTICE 'train: status=%, stage=%', fs.flush_status, fs.stage;

    PERFORM public.iceberg_flush_stage_cleanup('af_test', jid);
    SELECT flush_status, stage INTO fs FROM _gsiceberg.flush_state WHERE table_name='af_test';
    RAISE NOTICE 'cleanup: status=%, stage=%', fs.flush_status, fs.stage;
END $$;

-- M2-1: flush 后状态
SELECT 'idle|cleanup' AS expected, flush_status, stage
FROM _gsiceberg.flush_state WHERE table_name='af_test';
-- 预期: idle | cleanup

-- M2-2: 多次 flush 累积
INSERT INTO af_test VALUES (2, 2.0, 'round2');
SELECT iceberg_flush('af_test') AS job2;
SELECT pg_sleep(2);  -- bgworker 自动消费
SELECT count(*)>=2 AS "累积>=2" FROM iceberg_flush_history('af_test');
-- 预期: t

-- M2-3: 数据可见
INSERT INTO af_test VALUES (3, 3.0, 'round3');
SELECT iceberg_flush('af_test') AS job3;
SELECT pg_sleep(2);
SELECT count(*)>=1 AS "可见" FROM af_test WHERE name='round3';
-- 预期: t
```

### 7.3 模块三：后台调度

```sql
DELETE FROM _gsiceberg.flush_jobs; DELETE FROM _gsiceberg.flush_state;
SELECT iceberg_unmount('af_test');
SELECT iceberg_mount('public', 'af_test', '/tmp/test_iceberg_c_m3/basic');

-- M3-1: Phase1 → pending → bgworker → completed
INSERT INTO af_test VALUES (91, 1.0, 'phase1');
SELECT iceberg_flush('af_test') AS job_id;         -- 用户获得 job_id
SELECT status FROM _gsiceberg.flush_jobs WHERE table_name='af_test' ORDER BY job_id DESC LIMIT 1;
-- 预期: pending（Phase 1 完成）

SELECT pg_sleep(2);
SELECT status FROM _gsiceberg.flush_jobs WHERE table_name='af_test' ORDER BY job_id DESC LIMIT 1;
-- 预期: completed（bgworker 自动消费）

-- M3-2: sync flush 一步完成
INSERT INTO af_test VALUES (92, 2.0, 'sync');
SELECT iceberg_flush_sync('af_test') AS ok;
SELECT status FROM _gsiceberg.flush_jobs WHERE table_name='af_test' ORDER BY job_id DESC LIMIT 1;
-- 预期: completed

-- M3-3: bgworker 在 pg_stat_activity 可见
SELECT count(*)=1 AS "bgworker running" FROM pg_stat_activity
WHERE backend_type = 'gsiceberg auto_flush';
-- 预期: t

-- M3-4: GUC 存在
SELECT count(*)=1 AS "GUC exists" FROM pg_settings WHERE name='gsiceberg.bgworker_interval';
-- 预期: t
```

### 7.4 一键运行

```bash
# 1. 生成测试数据
python3 /home/gv/workspace/gsiceberg/test/fixtures/gen_basic.py
for d in c1 c2 c5 c6 c7 c_m2 c_m3; do cp -r /tmp/test_iceberg /tmp/test_iceberg_${d}; done

# 2. 确认 bgworker 运行中
psql -p 5432 -d postgres -c "SELECT pid, backend_type FROM pg_stat_activity WHERE backend_type='gsiceberg auto_flush';"

# 3. 运行全模块测试
psql -p 5432 -d gv -f /home/gv/workspace/flush_devlop/sql/test_all_modules.sql
```

### 7.5 验证结果

```sql
-- 验证 bgworker 存活
SELECT pid, backend_type, state FROM pg_stat_activity WHERE backend_type LIKE '%gsiceberg%';

-- 验证 flush 历史
SELECT table_name, count(*) AS jobs FROM _gsiceberg.flush_jobs GROUP BY table_name;

-- 验证数据
SELECT count(*) FROM af_test;
```
