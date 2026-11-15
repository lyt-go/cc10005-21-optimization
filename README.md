# 多源异构数据接入引擎

面向多源异构数据接入、智能代理调度、动态限速、爬虫深度管理、数据去噪、格式转换与采集质量监控的数据接入工程管理平台。

这是一个前后端分离的管理平台：前端 React + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 React + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 连接器管理 | `connector` | 数据连接器 | 连接器编号、协议类型、目标端点 |
| 代理池管理 | `proxypool` | 代理实例 | 实例编号、代理地址、代理端口 |
| 深度爬取 | `deepcrawl` | 爬虫配置 | 配置编号、种子URL、最大深度 |
| JS渲染采集 | `jsrender` | 渲染配置 | 配置编号、目标URL、浏览器类型 |
| 字段解析器 | `parser` | 解析模板 | 模板编号、目标字段数、XPath表达式 |
| 数据去噪 | `dataclean` | 去噪规则 | 规则编号、目标字段、HTML标签清理 |
| 解析清洗流水线 | `pipeline` | 依赖图/版本化解析清洗结果 | 目标字段、解析字段结果、清洗结果、规则版本、回填批次 |
| 格式转换 | `formatter` | 转换器 | 转换器编号、输入格式、输出格式 |
| 采集去重 | `dedupengine` | 去重引擎 | 引擎编号、去重字段组、哈希算法 |
| 动态限速 | `throttle` | 限速规则 | 规则编号、目标域组、初始速率 |
| 验证码处理 | `captcha` | 验证码策略 | 策略编号、验证码类型、识别服务商 |
| 采集监控 | `monitor` | 监控面板 | 面板编号、监控指标集、采样间隔秒 |
| 数据分发 | `exporttask` | 分发任务 | 任务编号、数据源标识、目标系统 |
| 种子管理 | `seedmgr` | URL种子 | 种子编号、种子URL、发现来源 |
| 站点地图 | `sitemap` | 站点描述 | 站点编号、站点域名、robots规则 |
| Cookie管理 | `cookiejar` | 会话凭证 | 凭证编号、所属站点、登录方式 |
| 采集器健康 | `healthcheck` | 健康报告 | 报告编号、采集器名称、CPU占用率 |
| 采集快照 | `snapshot` | 数据快照 | 快照编号、关联任务、快照时间 |
| 操作审计 | `audittrail` | 审计记录 | 记录编号、操作类型、操作对象 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 解析清洗流水线（pipeline）

字段规则改动按字段依赖图做增量重算，落库在 SQLite（默认 `backend/data/pipeline.sqlite3`，
可用环境变量 `PIPELINE_DB_PATH` 覆盖，`*.sqlite3` 已在 `.gitignore` 中）：

- **依赖图缩小重算范围**：字段为节点，解析规则的 `references` 为边；改动字段只失效
  自身与传递依赖它的字段，其他字段不触碰。
- **步骤级节点复用**：抽取、类型转换、六类清洗、敏感词脱敏各自独立成节点，按
  `config_hash`（脱敏只看配置哈希）与 `input_hash` 决定复用；配置与输入均未变的
  类型转换、敏感词脱敏继续复用，不会被错误推进。
- **可切换新版本 + 原子切换**：新规则先以 `pending` 落库并构建全部节点，成功后在同一
  事务内转 `confirmed`；构建期间读取的始终是上一版已确认结果，任何失败整批回滚。
- **批次冲突裁决**：每次确认分配单调 `confirmed_seq`，字段当前版本取覆盖该字段的最大
  seq；多批次重叠时冲突字段以后确认版本为准，其他字段不受牵连。
- **持久与历史**：历史规则版本与节点全部保留；列表与详情共用同一查询口径，
  刷新页面或重启进程结果一致。

接口前缀 `/api/pipeline`：`/graph` 依赖图、`/results` 与 `/records/{id}` 当前解析/清洗
结果、`/rules/versions` 版本历史、`/batches` 回填批次、`POST /backfill` 提交规则批次。

领域：数据工程
子方向：数据采集、爬虫与RPA
