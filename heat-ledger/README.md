# 供热检修交接账同步服务(heat-ledger)

城市供热调度中心本地服务:从源系统的变更日志同步检修交接账,值班员通过 HTTP
查询任意一次交接完成后某小区能够接通的全部在用热源,以及该场检修造成的
断供/恢复小区清单。只要小区能接通任一在用热源,即视为有供热。

- TypeScript + Node.js(≥22.5)+ SQLite(`node:sqlite` 嵌入式存储,零原生依赖)
- 批次乱序/重复/分批到达自动归位;新旧两种管段记录格式兼容
- 替换单触发级联重算,值班员可用新账复核自该场检修起的交接历史
- 等待补齐的日志只占用有界内存;算新账期间 HTTP 仍查询上一份完整交接账
- 源端可清理已确认日志;服务重启后凭 SQLite 中的游标接着同步

## 快速开始

```bash
npm install
npm run demo      # 演示+自检:自动编译,用自带回放数据走完整流程(约几秒)
npm start         # 启动常驻服务(默认端口 8080,先自动编译)
```

`npm run demo` 会依次演示:乱序/重复/混排日志同步 → 杀进程模拟重启续传 →
补交替换单并级联重算 → 新账复核与值班员查询,并对关键结果做断言。

> 注意:按同步协议,服务确认接收后源端会清理对应日志行。`npm start` 默认
> 以 `data/replay` 为源目录,运行后其中已确认的批次行会被删除(这正是
> “源端清理已确认日志”的行为)。想反复回放,请先复制该目录或改用
> `DATA_DIR` 指向副本;`npm run demo` 始终在副本上运行,不动自带数据。

环境变量(均有默认值):

| 变量 | 默认 | 说明 |
|---|---|---|
| `PORT` | `8080` | HTTP 端口 |
| `DB_PATH` | `./heat-ledger.db` | SQLite 账库文件 |
| `DATA_DIR` | `./data/replay` | 源系统回放目录(*.jsonl) |
| `POLL_MS` | `500` | 同步轮询间隔 |
| `PAGE_SIZE` | `100` | 每轮最多入库的批次数(内存上界) |

## 日志格式

源系统按**批次**提交,回放目录中每个 `*.jsonl` 文件每行一个批次(JSON):

```json
{"seq":6,"maintenance":"M-001","handover":"H-001","part":2,"parts":2,"kind":"normal",
 "records":[{"type":"valve.close","pipe":"P6"}]}
```

| 字段 | 含义 |
|---|---|
| `seq` | 全局提交序号(先后),正整数;重投保持不变,服务按序确认、幂等去重 |
| `maintenance` | 检修单号 |
| `handover` | 所属交接 |
| `part` / `parts` | 本批在检修内的序号 / 该检修总批数(范围);收齐 `1..parts` 才算交接完成 |
| `kind` | `normal` 普通批;`replacement` 替换单,覆盖同 `maintenance+part` 的旧批 |
| `records` | 变更记录数组,批内按顺序应用 |

**记录类型**:

| type | 必填字段 | 说明 |
|---|---|---|
| `community.add` | `node`,`name` | 登记小区(断供/恢复统计只覆盖已登记小区) |
| `pipe.add` | `id`,`a`,`b`;可选 `direction` | 新增管段。旧记录只登记两端(无 `direction`),按普通管段双向送热;新记录 `direction:"forward"` 为止回管段,只能按 A→B 送热,`"both"` 为普通管段 |
| `pipe.remove` | `id` | 拆除管段 |
| `valve.open` / `valve.close` | `pipe` | 管段阀门开/闭(关闭不通热;新增管段默认开) |
| `source.start` | `id`,`node`;可选 `name` | 热源投运(在用) |
| `source.stop` | `id` | 热源停运 |

**同步协议**:服务按游标(已确认的最大连续 `seq`)向源拉取,落库后回执
`ack(cursor)`;源端可据此清理已确认日志。游标、批次、账版都在 SQLite 中,
重启后续传不重复、不遗漏。

**替换单与账版**:每场交接完成即生成一个账版(单调递增版本号,含全网快照与
断供/恢复清单)。对已交接检修补交替换单后,该检修及其后所有交接按链式顺序
级联重算,各自产生替代旧账的新账版(`supersedes` 指向被替代版本);旧账版
保留可查。新账版算好后一次性提交,计算期间查询到的仍是上一份完整交接账。

## HTTP 接口

所有响应均为 JSON 并带 `ledgerVersion`,标明所用账版。

| 接口 | 说明 |
|---|---|
| `GET /api/status` | 同步游标、当前账版、各检修收批进度 |
| `GET /api/handovers?from=<检修单号>` | 交接账列表;`from` 表示“自该场检修起”(复核历史) |
| `GET /api/handovers/<交接号>/impact` | 该场检修的断供/恢复小区清单(该交接的最新账版);`交接号=latest` 表示最新交接;可加 `?version=N` 查指定旧账版 |
| `GET /api/communities/<节点>/sources` | 某次交接后小区可接通的全部在用热源与供热状态;默认最新交接,可加 `?handover=<交接号>` 或 `?version=N` |
| `GET /api/ledger?version=N` | 账版快照:管段、热源、各小区供热状态 |

示例:

```bash
curl -s localhost:8080/api/communities/C2/sources            # 最新交接后 C2 的热源
curl -s localhost:8080/api/communities/C2/sources?handover=H-001
curl -s localhost:8080/api/handovers/latest/impact           # 最新交接的断供/恢复
curl -s 'localhost:8080/api/handovers?from=M-001'            # 自 M-001 起的交接历史
```

响应示例(`GET /api/communities/C2/sources?handover=H-001`):

```json
{"node":"C2","name":"翠湖小区","handover":"H-001","ledgerVersion":5,
 "supplied":false,"sources":[]}
```

## 回放数据

`data/replay/` 内置一段完整剧本(6 个小区、2 个热源、8 条提交):

- `replay-part1.jsonl`:初始建账(M-000,3 批)、停炉检修(M-001,2 批,其中
  第 2 批**误录**阀门关错对象)、恢复(M-002)、拆除退役管段(M-003)。
  文件内批次故意乱序并含一行重复;管段记录新旧格式混排。
- `replay-part2.jsonl`:针对 M-001 第 2 批的**替换单**(seq=8)。

演示脚本会把它们复制到 `.demo/runtime/` 再回放(源端确认清理不污染自带数据),
账库写在 `.demo/heat.db`。

## 设计要点

- **有界内存**:源数据流式逐行读取,每轮最多 `PAGE_SIZE` 批落库;等待补齐的
  片段存 SQLite,不占内存。
- **不断链**:检修按“最小 seq”排序成链,前序交接未收齐时后续一律等待。
- **原子换账**:新账版链在内存算好后单事务提交;重放分片让出事件循环,
  大批次计算期间 HTTP 仍可读上一份完整交接账。
- **幂等**:`seq` 去重、`maintenance+part` 覆盖式替换,重投/重启安全。

## 目录结构

```
src/model.ts    日志模型与校验        src/ledger.ts  账版链式计算
src/network.ts  管网与可达性          src/sync.ts    同步引擎
src/store.ts    SQLite 持久化         src/source.ts  文件回放源(含确认清理)
src/server.ts   HTTP 服务             src/demo.ts    演示与自检
data/replay/    自带回放数据
```
