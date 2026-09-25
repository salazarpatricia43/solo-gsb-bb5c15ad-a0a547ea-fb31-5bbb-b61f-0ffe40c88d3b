# 实验室传感器时间线归并 + 发布后阈值告警 API（FastAPI + SQLite）

多个传感器来源（source）按各自的序列号（seq）**乱序批量上报**带事件时间（`observed_at`）的读数。
服务以“**最慢来源的事件时间 − 允许迟到量**”维护水位线（watermark），只发布越过水位线、
且此前尚未发布的读数，并按 `(observed_at, source_id, seq)` 输出**确定顺序**的归并流。
越过**已发布**水位线才到达的迟到读数不会改写历史，而是进入**隔离区**供查询。

采集会话是**有限集合**：每个来源可单独**封存（seal）**，声明自己的 `final_observed_at`；
各来源陆续封存期间水位线语义不变，**最后一个来源封存时**，服务在同一写事务内把全部剩余
buffered 读数按既定全局顺序一次性发布（**尾流发布**）、推进两类告警并把会话置为 `closed`，
此后拒绝任何新 key。

在此之上，本服务还提供两种发布后告警：

1. **单来源阈值告警**：为会话内某个来源创建规则后，系统在**每一条确定发布的读数**上
   驱动该来源独立的两态状态机（`normal` → `active` → `normal`），用**进入/恢复双阈值
   构成的滞回带**加**连续次数**消除边界抖动，并支持当前告警查询与进入/恢复事件的历史分页。
2. **跨来源复合告警**：一条规则声明 2～4 个**不同来源**的条件（各自的方向、阈值、
   可选 `value_key` 与 `window_seconds`）。系统按归并顺序为每个条件维护“最近一次命中”，
   当所有条件的命中都落在各自的事件时间窗口内时产生**一次** `correlated_entered` 事件；
   任一条件超窗即静默回到等待状态，可再次触发。

此外，**释放计划**接口为 open 会话只读回答“要按全局顺序放出最早 N 条 buffered 读数，
各来源的事件时钟最少需要推进到哪儿”：整个计算在同一个 SQLite 读快照内完成，
已封存而时钟不足的来源不会被建议推进，而是直接给出 `attainable=false` 及原因。

最后，**检查点安全的存储清理**接口 `POST /sessions/{session_id}/retention` 用于回收
旧读数的存储空间：它只把**事件时间严格早于**给定 `cutoff` 且状态为
`published`/`quarantined` 的读数作为候选（`buffered` 永不参与）；隔离候选可直接删除，
而已发布候选中凡是**会话最早检查点仍可能在续读时读到**的一律保留，并在响应中返回该
“检查点保护边界”。`dry_run=true` 只报告候选/保留/可删除数量，`dry_run=false` 在
**同一个 `BEGIN IMMEDIATE` 写事务**内原子删除；重复请求幂等，失败整体回滚，
且不改变水位线、告警与检查点的任何语义。每条被实际删除的读数都会在删除所在的
同一事务内留下一条**判重墓碑**（`(session_id, source_id, seq)` + 规范化 payload 摘要），
因此清理之后的同 key 重试仍按幂等判重处理（见“时间线语义”第 12 条）。

此外，**隔离影响预演**接口 `POST /sessions/{session_id}/quarantine-audit` 对隔离区读数做
只读“假如按事件时间插入已发布流”的推演：在**同一个 SQLite 读快照**内，按确定顺序
`(observed_at, source_id, seq)` 给出每条隔离读数将落在的**插入位置**、前后已发布邻居的
**键与事件时间差**，以及最早会受影响的**检查点名称**；不写任何数据，支持按 `source_id`、
事件时间闭区间 `from`/`to` 与 `limit` 稳定过滤。

最后，**来源缺口扫描**接口 `GET /sessions/{session_id}/source-gaps` 在**同一个 SQLite
读快照**内按来源扫描**已发布**读数，找出相邻读数事件时间差**严格大于** `max_gap_seconds`
的缺口，返回来源、起止读数键与间隔秒数；支持 `source_id`、事件时间闭区间 `from`/`to` 与
稳定游标分页，全程只读，不影响水位线、告警与检查点。

此外，**到达延迟剖析**接口 `GET /sessions/{session_id}/arrival-latency` 在**同一个 SQLite
读快照**内对会话读数做只读的到达延迟统计：以每条读数的**首次接收时刻减事件时刻**
（`first_received_at - observed_at`）计算延迟，按会话及可选 `source_id`、事件时间
**闭区间** `from`/`to` 筛选读数（`buffered`/`published`/`quarantined` 三种状态都参与），
逐来源返回样本数、最小/最大延迟与算术平均延迟；**负延迟照实保留**（时钟超前于接收时刻），
没有匹配读数的来源不出现在结果中，来源结果按 `source_id` 稳定排序。

最后，**阈值暴露时长**接口 `GET /sessions/{session_id}/exposure` 在**同一个 SQLite
读快照**内只按该来源的**已发布**读数，计算事件时间**半开区间** `[from, to)` 内其值
**严格高于** `threshold` 的累计秒数，同时返回有效采样覆盖秒数与未知秒数（无覆盖时占比为
`null`，否则为超阈秒数 / 有效覆盖秒数）。每条读数的值持续到该来源下一条已发布读数或查询
终点；区间起点继承此前最近读数；同一时刻多条读数按 `seq` 顺序、最后一条生效。缺值、
非有限数值与布尔值形成未知区间，不沿用更早的有效值，起点前无读数同样未知。

此外，**局部尖峰分析**接口 `GET /sessions/{session_id}/local-spikes` 在**同一个 SQLite
读快照**内只按指定来源的**已发布**读数，在事件时间**闭区间** `[from, to]` 内按
`(observed_at, seq)` 稳定排序后做局部中位数/MAD 异常检测：仅有限非布尔数值参与
（可用 `value_key` 从对象值提取指定顶层键，布尔值与缺失值跳过），每条有效读数只以区间内
事件时间距离不超过 `window_seconds`（正数、边界含等号）的**其他**有效读数为邻居；邻居
少于 3 条不判定，否则取邻居中位数与绝对偏差中位数（MAD），仅当读数与中位数的绝对差
**严格大于** `sensitivity`（正数）乘 MAD 时标记为尖峰（MAD 为零时仍按严格大于零判断）。
中位数与 MAD 的偶数项取平均时**不经过中间溢出**（四个 `1e308` 邻居的中位数仍为有限值
`1e308` 而非 `Infinity`），凡数学上可表示为有限浮点的统计量一律返回有限 JSON 数值。
响应给出每个尖峰的读数键、数值、局部中位数、MAD 与偏差，并列出有效读数数与邻居不足数；
区间内已发布读数超过 200 条直接报错。

- 语言/框架：Python 3.11 · FastAPI · Uvicorn
- 存储：SQLite（WAL 模式，单文件，零外部依赖）
- 并发：写操作在 `BEGIN IMMEDIATE` 事务内串行化，水位线推进单调、发布只发生一次，
  告警状态、来源封存与尾流发布均在同一把写锁/同一事务内推进，消除上报与封存的并发竞态

## 单条命令启动

```bash
docker compose up --build
```

启动后：

- 服务地址：`http://localhost:8000`
- 交互文档（Swagger UI）：`http://localhost:8000/docs`
- OpenAPI JSON：`http://localhost:8000/openapi.json`
- 健康检查：`http://localhost:8000/health`

数据库文件在容器内 `/data/sensors.db`，由 compose 命名卷 `sensor-data` 持久化；
建库与建表在应用启动时（FastAPI lifespan）自动完成，无需手动初始化。
停止并清理：`docker compose down`（加 `-v` 同时删除数据卷）。

## 必要配置

通过环境变量配置（compose 中已给默认值，可按需覆盖）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SENSOR_HOST` | `0.0.0.0` | uvicorn 绑定地址 |
| `SENSOR_PORT` | `8000` | 监听端口（容器内固定 `EXPOSE 8000`，改宿主机端口改 compose 的 `ports`） |
| `SENSOR_DB_PATH` | `/data/sensors.db` | SQLite 文件路径，父目录不存在会自动创建 |

## 本地开发（不用 Docker）

```bash
python3 -m pip install -r requirements.txt
SENSOR_DB_PATH=./sensors.db uvicorn app.main:app --reload --port 8000
pytest -q        # 运行 438 个端到端行为测试（需 pytest、httpx）
```

## 时间线语义

1. **创建会话固定来源列表与迟到容忍**：`source_ids` 之后不可更改；上报未知 `source_id` 返回 400。
2. **首报门槛**：所有来源都至少上报一次（或被封存，封存即声明了最终事件时间）后水位线才有定义；
   此前读数全部缓冲、不发布、不隔离。
3. **水位线**：`WM = min(每个来源已知最大 observed_at) − allowed_lateness_seconds`，单调不减
   （来源时钟回拨不会使已发布水位线倒退）。已封存来源以其 `final_observed_at` 作为已知最大值
   参与同一个水位线计算，因此各来源陆续封存时水位线保持原有语义。
4. **发布规则**：仅当 `observed_at < WM`（严格小于，落在边界上的读数继续等待）时发布；
   每次上报只发布此前未发布的缓冲读数。发布顺序固定为 `observed_at, source_id, seq`。
5. **幂等**：重复提交 `(source_id, seq)` 且 `value` 相同 → 计为 `duplicates` 的空操作；
   同 key 不同 payload → 409，且该批次整体回滚（不会半写）。来源封存、甚至会话关闭后，
   完全相同的既有读数重试**仍然**计为 `duplicates`。
6. **隔离（quarantine）**：读数到达时若 `observed_at < 已发布水位线`，它永远无法再进入已发布流，
   原样写入隔离区（记录当时的水位线），已发布历史不发生任何变化。
7. 时间统一按 UTC 处理：接受 `...Z`、带偏移量（`+02:00`）或不带时区（按 UTC）的 ISO 8601 字符串。
8. **来源封存（seal）**：封存接口为某来源声明 `final_observed_at`；该值**不得早于**该来源已见的
   最大事件时间（否则 400）。同值重试幂等（返回相同的封存结果、不改变 `sealed_at`）；
   已封存后用不同值封存 → 409。未知来源 → 400，会话不存在 → 404。
9. **封存后拒绝新读数**：来源封存后，任何此前未见过的 `(source_id, seq)` 上报均返回 409；
   但对**完全相同的既有读数**的重试照旧计为 `duplicates`，同 key 不同 payload 照旧 409。
   批次中只要混入一条被封存来源的新 key，整批回滚。
10. **尾流发布与关闭（close）**：当（且仅当）**最后一个**来源封存时，系统在封存所在的同一个
    `BEGIN IMMEDIATE` 写事务内：把所有剩余 buffered 读数按既定全局顺序 `(observed_at,
    source_id, seq)` 全部发布（以一个晚于任何缓冲事件时间的**终止水位线**一次性发布；
    此前正常推进的水位线保持单调），随后推进全部单来源告警与跨来源复合告警，最后把会话置为
    `closed`。关闭后任何新 key 上报一律 409（既有读数的相同重试仍计为 duplicates）。
11. **同一把写锁**：封存与上报都在同一串行化写事务内执行，因此“封存与新读数并发”只会得到
    两种线性化结果之一——读数先落库（封存照常进行），或封存先生效（新读数 409），
    不会出现封存后仍混入新读数、或尾流发布遗漏/重复读数的竞态。
12. **清理后的重试判重（retention tombstone）**：retention 每实际删除一条读数，都在删除所在的
    同一事务内向 `retention_tombstones` 写入一条墓碑（`(session_id, source_id, seq)` 主键 +
    规范化 payload 的 SHA-256 摘要）。之后上报在写入前先查现存行、再查墓碑：
    同 key 同值 → 仍计为 `duplicates` 的空操作——**不恢复读数、不推进水位线、不再次触发
    告警、也不会重新进入隔离区**；同 key 异值 → 409，混合批次整体回滚。即使来源已封存、
    会话已关闭，被清理 key 的相同重试依然计为 `duplicates` 而不是被拒绝；
    检查点续读结果不受任何影响。

## 告警状态机语义

每条规则绑定一个会话中的一个来源，方向为 `above`（高于阈值告警）或 `below`（低于阈值告警），
状态机**只消费该来源状态为“已发布”的读数**，按发布顺序 `(observed_at, seq)` 逐条推进：

- **normal（正常）**：连续出现 `enter_consecutive` 条“越限”读数后进入 `active` 并产生
  `entered` 事件。`above` 下越限指 `value > enter_threshold`；`below` 下指 `value < enter_threshold`。
- **active（告警中）**：连续出现 `recover_consecutive` 条“恢复”读数后回到 `normal` 并产生
  `recovered` 事件。`above` 下恢复指 `value < recover_threshold`；`below` 下指 `value > recover_threshold`。
- **滞回防抖**：`above` 要求 `recover_threshold < enter_threshold`，`below` 要求
  `recover_threshold > enter_threshold`；落在两个阈值之间（滞回带内）的读数不越限也不恢复，
  只会把当前连续计数清零。阈值比较是**严格**的，恰好等于阈值的读数不计入。
- 非数值（字符串、布尔、NaN/Infinity）、缺失值或指定了 `value_key` 但对象里没有该键时，
  该读数只会重置连续计数，不产生事件、不改变状态。**无法表示为有限浮点数的 JSON 整数**
  （如 `10**400`：能正常入库/发布，但 `float()` 转换会溢出）同等视为无效告警值——
  打断连续计数、既不触发进入也不触发恢复；读数本身照常保存与发布，不会让上报或封存事务
  失败回滚。
- **只有确定发布的读数才推进状态**：缓冲中的读数不参与；因迟到进入隔离区的读数状态永远是
  `quarantined`，不可能被状态机看到，因此**绝不会改变告警状态或补造事件**。
- **按来源独立**：不同来源（以及同一来源上的多条规则）各自维护独立的状态与连续计数。
- **晚建规则等价回放**：状态机是已发布流的纯函数。规则创建时会立即按顺序回放该来源此前全部
  已发布读数（不包括隔离读数），因此无论规则何时创建，其当前状态与历史事件都与“一开始就建好”
  完全一致——无法表示为有限浮点数的整数在回放中同样只打断连续计数、不产生事件。
- 数值来源：读数 `value` 本身是数字时直接比较；创建规则时给 `value_key`（如 `"temp"`）则从
  对象型读数的该顶层键取值。

## 跨来源复合告警语义

每条复合规则声明 **2～4 个条件**，条件按声明顺序（`position`）保存；每个条件绑定会话中
一个**互不相同**的来源，并自带方向（`above`/`below`）、阈值、可选 `value_key` 与窗口
`window_seconds`。规则消费的是**整个会话的已发布归并流**（顺序 `(observed_at, source_id,
seq)`），而不仅是条件来源的读数：

- 每处理一条确定发布的读数，若它属于某个条件且**严格越限**（`above`：`value > threshold`；
  `below`：`value < threshold`，相等不算），就刷新该条件的“最近命中”（记录 `seq`、
  `observed_at`、`value`）。非数值、布尔、NaN/Infinity、缺值或缺键的读数不会命中。
  **无法表示为有限浮点数的 JSON 整数**（如 `10**400`）同样不会被记作命中：它既不刷新
  任何条件的“最近命中”，也不替代既有命中；既有命中仍按各自的**原事件时间**窗口照常过期，
  规则状态完全由该读数以外的窗口判定。
- 每次处理后（也包括不属于任何条件的来源读数——它们只推进事件时间）检查：每个条件都存在
  命中，且命中时间与当前读数事件时间之差 **≤ 各自的 `window_seconds`**（窗口边界**含等号**，
  恰好在界上仍然有效）。
- 规则为 `waiting` 且全部条件满足 → 转为 `correlated`，产生**一次** `correlated_entered`
  事件，事件锚定在完成本次复合的那条读数（`completed_by`）上，并快照各条件当时的命中
  （`conditions` 列表，按 `position`）。
- 已处于 `correlated` 时继续满足**不会重复事件**；只有当后续某条读数使任一条件超出自己的
  窗口（或尚无命中）时，规则才**静默**回到 `waiting`（不产生事件），此后才能再次复合。
- 每个条件的窗口互相独立：例如 A 的窗口 30s、B 的窗口 1s，只要 B 的命中超窗就回到等待，
  哪怕 A 仍新鲜。
- **只消费确定发布的读数**：缓冲读数不参与；迟到进入隔离区（`quarantined`）的读数永远不会
  成为已发布读数，因此既不能制造复合，也不会让命中“过期”。重复上报相同读数是空操作，
  同样不可能重复触发。
- **晚建规则等价回放**：复合结果是已发布归并流的纯函数。规则创建时会按全局归并顺序回放该会话
  此前全部已发布读数（不含隔离读数），其当前状态与历史事件与“一开始就建好”完全一致；回放期间
  无法表示为有限浮点数的整数同样不记作命中，也不改变既有命中按原事件时间窗口的过期。

## 接口

### 1. 创建会话

`POST /sessions`

```bash
curl -s -X POST localhost:8000/sessions -H 'Content-Type: application/json' -d '{
  "session_id": "lab-1",
  "source_ids": ["thermometer-a", "thermometer-b"],
  "allowed_lateness_seconds": 10
}'
```

`session_id` 可省略（自动生成 UUID hex）。响应 201。

### 2. 批量上报读数（乱序、迟到均可）

`POST /sessions/{session_id}/readings`

请求体支持 `{"readings": [...]}`，也接受裸 JSON 数组：

```bash
curl -s -X POST localhost:8000/sessions/lab-1/readings \
  -H 'Content-Type: application/json' -d '{
  "readings": [
    {"source_id": "thermometer-b", "seq": 2, "observed_at": "2026-09-21T10:00:20Z", "value": 21.4},
    {"source_id": "thermometer-a", "seq": 1, "observed_at": "2026-09-21T10:00:05Z", "value": 22.1}
  ]
}'
```

响应汇总本批处理结果：

```json
{
  "session_id": "lab-1",
  "received": 2,
  "duplicates": 0,
  "quarantined": 0,
  "published_now": 0,
  "published_total": 0,
  "buffered": 2,
  "all_sources_reported": true,
  "published_watermark": null
}
```

字段：`received` 批内条数；`duplicates` 幂等重复条数；`quarantined` 本批被隔离条数；
`published_now` 本批触发新发布的条数；`published_total` 会话累计已发布；`buffered` 仍在等待的条数。
本批一旦有新读数发布，相关告警规则会在**同一事务**内推进并可能生成事件。

错误码：未知来源/非法时间/空批次 → 400；字段结构错误（缺字段、`seq<0` 等）→ 422；
同 key 不同 payload、向**已封存来源**或**已关闭会话**上报新 key → 409；会话不存在 → 404。
注意：来源封存/会话关闭后，对完全相同既有读数的重试仍正常返回 200 并计入 `duplicates`；
被 retention 清理掉的读数同理——同 key 同值重试计为 `duplicates`（不恢复、不重新隔离），
同 key 异值 → 409（见“时间线语义”第 12 条）。

`value` 接受任意 JSON 值且**原样存储、原样发布**，不要求能转成浮点数：例如
`100000...000`（`10**400`，401 位整数）这类**无法表示为有限浮点数的 JSON 整数**也会正常
缓冲、发布并在 `/stream`、隔离区等接口中原样返回，同 key 重试仍按原 payload 判重。它只在
**告警求值**时被当作无效值处理（单来源规则打断连续计数、不触发进入/恢复；跨来源规则不记作
命中，见“告警状态机语义”与“跨来源复合告警语义”），不会让发布或封存事务报错回滚；暴露时长、
局部尖峰等只读分析同样把它视为无效/未知值。

### 3. 封存来源（声明采集结束）

`POST /sessions/{session_id}/sources/{source_id}/seal`

```bash
curl -s -X POST localhost:8000/sessions/lab-1/sources/thermometer-a/seal \
  -H 'Content-Type: application/json' -d '{
  "final_observed_at": "2026-09-21T10:05:00Z"
}'
```

- `final_observed_at` 不得早于该来源已见的最大 `observed_at`，否则 400；
  接受与读数上报相同的全部 ISO 8601 时间形式（统一按 UTC 归一化）。
- **同值重试幂等**：对已封存来源用相同 `final_observed_at`（含不同写法但同一时刻，如
  `...T12:05:00+02:00` 与 `...T10:05:00Z`）再次封存 → 200 且结果与首次完全一致
  （`sealed_at` 不变）；用**不同**值封存 → 409。
- 未知来源 → 400；会话不存在 → 404；请求体缺字段/为空 → 422。
- 封存后该来源不再接受新读数（409），既有读数的相同重试仍计为 `duplicates`。
- 封存非最后一个来源时，其 `final_observed_at` 按普通最大事件时间参与水位线推进，
  可能立即发布一批读数；响应返回最新会话状态（结构同“会话状态”接口）。
- **封存最后一个来源**时，响应中 `closed` 变为 `true`：全部剩余 buffered 读数已在同一事务内
  按全局顺序发布、两类告警已推进完毕，此后任何新 key 上报均 409。

### 4. 拉取已发布流

`GET /sessions/{session_id}/stream?limit=100&cursor=...`

- 只返回已发布读数，严格按 `(observed_at, source_id, seq)` 排序；
- keyset 分页：响应中的 `next_cursor` 原样回传即可翻页，为 `null` 表示取完；
- 每条读数带发布时的 `published_watermark`，响应体也带当前水位线（尾流发布时为终止水位线）。

```bash
curl -s 'localhost:8000/sessions/lab-1/stream?limit=50'
```

### 5. 查看隔离项

`GET /sessions/{session_id}/quarantine?limit=100&cursor=...`

按到达时间排列的迟到读数，含 `observed_at`、原始 `value` 与被拦截时的
`quarantined_watermark`（`reason` 固定为 `behind_published_watermark`）。同样支持游标分页。

```bash
curl -s 'localhost:8000/sessions/lab-1/quarantine'
```

### 6. 会话状态（辅助接口）

`GET /sessions/{session_id}`：返回来源列表、迟到容忍、是否已全部首报、
已发布水位线与当前计算出的有效水位线，以及封存/关闭状态：

- 顶层 `status`（`open`/`closed`）、布尔 `closed` 与关闭时刻 `closed_at`；
- `sources` 数组（按会话声明顺序）逐项给出 `source_id`、已见最大事件时间 `max_observed_at`、
  封存值 `final_observed_at`、封存时刻 `sealed_at` 与布尔 `sealed`。

```json
{
  "session_id": "lab-1",
  "source_ids": ["thermometer-a", "thermometer-b"],
  "allowed_lateness_seconds": 10.0,
  "published_watermark": "2026-09-21T09:59:50Z",
  "all_sources_reported": true,
  "effective_watermark": "2026-09-21T09:59:50Z",
  "created_at": "2026-09-22T06:00:00Z",
  "status": "open",
  "closed": false,
  "closed_at": null,
  "sources": [
    {"source_id": "thermometer-a", "max_observed_at": "2026-09-21T10:00:00Z",
     "final_observed_at": "2026-09-21T10:00:00Z",
     "sealed_at": "2026-09-22T06:10:00Z", "sealed": true},
    {"source_id": "thermometer-b", "max_observed_at": "2026-09-21T10:00:00Z",
     "final_observed_at": null, "sealed_at": null, "sealed": false}
  ]
}
```

### 7. 创建告警规则

`POST /sessions/{session_id}/alert-rules`

为一个**会话内已声明的来源**创建阈值规则。请求字段：

| 字段 | 说明 |
| --- | --- |
| `source_id` | 会话 `source_ids` 之一；未知来源 → 400 |
| `direction` | `above`（高于进入阈值告警）或 `below`（低于进入阈值告警） |
| `enter_threshold` | 进入阈值（有限数值；比较为严格大于/小于） |
| `recover_threshold` | 恢复阈值；`above` 时必须严格小于进入阈值，`below` 时必须严格大于，否则 400 |
| `enter_consecutive` | 进入所需连续越限读数数（≥1） |
| `recover_consecutive` | 恢复所需连续恢复读数数（≥1） |
| `value_key` | 可选；从对象型读数中取该顶层键比较，省略则直接比较读数 `value` |
| `rule_id` | 可选自定义 id；重复 → 409 |

```bash
curl -s -X POST localhost:8000/sessions/lab-1/alert-rules \
  -H 'Content-Type: application/json' -d '{
  "rule_id": "temp-high",
  "source_id": "thermometer-a",
  "direction": "above",
  "enter_threshold": 30.0,
  "recover_threshold": 25.0,
  "enter_consecutive": 3,
  "recover_consecutive": 2
}'
```

响应 201，含规则当前的 `state`（`normal`/`active`）与 `streak`。若该来源此前已有已发布读数，
创建时立即回放并可能直接返回 `active` 且生成历史事件。

### 8. 查询规则

- `GET /sessions/{session_id}/alert-rules`：列出会话全部规则（含当前状态）。
- `GET /sessions/{session_id}/alert-rules/{rule_id}`：单条规则；不存在 → 404。

### 9. 查询当前告警

`GET /sessions/{session_id}/alerts/current?source_id=...`

只返回处于 `active` 的规则，按进入时间排序。每条含进入时的触发读数详情
（`seq`、`observed_at`、`value`、当时所用 `threshold`、服务端 `triggered_at`）
以及规则最近处理到的读数位置。可用 `source_id` 限定来源；未知来源 → 400。

```bash
curl -s 'localhost:8000/sessions/lab-1/alerts/current'
```

### 10. 查询告警历史（分页）

`GET /sessions/{session_id}/alerts/history?limit=100&cursor=...&source_id=...&rule_id=...&event_type=...`

- 返回 `entered` / `recovered` 事件，**按事件 id 倒序（最新在前）**；
- `next_cursor` 不透明游标，原样回传翻页；损坏游标 → 400；
- 可选过滤：`source_id`（未知来源 → 400）、`rule_id`（不存在 → 404）、
  `event_type`（仅允许 `entered`/`recovered`，否则 400）。

```bash
curl -s 'localhost:8000/sessions/lab-1/alerts/history?limit=20&event_type=entered'
```

### 11. 创建跨来源复合告警规则

`POST /sessions/{session_id}/correlated-rules`

请求体含 `conditions`（2～4 项）与可选 `rule_id`；每个条件字段：

| 字段 | 说明 |
| --- | --- |
| `source_id` | 会话 `source_ids` 之一，条件之间不可重复；未知来源 → 400 |
| `direction` | `above`（严格高于阈值命中）或 `below`（严格低于阈值命中） |
| `threshold` | 命中阈值（有限数值，严格比较，相等不计） |
| `window_seconds` | 该条件命中的有效时长（秒，> 0，边界含等号） |
| `value_key` | 可选；从对象型读数中取该顶层键比较，省略则直接比较读数 `value` |

```bash
curl -s -X POST localhost:8000/sessions/lab-1/correlated-rules \
  -H 'Content-Type: application/json' -d '{
  "rule_id": "heatwave",
  "conditions": [
    {"source_id": "thermometer-a", "direction": "above", "threshold": 30.0, "window_seconds": 15},
    {"source_id": "thermometer-b", "direction": "above", "threshold": 28.0,
     "window_seconds": 10, "value_key": "temp"}
  ]
}'
```

响应 201，含规则当前 `state`（`waiting`/`correlated`）、各条件的当前 `last_hit`、
复合锚点 `correlated_since` 以及归并流处理位置（`last_observed_at`/`last_source_id`/
`last_seq`）。创建时立即回放，已有的已发布读数可能直接让规则返回 `correlated`。
结构错误（条件数不在 2～4、来源重复、窗口非正、NaN 阈值等）→ 422；重复 `rule_id` → 409。

### 12. 查询复合规则

- `GET /sessions/{session_id}/correlated-rules`：列出会话全部复合规则（含各条件最近命中）。
- `GET /sessions/{session_id}/correlated-rules/{rule_id}`：单条规则；不存在 → 404。

### 13. 查询当前复合命中

`GET /sessions/{session_id}/correlated-alerts/current`

只返回处于 `correlated` 的规则，按复合完成时间排序。每条含 `correlated_entered`：
完成读数 `completed_by`（`source_id`/`seq`/`observed_at`）、完成时刻各条件命中快照
`conditions`（含位置、方向、阈值、窗口、命中读数与取值）以及服务端 `triggered_at`。

```bash
curl -s 'localhost:8000/sessions/lab-1/correlated-alerts/current'
```

### 14. 查询复合事件历史（分页）

`GET /sessions/{session_id}/correlated-alerts/history?limit=100&cursor=...&rule_id=...`

- 返回 `correlated_entered` 事件，**按事件 id 倒序（最新在前）**；
- `next_cursor` 不透明游标，原样回传翻页；损坏游标 → 400；
- 可选 `rule_id` 过滤（规则不存在 → 404）；会话不存在 → 404。

```bash
curl -s 'localhost:8000/sessions/lab-1/correlated-alerts/history?limit=20'
```

### 15. 创建消费者检查点

`POST /sessions/{session_id}/checkpoints`

为会话已发布读数流的某个消费者记录一个**检查点**。请求体 `{"name": "..."}`：
名称在会话内唯一，允许字母、数字、`.`、`_`、`-`（须以字母/数字开头，最长 128）。

创建在**同一个 `BEGIN IMMEDIATE` 事务（同一只读快照）**内记录三件事：

- 当前发布流**位置**：已发布读数条数 `stream_offset`，以及最后一条已发布读数的
  确定顺序键 `(last_observed_at, last_source_id, last_seq)`（首条发布前三者为
  `null`、偏移为 0）；
- 该时刻会话的 `published_watermark`；
- 会话的**发布位置标记**：一个随实际发布单调递增的持久计数（内部字段
  `published_seq`，每确定发布一条读数加一，只在发布所在事务内推进；
  retention 清理永不改写它）。

```bash
curl -s -X POST localhost:8000/sessions/lab-1/checkpoints \
  -H 'Content-Type: application/json' -d '{"name": "consumer-1"}'
```

响应 201。**幂等与冲突**（比较的是检查点记录的标记与当前标记，而不是当前还在
库里的行数/末尾键）：

- 同名重试时**标记未推进**（期间没有任何新发布）→ 幂等返回 200 与首次完全一致的
  结果（`created_at`、位置快照与记录的水位线均不变），**即使检查点覆盖的读数已被
  retention 清理**——幂等判断不依赖这些行是否还在库里；
- 同名重试时**已有新发布**（标记已推进）→ 409，且既有检查点绝不被改写；标记只增
  不减，因此即使新发布的行随后也被清理，409 语义不变。
  注意幂等只取决于**是否有新发布**：水位线可能在没有新读数发布时前移（读数仍停在
  边界外），此时重复创建仍返回首次快照。

会话不存在 → 404；名称非法（空、含空格/斜杠等）或缺字段 → 422。
检查点行一旦写入即为**不可变快照**：后续发布、来源封存乃至会话关闭都不会修改它。

### 16. 列出 / 删除检查点

- `GET /sessions/{session_id}/checkpoints`：列出会话全部检查点（按创建时间、名称排序），
  每项含完整位置与水位线快照；会话不存在 → 404。
- `DELETE /sessions/{session_id}/checkpoints/{name}`：删除一个检查点，成功 → 204；
  检查点不存在 → 404，会话不存在 → 404。删除后该名称可重新创建。

```bash
curl -s localhost:8000/sessions/lab-1/checkpoints
curl -s -X DELETE localhost:8000/sessions/lab-1/checkpoints/consumer-1
```

### 17. 从检查点继续读取

`GET /sessions/{session_id}/checkpoints/{name}/stream?limit=100&cursor=...`

- 沿用“拉取已发布流”的**确定顺序** `(observed_at, source_id, seq)` 与**不透明游标分页**
  （`next_cursor` 原样回传，`null` 取完；`limit` 1～1000；损坏游标 → 400）。
- 不带 `cursor` 时，首页从检查点捕获位置的**严格之后**开始（空流检查点等价于从头读）；
  之后翻页把上一页的 `next_cursor` 原样回传。**游标不得指向检查点位置或其之前**（→ 400）。
- 响应内嵌不可变的 `checkpoint` 快照；顶层 `published_watermark` 是会话**当前**水位线
  （尾流发布后为终止水位线），与检查点记录的快照水位线各自独立。
- 检查点含义不随后续发布或会话关闭改变：尾流发布、会话 `closed` 之后从同一检查点继续读，
  恢复点仍然是创建时的位置，只会多读之后（含尾流）发布的读数；隔离读数永不出现。

```bash
curl -s 'localhost:8000/sessions/lab-1/checkpoints/consumer-1/stream?limit=50'
```

检查点不存在 → 404，会话不存在 → 404。

### 18. 计算释放计划（只读）

`GET /sessions/{session_id}/release-plan?count=N`

为 **open** 会话只读计算：要按全局顺序 `(observed_at, source_id, seq)` 释放最早 N 条
buffered 读数，各来源的事件时钟最少需要推进到哪里。整个计算在**同一个 SQLite 读快照**
内完成（缓冲前缀、各来源时钟与迟到容忍来自同一一致视图），不写任何数据：

- **目标水位线** = 第 N 条 buffered 读数 `observed_at` 的**下一微秒**（发布规则是严格
  小于、存储精度为微秒，这是能放出该读数的最小水位线）；已发布/已隔离读数不参与。
- 各来源所需 effective max `required` = 目标水位线 + `allowed_lateness_seconds`
  （水位线 = 最慢来源时钟 − 迟到容忍，因此每个来源的时钟都必须到达这里）。
- 逐来源返回（按会话声明顺序）：`current`（当前有效时钟，封存来源为其
  `final_observed_at`；从未上报的来源为 `null`）、`required`、`advance_seconds`
  （还需推进的秒数，已达标为 0）与 `blocker`。
- `blocker` 取值：`null` 已达标；`below_required` 时钟不足、按 `advance_seconds`
  推进即可；`not_reported` 尚未首报（`current`/`advance_seconds` 均为 `null`）。
- **已封存来源时钟不足时不得建议推进**：其 `advance_seconds` 为 `null`、`blocker`
  为 `sealed_below_required`，且顶层 `attainable=false`、`reason` 说明原因
  （封存时钟已不可再推进，靠推进时钟无法释放这些读数）。
- `count` 限 1～100；超过当前 buffered 条数 → 422；会话已 `closed` → 409
  （关闭时尾流发布已清空缓冲区）；会话不存在 → 404。

```bash
curl -s 'localhost:8000/sessions/lab-1/release-plan?count=2'
```

```json
{
  "session_id": "lab-1",
  "count": 2,
  "buffered": 4,
  "allowed_lateness_seconds": 10.0,
  "target_watermark": "2026-09-21T10:00:30.000001Z",
  "attainable": true,
  "reason": null,
  "sources": [
    {"source_id": "thermometer-a", "current": "2026-09-21T10:00:40Z",
     "required": "2026-09-21T10:00:40.000001Z", "advance_seconds": 0.000001,
     "blocker": "below_required"},
    {"source_id": "thermometer-b", "current": "2026-09-21T10:01:00Z",
     "required": "2026-09-21T10:00:40.000001Z", "advance_seconds": 0.0,
     "blocker": null}
  ]
}
```

### 19. 检查点安全的存储清理（retention）

`POST /sessions/{session_id}/retention`

回收旧读数占用的存储，同时保证任何检查点的续读语义不受破坏。请求体：

| 字段 | 说明 |
| --- | --- |
| `cutoff` | 事件时间边界（ISO 8601，统一按 UTC 归一化）；只考虑 `observed_at` **严格早于**它的读数 |
| `dry_run` | `true`（默认）只报告数量、不写数据；`false` 在单个写事务内原子删除可删除候选 |

**候选范围**：只有状态为 `published` 与 `quarantined` 的读数参与；`buffered` 读数仍可能被
水位线发布并驱动告警状态机，**永远不是候选、永远不会被删除**。

**检查点保护规则（仅作用于 published 候选）**：检查点续读返回确定顺序键
`(observed_at, source_id, seq)` **严格大于**检查点捕获键的全部已发布读数。因此只需保留
**会话最早检查点**还可能读到的读数。最早保护者**只按不可变的捕获键
`(last_observed_at, last_source_id, last_seq)` 判定**（`name` 仅在同位置的不同检查点间破并），
绝不使用 `stream_offset`：偏移只是检查点创建时“当时还在库里”的已发布读数条数，清理会把旧
检查点已覆盖的行删掉，此后在重新长出来的流上创建的新检查点可能带着**更小**的偏移、捕获键却
更晚；若按偏移选“最早”，就会误删旧检查点仍应续读到的记录。捕获键在检查点创建时冻结、
与之后的删除无关：

- 已发布候选的键**严格大于**最早检查点键 → **保留**（该检查点续读仍会返回它们，更晚的
  检查点自然也被覆盖）；
- 边界键本身（检查点创建时已覆盖的最后一条）及其之前的候选 → 可删除，任何续读都不会
  再返回；
- **会话没有任何检查点** → 没有检查点流可服务，全部 published 候选均可删除
  （`protected_by_checkpoint` 为 `null`）；
- 最早检查点创建于**空流**（捕获键为 `null`，`stream_offset=0`）→ 它在键序上永远最靠前，
  因而始终是保护者，续读从头开始、**保护所有后续** published 读数（即使它的偏移 0 小于
  之后任何检查点的偏移）；
- `quarantined` 候选不属于发布流、检查点永远读不到，**一律可删除**。

响应同时给出总数与分状态明细。`dry_run=true`：

```bash
curl -s -X POST localhost:8000/sessions/lab-1/retention \
  -H 'Content-Type: application/json' -d '{
    "cutoff": "2026-09-21T10:00:35Z",
    "dry_run": true
  }'
```

```json
{
  "session_id": "lab-1",
  "cutoff": "2026-09-21T10:00:35Z",
  "dry_run": true,
  "deleted": 0,
  "protected_by_checkpoint": {
    "checkpoint_name": "consumer-1",
    "stream_offset": 2,
    "last_observed_at": "2026-09-21T10:00:10Z",
    "last_source_id": "thermometer-b",
    "last_seq": 1
  },
  "candidates": 4,
  "retained": 1,
  "deletable": 3,
  "by_status": {
    "published":    {"candidates": 3, "retained": 1, "deletable": 2},
    "quarantined":  {"candidates": 1, "retained": 0, "deletable": 1}
  }
}
```

字段：`candidates` 事件时间严格早于 cutoff 的 published/quarantined 总数；
`retained` 其中因最早检查点仍可能读到而保留的数量；`deletable = candidates - retained`。
`protected_by_checkpoint` 即检查点保护边界（名称、捕获偏移与键）；无检查点时为 `null`，
此时没有任何 published 读数受保护。

确认执行时把 `dry_run` 改为 `false`：隔离与已发布候选的 `DELETE` 与计数在**同一个
`BEGIN IMMEDIATE` 写事务**内完成，`deleted` 返回实际删除行数（等于该次调用的
`deletable`），任何失败都整体回滚、数据不变。

**判重墓碑**：每一条被实际删除的读数，都会在同一事务内向 `retention_tombstones`
写入一条墓碑（`(session_id, source_id, seq)` 主键、规范化 payload 的 SHA-256 摘要、
删除时刻）；dry run 与受检查点保护而保留的读数**不会**产生墓碑。此后上报同 key
读数时，服务在写入前先查墓碑：同值重试计为 `duplicates`（读数不会被恢复、不会重新
进入隔离区、水位线与告警不受影响），异值重试返回 409 且整批回滚——即使该来源已封存
或会话已关闭也是如此。

- **幂等**：相同请求重复提交，候选已在上次删除，第二次返回 `deleted=0`、`deletable=0`，
  且再无副作用（两个相同执行并发也被串行化为“一次删除 + 一次空操作”）；墓碑在后续
  清理中原样保留，重试判重长期有效。
- **语义不变**：清理只删除 `readings` 行并在同事务写入墓碑表；不写 `sessions` /
  `session_sources` / 告警表 / `checkpoints`，因此已发布水位线、有效水位线、来源封存/
  关闭状态、告警当前态与历史事件、检查点不可变快照及续读结果全部保持不变（删除只可能让
  `/stream` 变短，受保护的检查点续读窗口一条不少）。检查点的同名重试幂等同样不受清理
  影响：发布位置标记只随实际发布推进，清理永不回拨（见“创建消费者检查点”）。
- `open` 与 `closed` 会话均可清理；会话不存在 → 404；`cutoff` 非法/为空 → 400/422。

```bash
curl -s -X POST localhost:8000/sessions/lab-1/retention \
  -H 'Content-Type: application/json' -d '{
    "cutoff": "2026-09-21T10:00:35Z",
    "dry_run": false
  }'
```

### 20. 隔离影响预演（只读）

`POST /sessions/{session_id}/quarantine-audit`

对隔离区读数做“**假如这条迟到读数按事件时间插入已发布流**”的影响预演。**完全只读**：
整个计算（隔离行、已发布邻居、计数、检查点）在**同一个 SQLite 读快照**内完成，绝不发布、
隔离、告警或写检查点，重复调用幂等。请求体可全部省略（视为 `{}`），字段均可选：

| 字段 | 说明 |
| --- | --- |
| `source_id` | 只预演该来源的隔离读数；须为会话已声明来源，否则 400 |
| `from` | 事件时间下界（**含等号**，ISO 8601，统一按 UTC 归一化）；`observed_at >= from` |
| `to` | 事件时间上界（**含等号**，同上）；`observed_at <= to`；`from` 晚于 `to` → 400 |
| `limit` | 单页条数，整数 1～1000，默认 100；越界/非整数（含布尔）→ 400 |

对每条匹配的隔离读数，按确定顺序 `(observed_at, source_id, seq)` 在当前已发布流中定位，
返回：

- `insert_position`：该读数将占据的**零基偏移**——键严格小于它的已发布读数条数；
- `stream_size`：同一快照内已发布读数总数；
- `prev_key` / `next_key`：插入位置前后两条已发布邻居的键（`observed_at`/`source_id`/
  `seq`）；位于流首/流尾时相应一侧为 `null`（空流时两者皆 `null`，位置为 0）；
- `gap_prev_seconds` / `gap_next_seconds`：与前后邻居的**事件时间差（秒）**，无邻居时为
  `null`（与前键同刻时前差为 0）；
- `earliest_affected_checkpoint`：**最早受影响的检查点名称**。检查点续读返回其捕获键
  严格之后的已发布读数，因此捕获键严格小于插入键的检查点都会在续读时读到该读数；在这些
  检查点中按**不可变捕获键** `(last_observed_at, last_source_id, last_seq)` 选出最靠前者
  （不按 `stream_offset`——清理后新建检查点的偏移可能更小却更晚）。空流检查点
  （捕获键为 `null`）从头续读，**总会**受影响且永远最靠前。无任何受影响检查点（没有
  检查点，或全部检查点已越过该插入位置）时为 `null`。

结果按 `(observed_at, source_id, seq)` 排序，过滤为闭区间、顺序稳定。顶层还回显归一化后的
`source_id`/`from`/`to` 与生效 `limit`，并给出过滤后、limit 前的 `matched` 总数、本页
`returned` 与是否还有更多的 `has_more`（本接口不提供游标，翻页请收窄 `from`/`to`）。

```bash
curl -s -X POST localhost:8000/sessions/lab-1/quarantine-audit \
  -H 'Content-Type: application/json' -d '{
    "from": "2026-09-21T10:00:00Z",
    "to":   "2026-09-21T10:05:00Z",
    "limit": 50
  }'
```

```json
{
  "session_id": "lab-1",
  "source_id": null,
  "from": "2026-09-21T10:00:00Z",
  "to": "2026-09-21T10:05:00Z",
  "limit": 50,
  "matched": 1,
  "returned": 1,
  "has_more": false,
  "readings": [
    {
      "session_id": "lab-1",
      "source_id": "thermometer-a",
      "seq": 7,
      "observed_at": "2026-09-21T10:00:12Z",
      "value": 29.8,
      "quarantined_watermark": "2026-09-21T10:00:20Z",
      "reason": "behind_published_watermark",
      "insert_position": 3,
      "stream_size": 6,
      "prev_key": {"observed_at": "2026-09-21T10:00:10Z", "source_id": "thermometer-a", "seq": 2},
      "next_key": {"observed_at": "2026-09-21T10:00:15Z", "source_id": "thermometer-b", "seq": 3},
      "gap_prev_seconds": 2.0,
      "gap_next_seconds": 3.0,
      "earliest_affected_checkpoint": "consumer-1"
    }
  ]
}
```

- **严格只读**：调用前后会话状态、`/stream`、`/quarantine`、告警与检查点逐项相同；
  不修改发布流、不改变水位线、不推进任何告警、不写检查点。
- 隔离读数与已发布读数不可能同键（主键 `(session_id, source_id, seq)`），因此每个插入位置
  唯一确定；`open` 与 `closed` 会话均可预演（关闭后针对冻结的已发布流计算）。
- 错误码：会话不存在 → 404；非法 JSON / 请求体不是对象、非法 `from`/`to`、`from` 晚于
  `to`、非法 `limit`、空白或非字符串 `source_id`、未声明来源 → 400。

### 21. 来源缺口扫描（只读）

`GET /sessions/{session_id}/source-gaps?max_gap_seconds=N`

在**同一个 SQLite 读快照**内，把会话的**已发布**读数按来源分组、按事件顺序
`(observed_at, seq)` 扫描，返回每一对**相邻**读数中事件时间差**严格大于**
`max_gap_seconds` 的缺口。`buffered` 与 `quarantined` 读数永不参与（隔离读数不属于已发布
时间线；缓冲读数尚未发布），因此迟到读数不会把一个缺口“劈开”。

查询参数（全部通过 query string 传递）：

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `max_gap_seconds` | 是 | 缺口阈值（秒，有限、非负数）；仅返回间隔**严格大于**它的相邻对，相等不算 |
| `source_id` | 否 | 只扫描该来源；须为会话已声明来源，否则 400 |
| `from` | 否 | 事件时间下界（**含等号**，ISO 8601，统一按 UTC 归一化）；`observed_at >= from` |
| `to` | 否 | 事件时间上界（**含等号**，同上）；`observed_at <= to`；`from` 晚于 `to` → 400 |
| `limit` | 否 | 单页条数，整数 1～1000，默认 100；越界/非整数 → 400 |
| `cursor` | 否 | 上一页返回的不透明 `next_cursor`，原样回传；损坏游标 → 400 |

**区间语义**：相邻对在**落在闭区间内的读数之间**形成——一个缺口若有一端在 `from`/`to`
之外（跨越边界）则不报告，两端都在区间内才报告；`from`/`to` 含等号。

**阈值精度**：阈值按请求中的秒数原值与存储的整数微秒时间线精确比较（不足 1 微秒的
零头只向下取、绝不四舍五入），例如阈值 `0.0000015` 秒时，2 微秒的间隔仍严格大于阈值、
会被报告，而恰好 2 微秒宽的阈值 `0.000002` 则排除同样的间隔。

**排序与分页**：所有来源的缺口按其起始读数的键
`(start.observed_at, source_id, start.seq)` 全局升序排列（同一来源用 `seq` 破并，
跨来源用 `source_id` 破并）。keyset 游标分页保证过滤与分页跨页仍保持这一确定顺序，
响应中的 `next_cursor` 原样回传即可翻页，为 `null` 表示取完。

```bash
curl -s 'localhost:8000/sessions/lab-1/source-gaps?max_gap_seconds=30&source_id=thermometer-a'
```

```json
{
  "session_id": "lab-1",
  "source_id": "thermometer-a",
  "from": null,
  "to": null,
  "max_gap_seconds": 30.0,
  "limit": 100,
  "matched": 1,
  "next_cursor": null,
  "gaps": [
    {
      "source_id": "thermometer-a",
      "start_key": {"observed_at": "2026-09-21T10:00:10Z", "source_id": "thermometer-a", "seq": 2},
      "end_key":   {"observed_at": "2026-09-21T10:00:40Z", "source_id": "thermometer-a", "seq": 3},
      "gap_seconds": 30.0
    }
  ]
}
```

字段：`matched` 为满足过滤与阈值、分页前的缺口总数；`gap_seconds` 为
`end_key.observed_at - start_key.observed_at` 的秒数（存储微秒精度，精确计算、不会舍入）。

- **严格只读**：过滤读数、配对、计数与取页全部在同一个 deferred 读事务（单一一致快照）内
  完成，绝不写库；调用前后会话状态、`/stream`、`/quarantine`、水位线、两类告警与检查点
  逐项相同。`open` 与 `closed` 会话均可扫描（关闭后针对冻结的已发布流计算）。
- 错误码：会话不存在 → 404；缺少/非法 `max_gap_seconds`（空、非数字、NaN/Infinity、负数）、
  非法 `from`/`to`、`from` 晚于 `to`、非法 `limit`、空白 `source_id`、未声明来源、
  损坏游标 → 400。

### 22. 到达延迟剖析（只读）

`GET /sessions/{session_id}/arrival-latency`

在**同一个 SQLite 读快照**内对会话读数做只读的“到达延迟”统计。每条读数的延迟为
**首次接收时刻 − 事件时刻**：

```
latency_seconds = first_received_at - observed_at
```

- `buffered`、`published`、`quarantined` 三种状态的读数**都参与**：它们都记录了真实的
  首次接收时刻（按同一批处理写入的服务端 UTC 时刻）。
- **负延迟照实保留**：若读数时钟超前于服务端（先到达、事件时刻更晚），延迟为负，不做
  绝对值或截断处理；最小值、平均值都可能为负。
- 逐来源返回 `sample_count`、`min_latency_seconds`、`max_latency_seconds` 与
  `avg_latency_seconds`（各条延迟的**算术平均**，内部以整数微秒精确求和再除以样本数）。
- **没有匹配读数的来源不出现在结果中**；`sources` 按 `source_id` **稳定升序**排列。

查询参数（均可选，全部通过 query string 传递）：

| 参数 | 说明 |
| --- | --- |
| `source_id` | 只统计该来源；须为会话已声明来源，否则 400 |
| `from` | 事件时间下界（**含等号**，ISO 8601，统一按 UTC 归一化）；`observed_at >= from` |
| `to` | 事件时间上界（**含等号**，同上）；`observed_at <= to`；`from` 晚于 `to` → 400 |

```bash
curl -s 'localhost:8000/sessions/lab-1/arrival-latency?from=2026-09-21T10:00:00Z&to=2026-09-21T11:00:00Z'
```

```json
{
  "session_id": "lab-1",
  "source_id": null,
  "from": "2026-09-21T10:00:00Z",
  "to": "2026-09-21T11:00:00Z",
  "sample_count": 3,
  "sources": [
    {"source_id": "thermometer-a", "sample_count": 2,
     "min_latency_seconds": -0.5, "max_latency_seconds": 2.5,
     "avg_latency_seconds": 1.0},
    {"source_id": "thermometer-b", "sample_count": 1,
     "min_latency_seconds": 4.0, "max_latency_seconds": 4.0,
     "avg_latency_seconds": 4.0}
  ]
}
```

顶层 `sample_count` 为所有返回来源的样本总数；`from`/`to` 回显归一化后的 UTC 形式
（如 `...T12:00:00+02:00` 回显为 `...T10:00:00Z`）。没有任何读数时 `sources` 为 `[]`、
`sample_count` 为 0。

- **严格只读**：筛选与聚合同一个 deferred 读事务（单一一致快照）内完成，绝不写库；
  其他事务未提交的插入对本次统计不可见。`open` 与 `closed` 会话均可剖析。
- 错误码：会话不存在 → 404；非法 `from`/`to`、`from` 晚于 `to`、空白 `source_id`、
  未声明来源 → 400。

### 23. 阈值暴露时长（只读）

`GET /sessions/{session_id}/exposure`

只看会话内**某个来源**的**已发布**读数，在事件时间**半开区间** `[from, to)` 上计算一条
分段恒定的值时间线，并返回该来源取值**严格高于阈值**的累计秒数、有效采样覆盖秒数与未知
秒数。

查询参数（全部通过 query string 传递）：

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `source_id` | 是 | 只计算该来源；须为会话已声明来源，否则 400 |
| `from` | 是 | 区间起点（**含等号**，ISO 8601，统一按 UTC 归一化） |
| `to` | 是 | 区间终点（**不含**，ISO 8601，同上）；必须严格晚于 `from`，否则 400 |
| `threshold` | 是 | 数值阈值（必须**有限**，NaN/Infinity/非数字/缺失 → 400）；比较为严格 `value > threshold` |
| `value_key` | 否 | 从对象型读数中取该顶层键比较，省略则直接比较读数 `value`；空白 → 400 |

**取值持续（hold）语义**：

- 每条已发布读数的值从其 `observed_at` 起持续，直到该来源**下一条已发布读数**或查询
  终点 `to`；读数恰好在 `to` 时刻时不参与（终点不含）。
- 区间起点 `from` **继承此前最近一条已发布读数**（`observed_at <= from` 中最晚的一条，
  含恰好在 `from` 时刻者）；起点前没有任何读数则 `[from, 下一条读数)` 为**未知**。
- **同一事件时刻的多条读数按 `seq` 顺序生效，以最后一条（seq 最大）为准**；它们只形成
  零长度切换，不重复计时。

**有效覆盖与未知**：

- 有限且非布尔的数值是**有效采样**：严格高于阈值的时间计入 `above_threshold_seconds`，
  其余（恰好等于阈值也算）计入有效但不超阈的覆盖时间；二者之和为 `covered_seconds`。
- **缺值（null）、非有限数值（NaN/Infinity）与布尔值形成未知区间**：从该读数时刻起到
  下一条读数为止都是未知，**不会沿用更早的有效值**；非数值字符串、对象取值缺键同理。
- 起点继承到的最近读数若本身无效，区间起点同样是未知。
- `ratio_above = above_threshold_seconds / covered_seconds`；`covered_seconds` 为 0
  （全程未知）时为 `null`。

**只看已发布流**：`buffered` 与 `quarantined` 读数永不参与，因此隔离读数、尚未发布的
读数既不能拉高/拉低取值，也不会制造未知缺口。全部计算（来源校验、读数读取、分段计时）
在**同一个 deferred 读事务（单一一致快照）**内完成，绝不写库，不推进水位线、不改变告警
与检查点；其他事务未提交的插入对本次计算不可见。`open` 与 `closed` 会话均可计算。

```bash
curl -s 'localhost:8000/sessions/lab-1/exposure?source_id=thermometer-a&from=2026-09-21T10:00:00Z&to=2026-09-21T11:00:00Z&threshold=30'
```

```json
{
  "session_id": "lab-1",
  "source_id": "thermometer-a",
  "from": "2026-09-21T10:00:00Z",
  "to": "2026-09-21T11:00:00Z",
  "threshold": 30.0,
  "value_key": null,
  "above_threshold_seconds": 1820.5,
  "covered_seconds": 3000.0,
  "unknown_seconds": 600.0,
  "ratio_above": 0.6068333333333333
}
```

字段：时长以存储的微秒精度精确累计后换算为秒（浮点输出），故
`covered_seconds + unknown_seconds` 恰好等于区间总秒数。`from`/`to` 回显归一化后的
UTC 形式（如 `...T12:00:00+02:00` 回显为 `...T10:00:00Z`，不带时区的时间按 UTC）。

- 错误码：会话不存在 → 404；缺少/非法 `source_id`、未声明来源、非法 `from`/`to`、
  `from >= to`（空或反向区间）、缺少/非有限 `threshold`、空白 `value_key` → 400。

### 24. 局部尖峰分析（只读）

`GET /sessions/{session_id}/local-spikes`

只看会话内**某个来源**的**已发布**读数，在事件时间**闭区间** `[from, to]`（两端含等号）
内做局部“邻居中位数 + MAD”尖峰检测。`buffered` 与 `quarantined` 读数永不参与。

查询参数（全部通过 query string 传递）：

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `source_id` | 是 | 只分析该来源；须为会话已声明来源，否则 400 |
| `from` | 是 | 区间起点（**含等号**，ISO 8601，统一按 UTC 归一化）；`observed_at >= from` |
| `to` | 是 | 区间终点（**含等号**，同上）；`observed_at <= to`；`from` 晚于 `to` → 400（`from == to` 的点区间合法） |
| `window_seconds` | 是 | 邻居窗口（秒，**严格为正**的有限数；零、负数、NaN/Infinity → 400） |
| `sensitivity` | 是 | 灵敏度系数（**严格为正**的有限数，同上）；判定阈值为 `sensitivity * MAD` |
| `value_key` | 否 | 从对象型读数中取该顶层键；省略则直接比较读数 `value`；空白 → 400 |

**有效读数与稳定排序**：区间内读数按 `(observed_at, seq)` 稳定排序（同一事件时刻按
`seq` 破并）。只有**有限的非布尔数值**参与：给了 `value_key` 时从对象型读数读取该顶层键，
键缺失、值为布尔/字符串/`null`/NaN/Infinity，或读数本身不是带该键的对象，一律跳过；
不给 `value_key` 时读数 `value` 本身须为有限非布尔数值。被跳过的读数既不是候选也不是
邻居。

**邻居与判定**：对每条有效读数，其邻居是**区间内**事件时间距离
`|observed_at_i − observed_at_j| <= window_seconds`（窗口边界**含等号**）的**其他**
有效读数（不含自身）。窗口按请求中的秒数原值与存储的整数微秒时间线精确比较
（不足 1 微秒的零头只向下取、绝不四舍五入），例如窗口 `0.0000015` 秒够不到
2 微秒外的读数，而窗口 `0.000002` 恰好含等号覆盖 2 微秒处。

- 邻居**少于 3 条** → 不判定，计入 `insufficient_neighbors_count`；
- 否则计算邻居取值的中位数 `local_median`，以及邻居对该中位数绝对偏差的中位数
  `MAD = median(|v − local_median|)`（偶数个邻居取中间两值的算术平均）；
- 当且仅当 `|value − local_median| > sensitivity * MAD`（**严格**大于）时标记为尖峰；
  因此 `MAD = 0`（邻居取值完全相同）时，只有偏差严格大于零的读数才会被标记。

**防溢出偶数项平均**：中位数与 MAD 在偶数个取值时需要对中间两值取算术平均，直接做
`(a + b) / 2` 会因中间相加溢出得到 `Infinity`（例如四条邻居均为 `1e308`：中位数本应是
`1e308`，却变成 `Infinity`，继而使 `MAD` 与偏差全部为 `Infinity`，尖峰被漏判且返回值
不是合法的有限 JSON 数值）。计算时优先走有限快路，仅在中间和非有限时改为
`a/2 + b/2`（两有限值的中点必为有限可表示值）；因此凡数学上是有限双精度数的统计量都
以有限数值返回（如该例：候选 `0` 相对四个 `1e308` 邻居被标记，`local_median=1e308`、
`mad=0`、`deviation=1e308`）。奇数个取值的中位数直接取中间值，行为不变。

响应中的 `spikes` 按 `(observed_at, seq)` 稳定排序，每项给出读数键 `key`
（`observed_at`/`source_id`/`seq`）、`value`、`local_median`、`mad`、`deviation`；
顶层给出回显的归一化参数与 `readings_count`（区间内已发布读数总数，先于有效值过滤）、
`valid_count`（有效读数数）、`insufficient_neighbors_count`（邻居不足、未判定的有效读数数）。

**200 条上限**：为保证二次邻居扫描始终廉价，区间内该来源的已发布读数（含随后被跳过的
无效值读数）**超过 200 条**时直接返回 400，请收窄 `from`/`to` 后重试；恰好 200 条允许。

**同一读快照、严格只读**：条数上限检查、读数读取与全部统计都在**同一个 deferred
读事务（单一一致快照）**内完成，绝不写库，不推进水位线、不改变告警与检查点；其他事务
未提交的插入对本次分析不可见。`open` 与 `closed` 会话均可分析。

```bash
curl -s 'localhost:8000/sessions/lab-1/local-spikes?source_id=thermometer-a&from=2026-09-21T10:00:00Z&to=2026-09-21T11:00:00Z&window_seconds=60&sensitivity=3'
```

```json
{
  "session_id": "lab-1",
  "source_id": "thermometer-a",
  "from": "2026-09-21T10:00:00Z",
  "to": "2026-09-21T11:00:00Z",
  "window_seconds": 60.0,
  "sensitivity": 3.0,
  "value_key": null,
  "readings_count": 5,
  "valid_count": 5,
  "insufficient_neighbors_count": 0,
  "spikes": [
    {
      "key": {"observed_at": "2026-09-21T10:00:30Z", "source_id": "thermometer-a", "seq": 4},
      "value": 50.0,
      "local_median": 10.0,
      "mad": 2.0,
      "deviation": 40.0
    }
  ]
}
```

- 错误码：会话不存在 → 404；缺少/非法 `source_id`、未声明来源、非法 `from`/`to`、
  `from` 晚于 `to`、缺少/非正或非有限的 `window_seconds`/`sensitivity`、空白
  `value_key`、区间内已发布读数超过 200 条 → 400。

## 数据库迁移

建表采用“**幂等基础 schema + 版本化迁移**”：应用启动（FastAPI lifespan）时自动创建
基础表（`CREATE TABLE IF NOT EXISTS`），并按 `schema_migrations` 表记录的版本号依次执行
尚未应用的迁移；每个迁移与其版本登记在同一个 `BEGIN IMMEDIATE` 事务内提交。
因此老版本数据库会在下次启动时**原地升级、不丢数据**，新数据库则直接建到最新版本，
无需手工初始化。当前迁移版本：

- **v1**：跨来源复合告警的三张表（`correlated_rules` / `correlated_conditions` /
  `correlated_events`）。
- **v2**：有限采集会话封存——`sessions` 增加 `status`（`open`/`closed`）与 `closed_at`，
  `session_sources` 增加 `final_observed_at` 与 `sealed_at`（老库升级后既有会话为
  `open`、各来源未封存，行为不变）。
- **v3**：发布流消费者检查点——新增 `checkpoints` 表（`(session_id, name)` 主键），
  保存创建时同一快照内的发布流位置（`stream_offset` 与最后一条已发布读数的
  `(last_observed_at, last_source_id, last_seq)` 键）和 `published_watermark`；
  检查点行不可变，后续发布/封存/关闭均不改写它。
- **v4**：清理判重墓碑——新增 `retention_tombstones` 表（`(session_id, source_id, seq)`
  主键），保存每条被 retention 实际删除读数的规范化 payload 摘要（SHA-256）与删除时刻；
  墓碑与删除在同一事务内提交，上报据此在写入前判重（同值 → `duplicates`，异值 → 409），
  老库启动时自动补齐该表、数据不变。
- **v5**：检查点幂等的发布位置标记——`sessions` 增加 `published_seq`（随实际发布单调
  递增、只在发布所在事务内推进、retention 清理永不改写），`checkpoints` 增加同名列，
  在创建检查点的同一事务内记录该标记；同名重试改为比较标记而非当前行数/末尾键，因此
  检查点覆盖的读数被清理删除后，没有新发布的重试仍幂等返回原检查点，有新发布则仍 409。
  迁移为既有会话与检查点**回填**标记：会话取当前已发布行数，捕获键不在当前流末尾之前
  （或流为空）的检查点取同一标记（重试保持幂等），其余检查点取严格更小的值（重试保持
  409）；全部既有检查点行与续读边界原样保留，老库启动时自动升级、数据不变。

## 端到端示例（来源封存与尾流发布）

两个温度计，迟到容忍 10s。封存前后读数的命运：

| 时刻/动作 | 结果 |
| --- | --- |
| A 上报 seq=1@:00、seq=2@:10；B 上报 seq=1@:20 | WM=:00，三条全部 buffered |
| 封存 A `final_observed_at=:10` | A 封存；最慢来源仍为 A@:10，WM=:00，会话保持 `open` |
| A 再上报 seq=3（任意时间） | **409**（新 key 拒绝）；重试 seq=2 同 payload → `duplicates=1` |
| 再次封存 A 同值 :10 | 幂等 200，`sealed_at` 不变；改值封存 → **409** |
| 封存 B `final_observed_at=:20`（最后一个来源） | 同一事务尾流发布：终止水位线 :20.000001，三条 buffered 全部按 `(observed_at, source_id, seq)` 发布；两类告警推进；会话 **closed** |
| close 后上报 B seq=2 | **409 closed**；重试 B seq=1 同 payload → 仍计 `duplicates` |

封存非最后一个来源时水位线完全沿用普通语义：例如上例若 B 在 A 封存后先把自己的
`final_observed_at` 声明到 :40，则最慢来源变为 A@:10、WM 推进到 :00:00，seq=1@:00
会在封存 B 的请求中被正常发布，而无需等待会话关闭。

## 端到端示例（滞回 + 连续次数）

规则：`above`，进入阈值 30、恢复阈值 25，连续进入 3 次、连续恢复 2 次。
温度计 A 的读数只有在被水位线确定发布后才被消费：

| 已发布读数 | 计数/状态 |
| --- | --- |
| 31, 31 | 越限计数 2，仍 normal |
| 28（滞回带内：25 < 28 ≤ 30） | 越限计数清零，仍 normal |
| 31, 31, 31 | 第 3 次连续越限 → **entered**，转 active |
| 24 | 恢复计数 1 |
| 27 | 仍在滞回带内，恢复计数清零，保持 active |
| 24, 23 | 连续 2 次严格低于 25 → **recovered**，转 normal |

期间任何因迟到而被隔离的高温读数都不会触发或延长告警。

## 端到端示例（跨来源同时越限）

复合规则：两个来源都 `above`，A 阈值 30/窗口 15s，B 阈值 28/窗口 10s。
读数在被水位线确定发布后按归并顺序逐条驱动规则：

| 已发布归并流（事件时间） | 规则状态 |
| --- | --- |
| A@:00 = 31（命中 A） | `waiting`（B 尚无命中） |
| B@:05 = 29（命中 B；A 命中年龄 5s ≤ 15s，B 新鲜） | → **correlated_entered**，锚定 B@:05 |
| A@:12 = 31、B@:13 = 29 | 仍 `correlated`，**不重复事件** |
| C@:20 = 0（C 不参与规则，只推进事件时间） | B 命中年龄 15s > 10s → 静默回 `waiting` |
| A@:21 = 31、B@:21 = 29 | 两个条件同时在窗内 → 再次 **correlated_entered** |

期间任何因迟到而被隔离的读数都不会参与复合或使命中过期。

## 项目结构

```
.
├── app/
│   ├── main.py          # FastAPI 路由、异常映射
│   ├── service.py       # 水位线计算 / 发布 / 隔离 / 幂等等核心逻辑
│   ├── alerts.py        # 单来源告警状态机、规则创建（回放）、当前/历史查询
│   ├── correlated.py    # 跨来源复合规则、窗口命中维护、回放、当前/历史查询
│   ├── checkpoints.py   # 发布流消费者检查点：创建（同快照位置+水位线+发布标记）/列表/删除/继续读取
│   ├── release_plan.py  # 释放计划：同一读快照内计算放出最早 N 条 buffered 读数的最小时钟推进
│   ├── retention.py     # 检查点安全清理：dry_run 报告/单事务原子删除 cutoff 之前的已发布与隔离读数
│   ├── quarantine_audit.py # 隔离影响预演：同一读快照内计算隔离读数插入已发布流的位置/邻居/检查点影响
│   ├── source_gaps.py   # 来源缺口扫描：同一读快照内按来源找已发布相邻读数的超阈缺口（过滤+游标分页）
│   ├── arrival_latency.py # 到达延迟剖析：同一读快照内按来源统计 first_received_at - observed_at 的样本数/min/max/avg
│   ├── exposure.py       # 阈值暴露时长：同一读快照内计算来源在 [from,to) 严格高于阈值/有效覆盖/未知的秒数
│   ├── local_spikes.py   # 局部尖峰：同一读快照内按闭区间与事件时间窗口用邻居中位数+MAD 严格判定尖峰（上限 200 条）
│   ├── errors.py        # 400/404/409/422 业务异常
│   ├── cursor.py        # 不透明 keyset 分页游标
│   ├── database.py      # SQLite schema、版本化迁移、连接、BEGIN IMMEDIATE 事务
│   ├── models.py        # Pydantic 请求/响应模型
│   ├── time_utils.py    # ISO 8601 解析与 UTC 规范化
│   └── config.py        # 环境变量配置
├── tests/
│   ├── test_api.py          # 时间线/隔离/幂等等端到端行为测试（22 个）
│   ├── test_alerts.py       # 单来源告警状态机/滞回/连续次数/隔离/回放/分页测试（27 个）
│   ├── test_correlated.py   # 跨来源复合/窗口/过期/再触发/隔离/回放/迁移测试（35 个）
│   ├── test_sealing.py      # 来源封存/幂等冲突/尾流发布/关闭/封存-上报并发测试（25 个）
│   ├── test_checkpoints.py  # 检查点创建/幂等（含清理交错）/409/继续读取分页/不可变/迁移测试（45 个）
│   ├── test_release_plan.py # 释放计划目标水位线/逐来源推进/封存不可达/422/409/并发只读测试（19 个）
│   ├── test_retention.py    # 检查点安全清理：dry_run/保护边界/跨轮清理偏移倒挂/原子删除/幂等/不改变既有语义测试（31 个）
│   ├── test_retention_retry.py # 清理后重试判重：墓碑判重/异值 409/整批回滚/封存关闭后重试/不推进水位线告警/检查点不变/迁移测试（17 个）
│   ├── test_quarantine_audit.py # 隔离影响预演：插入位置/邻居时间差/检查点影响/跨轮清理/过滤/400/只读测试（45 个）
│   └── test_source_gaps.py # 来源缺口扫描：相邻配对/严格阈值/微秒精度/闭区间/游标分页/400/只读测试（30 个）
│   └── test_arrival_latency.py # 到达延迟剖析：逐来源统计/负延迟/微秒精度/闭区间/空来源/400/只读/单快照测试（19 个）
│   └── test_exposure.py   # 阈值暴露时长：持有语义/起点继承/同刻 seq/未知区间/value_key/只看已发布/400/只读/单快照测试（47 个）
│   └── test_local_spikes.py # 局部尖峰：中位数/MAD/严格判定/MAD 为零/邻居窗口与下限/闭区间/value_key/200 上限/400/只读/单快照/偶数项中位数与 MAD 防溢出（1e308）测试（60 个）
│   └── test_nonfinite_integers.py # 无法表示为有限浮点的整数（10**400）：正常保存/发布/判重、单来源打断连续计数、跨来源不计命中且窗口照常过期、两类规则晚建回放、尾流封存、exposure/local-spikes 边界测试（16 个）
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```
