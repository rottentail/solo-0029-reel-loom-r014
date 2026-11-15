# 声画织场 · Reel Loom

Python 音视频制作后端：将上传的自包含媒体编成有修订历史的剪辑卷，冻结处理参数生成可复听成片，再打包为交付印本。支持 macOS/Linux、Python 3.11+。本机验收为 macOS ARM64、Python 3.14、PyAV 16.1 与 FFmpeg 7.1。

## 启动

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/python -m reel_loom.server
```

服务入口 `http://127.0.0.1:8729/loom/v1/`。依赖包 imageio-ffmpeg 提供可执行文件；可用 `LOOM_FFMPEG` 指定系统 FFmpeg。运行时不需要在线服务。数据库与媒体默认写入 `.reel-loom/`，用 `LOOM_DATA` 改变存储位置；`LOOM_PORT`、`LOOM_HOST`、`LOOM_WORKERS` 控制监听与 1–4 个编码线程，`LOOM_LEASE_SECONDS` 控制远端执行租约时长（默认 15 秒）。每个存储位置只允许一个服务进程，启动锁防止把另一服务的运行任务误判为中断。

## 远端执行 worker

默认启动即可单机使用：服务进程内的编码线程直接领取渲染单，无需任何额外组件。需要把真实音视频渲染分给多台机器时，在每台执行机上运行：

```bash
.venv/bin/python -m reel_loom.worker --server http://127.0.0.1:8729/loom/v1 --label studio-a
```

worker 通过 HTTP 注册、领取渲染单、按冻结快照下载源内容（`GET /blobs/{sha256}`，摘要校验）、用本机 FFmpeg 编码并回传成片；编码期间按租约心跳续期并观察取消。worker 只接触冻结快照与内容寻址字节，不会重新读取当前稿，已退役来源的冻结引用仍可渲染。租约到期未续期时，其他 worker（或本机编码线程）可接力同一渲染单并递增尝试次数；同一渲染单只承认当前租约的提交，迟到结果返回 409 且不改写既有成片、任务详情与印本事实。服务重启不清除未过期的远端租约；本机线程的运行状态仍在重启时按中断恢复。制作人员的渲染单、矩阵与印本入口不变。

当前服务用于可信制作网络，未提供身份认证。默认只监听回环地址。单素材 128 MiB、最长 3600 秒、最大 3840×2160，最多 64 个镜段；同步分析与编码有容量和超时限制。上传原始媒体字节；不接受远端地址或流式播放清单。元信息和成片均来自真实媒体工具。

## 一条完整制作流程

可直接运行有界示例，它生成三秒色条与音调，经临时 HTTP 服务上传、编排和渲染，打印结果后清理其专属临时存储：

```bash
.venv/bin/python -m reel_loom.demo --workflow render
.venv/bin/python -m reel_loom.demo --workflow imprint
```

生产集成可使用 `reel_loom.client.Client`，或直接请求 API（路径均相对 `/loom/v1`）：

| 操作 | 入口及请求 |
| --- | --- |
| 接收媒体 | `POST /assets`，原始字节；`X-Media-Label` 为 ASCII 标签 |
| 建剪辑卷 | `POST /reels`，`{"title":"潮声短片","slices":[{"asset":"asset_…","in":0.2,"out":2.5}]}` |
| 改稿 | `PUT /reels/{id}`，相同文档结构，`If-Match: 1`；旧版本 `GET ?revision=1` |
| 封卷 | `POST /reels/{id}/seal`，`If-Match` 指定头修订 |
| 版式 | `PUT /looks/{id}`，`{"kind":"video","width":640,"height":360,"fit":"contain"}`；更新需 `If-Match` |
| 编码 | `POST /tickets`，`{"reel":"reel_…","look":"preview"}`；可指定 `revision`、`look_version` |
| 渲染矩阵 | `POST /matrices`，`{"reel":"reel_…","looks":[{"id":"preview"},{"id":"listening"}]}` |
| 状态/取消/重试 | `GET /tickets/{id}`，`POST /tickets/{id}/cancel`、`retry` |
| 注册 worker | `POST /workers`，`{"label":"studio-a"}`；`GET /workers` 查看 |
| 领取/心跳 | `POST /workers/{id}/claims`，`POST /claims/{id}/heartbeat`，`{"progress":0.4}` |
| 回传/失败 | `POST /claims/{id}/result` 原始媒体字节；`POST /claims/{id}/fail`，`{"error":"encoder_failed"}` |
| 音频测量 | `POST /assets/{id}/signal`、`waveform`、`loudness`，默认请求 `{}` |
| 画面索引 | `POST /assets/{id}/contact-sheet`，`{"count":9,"columns":3}` |
| 导入字幕 | `POST /assets/{id}/cues`，`{"language":"zh-CN","srt":"…"}` |
| 映射字幕 | `POST /reels/{id}/captions`，`{"revision":1,"tracks":["cue_…"]}` |
| 成片验规 | `GET /artifacts/{id}/conformance`，真实全解码 |
| 血缘 | `GET /tickets/{id}/lineage` |
| 交付印本 | `POST /tickets/{id}/imprints`，`{"cue_tracks":[]}`；`GET /imprints/{id}/verify` |
| 下载 | `GET /artifacts/{id}/content`、`/imprints/{id}/content` 或 `/blobs/{sha256}`，支持单一 byte range 与 ETag |
| 资源 | `POST /assets/{id}/retire`、`restore`；`GET /resources`；`POST /resources/sweep`，默认 `dry_run:true` |

`Idempotency-Key` 可用于创建剪辑卷、渲染单、渲染矩阵；重放返回最初创建响应，当前状态需 GET 回读。不同内容复用同一键返回 409。所有新引用要求 ready 来源；已冻结任务可以继续读退役来源。未完成渲染不能打包。取消运行单是请求，最终状态以 GET 为准；提交成片与成功状态在同一 SQLite 事务中。远端提交的成片同样先落盘核验再登记，租约失效或已取消的提交返回 409 并计入任务事件。

视频版式输出 H.264/AAC MP4，音频版式输出 PCM WAV；版式支持 1/2 声道、采样率、常量增益、contain/cover 和恒定帧率。剪辑卷当前采用顺序硬切，不具备多轨、转场、变速、字幕烧录或分段流能力。字幕保留源时间并在投影时按剪辑区间裁切；字幕轨创建后不可变。signal 输出 dBFS，loudness 才是 EBU R128，二者不可互换。

## 构建与生产流程校验

```bash
.venv/bin/python -m compileall -q reel_loom
.venv/bin/python -m reel_loom.demo --workflow conformance
.venv/bin/python -m reel_loom.demo --workflow remote-relay
```

全部 17 个有界流程记录在 `.project-runtime.json`，每条都经过真实领域与持久化路径，并观察成功或拒绝结果。`remote-relay` 启动两个真实 worker 进程演示领取、失联接力与迟到提交。测试延后：基线不包含单元/集成测试文件、不声明 test_command；这些生产示例不是完整测试保障。后续质量保障任务可从这些入口补建测试。

源映射见 `.task-source.json`，业务边界见 `PROJECT_SPEC.md`。题库和内部难度证据在仓库外独立维护，不属于基线运行依赖。
