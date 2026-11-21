# 砺川原始波形交换站

砺川材料可靠性实验室的仪器员导入疲劳试验波形，质控员从封定清册选择补送原件，资料员签发，隔离站独立验收，夜班重开两站数据库复领历史。系统按原号与出处区分正文相同的试验记录，并在导入时拦截截帧。

## 运行

使用 Python 3.11 标准库及其 SQLite。请从项目根目录执行。

```sh
python3 -B check_project.py
python3 -B wave_station.py demo
```

标准回归入口随项目物化提供。演示在临时目录建立两个站库，从 `examples/shift.json` 导入 R20、R03，保持采集原序。两个峰值分别为 32768 和 256 个仪器计数。演示按峰值选择 R20，签发后关闭源库，隔离站依据到件复算。随后重开两站并核对首执、原件和实收。

可在项目下保存演示库，或物化一份已有签发历史的 V1 样例库。

```sh
python3 -B wave_station.py init-demo .demo
python3 -B wave_station.py restore-v1 .demo-v1
```

两条命令分别创建 `source.sqlite3` 和 `receiver.sqlite3`。样例写入新建源库。再次执行同一命令会补建缺失表，保留原有数据和用户通过接口提交的内容。旧库样例以 `examples/v1.sql` 存放，物化后可用 `receipt` 查询签发号 `P00000001`，或用申请号 `legacy-request` 重投。站点连接使用默认元组行格式，由同一调用者顺序使用，业务入口自行管理其写事务。用 `contextlib.closing` 或显式 `close()` 结束连接。

## 公开 Python 接口

从 `wave_station` 导入以下函数。`connect(path)` 打开一个数据库。首次使用时分别调用 `initialize_source(source)` 与 `initialize_receiver(receiver)` 创建站内表。每个业务函数的首个参数为该站的连接。

| 接口 | 输入与成功结果 | 拒绝条件 |
| --- | --- | --- |
| `ingest(source, batch_id, frames)` | 有序的 `(record_id, origin, bytes)` 序列或迭代器，返回 `RawBatch` | 非法帧、空原号或出处、批内重号、同批号异内容或异序 |
| `seal(source, batch_id)` | 冻结导入批，返回 `SealedManifest`，可重投 | 未知导入号或非法原帧 |
| `manifest(source, batch_id)` | 返回持久原序的清册及批次摘要 | 未知封定号 |
| `request(source, batch_id, request_id, record_id)` | 登记一个明确原号，返回带待签或已签状态的 `OrdinaryRequest` | 批外指定、未知原号、同申请号改绑、申请号已用作预约 |
| `publish(source, request_id)` | 签出指定原据并返回首次 `Receipt`，重投复用原首执 | 未知申请、原件偏离清册、核销失利、提交失败、取件会破坏同批未签预约的可兑现性 |
| `reserve(source, batch_id, order_id, eligible_ids)` | 按候选集合登记补送预约，返回 `reserved`；不可共同兑现或候选非法时不登记 | 非法候选、空集合、批外指定、未知封定批、同号异绑或用作普通申请 |
| `issue_reserved(source, batch_id, order_id, *, before_commit=None)` | 办理获准的预约，从候选中签出保留余单可兑现性的一件，返回 `Receipt` | 未预约或他批单号、不可兑现、提交失败 |
| `receipt(source, publication_id)` | 返回含 `receipt` 和原 `package` 字节的 `Publication` | 未知签发号 |
| `accept(receiver, package)` | 独立验值并返回 `AcceptedRecord`，同签发号同件重投复用 | 格式或校验错误、身份矛盾、同签发号绑定相异交换件字节 |
| `accepted(receiver, acceptance_id)` | 返回原实收记录及交换件 | 未知实收号 |

`InputError` 表示输入格式不符，`NotFound` 表示相应身份缺失，`Conflict` 表示绑定冲突或核销失利，`IntegrityFailure` 表示原件或交换件与记录的证据矛盾。SQLite 写入或提交错误向调用方报告。测试可向 `publish` 或 `issue_reserved` 传入关键字参数 `before_commit`，其无参回调在业务写入后、提交前调用。回调抛出异常会回滚核销、首执、交换件和单号已签状态。源站单批原号的唯一性由批内核销键维护。站内签发号与实收号各自在相应数据库中分配，已提交的号在重投和重开后保持。

预约与普通申请共用申请号空间，两类单据的号在各自表中登记；同一号用于另一类单据即 `Conflict`。`reserve` 的候选按集合比较，输入顺序与重复无影响；同一号、同批、同集合的重投（含已签单）复用并返回 `reserved`。接单时对同批所有未签预约做二部匹配，确保每单可分得不同的未核销原件，否则返回 `unavailable` 且不登记。`issue_reserved` 只在取件后余单仍可共同兑现的候选里选择，签发号、交换件与核销范围与普通签发共用。旧版库的 `consumed_records` 与 `publications` 会在初始化时去掉指向 `ordinary_requests` 的外键，其余约束与历史行保持不变。

## 波形和证据

每两个原字节构成一个小端有符号 16 位样本。帧须有正偶数个字节，批可以为空。封定逐样本取整数绝对值再求最大值，`-32768` 的绝对振幅为 `32768`。原号和出处保留输入的有效文本原表示。清册记录从零开始的采集位置及帧的原号、出处、字节长度、SHA-256 摘要、峰值。批次摘要取自批号和有序清册的规范 UTF-8 JSON。

普通交换件使用 UTF-8 JSON 和 Base64 波形字节，携带签发身份、申请身份、所选封定条目及源站批次摘要。预约签发的 `request_id` 即预约单号。隔离站核对件头与条目的批号、原号和出处，实算所收字节的长度、SHA-256 与整数峰值。整批摘要随件保存为源站的封定声明。实收记录保存所核验的原交换件字节及实际峰值，复领沿用首次提交内容。
