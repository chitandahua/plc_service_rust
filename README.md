# plc_service_rust (plc_service)

PLC(电力线载波)采集服务。通过串口(或 TCP)挂接 HPLC 本地通信模块,以 DLT645 风格帧协议对电表进行初始化、抄表、路由、控制、升级等操作,并通过 **MQTT** 与上层主站对接。程序名为 `PLCServiceGW`,是 C++ 版 `PLCService` 的 Rust 重写。

## 功能特性

- **本地通信**:经串口(`/dev/plctty`)或 TCP 与 HPLC 模块通信,支持多地址/多从节点。
- **MQTT 对接**:订阅/发布协议主题,完成主站下行指令与上行数据、事件、应答的透传。
- **业务服务**:模块信息、电表信息/状态、主站地址、节点管理、路由控制、并发抄表、控制命令、数据透传、事件上报、文件升级、区域识别、HPLC 信息、广播、监控节点、调试方法等。
- **并发抄表**:多表并发调度(默认 128 并发、32 地址/批),带队列老化与状态管理。
- **初始化流程**:上电后自动完成模块初始化、电表建档,失败自动恢复(`resume_interval`)。
- **设备保活**:PLC 设备心跳与连续超时计数,断线/离线检测。
- **配置与校验**:JSON 配置(串口、MQTT、设备、抄表参数),`jsonschema` 对下行报文做 Schema 校验。
- **SQLite**:节点/设备数据持久化(rusqlite)。
- **日志与运维**:`tracing` 日志,支持 syslog 输出;编译期版本信息。

## 架构设计

### 数据流

```
主站 ──MQTT──▶ MqttClient ─▶ MqttMsgHandler ─▶ ModuleService(业务处理)
                  ▲                                  │
                  │                       组帧(protocol/frame) 
              MqttMsgHandler ◀─────── uart/timeout ──▼
                  ▲                             │
   UartAgent(读/写/超时) ◀── mpsc 通道 ── UartMsgHandler / UartTimeoutHandler
                  ▲
                  ▼
         SerialPort / TCP ◀─── HPLC 模块/电表
```

### 线程/actor 划分

`PlcService::run`(src/lib.rs:86)创建 4 条 `mpsc` 通道(`uart_msg` / `concurrent_msg` / `mqtt_msg` / `msg`)连接各 actor 线程:

| Actor | 职责 |
|-------|------|
| `MqttClient` / `Handler` | MQTT 连接、订阅主题、回调分发 |
| `MqttMsgHandler` | 下行指令解析与业务服务注册分发,上行应答封装 |
| `UartAgent` | 串口/TCP 读写,请求-响应匹配,读线程+写线程+超时线程,`Condvar` 协调 |
| `UartMsgHandler` | UART 上行帧解析,派发到对应服务;维护电表并发状态 |
| `UartTimeoutHandler` | 请求超时处理,超时计数、并发请求清理 |
| `PlcInit` | 初始化流程(模块初始化/电表建档) |
| `PlcDevice` | 设备级心跳与连续超时统计,超限上报 |
| `DeviceInfo` | 设备信息上报,更新主站地址 |

### 协议层(src/protocol)

- **frame**:`68 <length:2B LE> <ctrl:1B> <address> <info_field> <user_data> <checksum> 16` 帧构造与解析,含 Dir(Down/Up)、PRM(Master/Slave)、AFN 应用功能码,支持流式 `Cursor` 解析。
- **app_data**:各 AFN 的业务数据区(active_report、answer、ctrl_cmd、data_trans、file_transfer、init、meter_ctrl、meter_reading、query_data、route_data_forward/read/get/set 等),实现 Confirm/Deny 应答。
- **user_data / info_field**:用户数据与信息字段编码。

### 服务层(src/service)

`ModuleService`(src/service/mod.rs:74)聚合全部业务模块并在 `init` 中注册到 `MqttMsgHandler`:module_info、master_address、node_manage、concurrent_meter、hplc_info、device_info、monitor_node、route_ctrl、broadcast、data_transfer、meter_state、identify_area、control_cmd、file_upgrade 等。

**应答封装**:`IntoMqttMessage` trait + `impl_into_mqtt_message!(.., nested|flat)` 宏统一把业务结果/错误转为 MQTT 上行报文,错误走 `Status::Failure` + reason。

## 技术栈与技术点

| 技术点 | 说明 |
|--------|------|
| Rust 2021,std 线程模型 | 基于 `std::thread` + `mpsc` 通道 + `timer` 定时器,非 async 运行时 |
| `paho-mqtt`(bundled) | MQTT 客户端(C 库捆绑编译),`paho_mqtt_c` 底层回调 |
| `serialport` + `nix`(ioctl) | 串口读写与 tty ioctl 控制;可选 TCP(`--tcp-addr`) |
| 请求-响应匹配 | `UartAgent` 以 `Condvar` + 共享 `ReqInfo` 实现同步等待与并发请求表 |
| 并发抄表 | 128 并发限流,32 地址/批,队列缓存 + 老化时间,`AtomicBool` 抄表状态机 |
| `jsonschema` | 下行报文 Schema 校验(`schema_check.rs`) |
| `rusqlite`(SQLite) | 节点/设备持久化 |
| `clap` | CLI:日志级别、syslog、超时/并发参数、TCP 模式、`--ver` 版本 |
| `tracing` / `tracing-subscriber` / `syslog-tracing` | 日志与 syslog 输出 |
| `vergen` / `compile-time` | 编译期注入版本与构建时间 |
| `threadpool` | 服务线程池(默认 2) |
| `num_enum` / `strum` / `thiserror` / `anyhow` / `chrono` / `hex` | 枚举转换、错误、时间、十六进制等 |

## 配置

| 配置文件 | 说明 |
|----------|------|
| `com_setting.json` | 串口参数(波特率/校验/停止位/数据位) |
| `plc_device.json` | PLC 设备端口(默认 `/dev/plc`) |
| `mqtt_server.json` | MQTT 服务器地址/端口/账号 |
| `meter_config.json` | 初始化超时、串口超时、并发抄表参数、恢复间隔 |

## 构建与运行

```bash
cargo build --release        # 依赖 paho-mqtt bundled 编译,需 C 编译器
./target/release/plc_service --help
./target/release/plc_service --ver                 # 打印版本/编译时间
./target/release/plc_service --log-level debug
./target/release/plc_service --syslog              # 输出到 syslog
```

## 关键流程

1. **启动**:读配置 → 初始化 MQTT/UART/服务模块 → 启动各 actor 线程 → 上报设备信息 → 更新主站地址 → 进入 PLC 初始化。
2. **下行**:主站 MQTT 报文 → `MqttMsgHandler`(Schema 校验)→ 业务服务组帧 → 投递 `uart_msg` → `UartAgent` 写串口并登记请求。
3. **上行**:串口读到帧 → `UartMsgHandler` 解析并匹配请求 → 业务处理 → `IntoMqttMessage` 封装 → 经 `mqtt_msg` 上发主站。
4. **超时**:请求超时由 `UartTimeoutHandler` 清理,连续超时累积上报;初始化失败按 `resume_interval` 重试。
