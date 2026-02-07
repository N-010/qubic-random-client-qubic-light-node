# Rust/codex-rs

In the codex-rs folder where the rust code lives:

- Crate names are prefixed with `codex-`. For example, the `core` folder's crate is named `codex-core`
- When using format! and you can inline variables into {}, always do that.
- Install any commands the repo relies on (for example `just`, `rg`, or `cargo-insta`) if they aren't already available before running instructions here.
- Never add or modify any code related to `CODEX_SANDBOX_NETWORK_DISABLED_ENV_VAR` or `CODEX_SANDBOX_ENV_VAR`.
    - You operate in a sandbox where `CODEX_SANDBOX_NETWORK_DISABLED=1` will be set whenever you use the `shell` tool. Any existing code that uses `CODEX_SANDBOX_NETWORK_DISABLED_ENV_VAR` was authored with this fact in mind. It is often used to early exit out of tests that the author knew you would not be able to run given your sandbox limitations.
    - Similarly, when you spawn a process using Seatbelt (`/usr/bin/sandbox-exec`), `CODEX_SANDBOX=seatbelt` will be set on the child process. Integration tests that want to run Seatbelt themselves cannot be run under Seatbelt, so checks for `CODEX_SANDBOX=seatbelt` are also often used to early exit out of tests, as appropriate.
- Always collapse if statements per https://rust-lang.github.io/rust-clippy/master/index.html#collapsible_if
- Always inline format! args when possible per https://rust-lang.github.io/rust-clippy/master/index.html#uninlined_format_args
- Use method references over closures when possible per https://rust-lang.github.io/rust-clippy/master/index.html#redundant_closure_for_method_calls
- When possible, make `match` statements exhaustive and avoid wildcard arms.
- When writing tests, prefer comparing the equality of entire objects over fields one by one.
- When making a change that adds or changes an API, ensure that the documentation in the `docs/` folder is up to date if applicable.
- If you change `ConfigToml` or nested config types, run `just write-config-schema` to update `codex-rs/core/config.schema.json`.

Run `just fmt` (in `codex-rs` directory) automatically after you have finished making Rust code changes; do not ask for approval to run it. Additionally, run the tests:

1. Run the test for the specific project that was changed. For example, if changes were made in `codex-rs/tui`, run `cargo test -p codex-tui`.
2. Once those pass, if any changes were made in common, core, or protocol, run the complete test suite with `cargo test --all-features`. project-specific or individual tests can be run without asking the user, but do ask the user before running the complete test suite.

Before finalizing a large change to `codex-rs`, run `just fix -p <project>` (in `codex-rs` directory) to fix any linter issues in the code. Prefer scoping with `-p` to avoid slow workspace‑wide Clippy builds; only run `just fix` without `-p` if you changed shared crates.

## TUI style conventions

See `codex-rs/tui/styles.md`.

## TUI code conventions

- Use concise styling helpers from ratatui’s Stylize trait.
    - Basic spans: use "text".into()
    - Styled spans: use "text".red(), "text".green(), "text".magenta(), "text".dim(), etc.
    - Prefer these over constructing styles with `Span::styled` and `Style` directly.
    - Example: patch summary file lines
        - Desired: vec!["  └ ".into(), "M".red(), " ".dim(), "tui/src/app.rs".dim()]

### TUI Styling (ratatui)

- Prefer Stylize helpers: use "text".dim(), .bold(), .cyan(), .italic(), .underlined() instead of manual Style where possible.
- Prefer simple conversions: use "text".into() for spans and vec![…].into() for lines; when inference is ambiguous (e.g., Paragraph::new/Cell::from), use Line::from(spans) or Span::from(text).
- Computed styles: if the Style is computed at runtime, using `Span::styled` is OK (`Span::from(text).set_style(style)` is also acceptable).
- Avoid hardcoded white: do not use `.white()`; prefer the default foreground (no color).
- Chaining: combine helpers by chaining for readability (e.g., url.cyan().underlined()).
- Single items: prefer "text".into(); use Line::from(text) or Span::from(text) only when the target type isn’t obvious from context, or when using .into() would require extra type annotations.
- Building lines: use vec![…].into() to construct a Line when the target type is obvious and no extra type annotations are needed; otherwise use Line::from(vec![…]).
- Avoid churn: don’t refactor between equivalent forms (Span::styled ↔ set_style, Line::from ↔ .into()) without a clear readability or functional gain; follow file‑local conventions and do not introduce type annotations solely to satisfy .into().
- Compactness: prefer the form that stays on one line after rustfmt; if only one of Line::from(vec![…]) or vec![…].into() avoids wrapping, choose that. If both wrap, pick the one with fewer wrapped lines.

### Text wrapping

- Always use textwrap::wrap to wrap plain strings.
- If you have a ratatui Line and you want to wrap it, use the helpers in tui/src/wrapping.rs, e.g. word_wrap_lines / word_wrap_line.
- If you need to indent wrapped lines, use the initial_indent / subsequent_indent options from RtOptions if you can, rather than writing custom logic.
- If you have a list of lines and you need to prefix them all with some prefix (optionally different on the first vs subsequent lines), use the `prefix_lines` helper from line_utils.

## Tests

### Snapshot tests

This repo uses snapshot tests (via `insta`), especially in `codex-rs/tui`, to validate rendered output. When UI or text output changes intentionally, update the snapshots as follows:

- Run tests to generate any updated snapshots:
    - `cargo test -p codex-tui`
- Check what’s pending:
    - `cargo insta pending-snapshots -p codex-tui`
- Review changes by reading the generated `*.snap.new` files directly in the repo, or preview a specific file:
    - `cargo insta show -p codex-tui path/to/file.snap.new`
- Only if you intend to accept all new snapshots in this crate, run:
    - `cargo insta accept -p codex-tui`

If you don’t have the tool:

- `cargo install cargo-insta`

### Test assertions

- Tests should use pretty_assertions::assert_eq for clearer diffs. Import this at the top of the test module if it isn't already.
- Prefer deep equals comparisons whenever possible. Perform `assert_eq!()` on entire objects, rather than individual fields.
- Avoid mutating process environment in tests; prefer passing environment-derived flags or dependencies from above.

### Spawning workspace binaries in tests (Cargo vs Bazel)

- Prefer `codex_utils_cargo_bin::cargo_bin("...")` over `assert_cmd::Command::cargo_bin(...)` or `escargot` when tests need to spawn first-party binaries.
    - Under Bazel, binaries and resources may live under runfiles; use `codex_utils_cargo_bin::cargo_bin` to resolve absolute paths that remain stable after `chdir`.
- When locating fixture files or test resources under Bazel, avoid `env!("CARGO_MANIFEST_DIR")`. Prefer `codex_utils_cargo_bin::find_resource!` so paths resolve correctly under both Cargo and Bazel runfiles.

### Integration tests (core)

- Prefer the utilities in `core_test_support::responses` when writing end-to-end Codex tests.

- All `mount_sse*` helpers return a `ResponseMock`; hold onto it so you can assert against outbound `/responses` POST bodies.
- Use `ResponseMock::single_request()` when a test should only issue one POST, or `ResponseMock::requests()` to inspect every captured `ResponsesRequest`.
- `ResponsesRequest` exposes helpers (`body_json`, `input`, `function_call_output`, `custom_tool_call_output`, `call_output`, `header`, `path`, `query_param`) so assertions can target structured payloads instead of manual JSON digging.
- Build SSE payloads with the provided `ev_*` constructors and the `sse(...)`.
- Prefer `wait_for_event` over `wait_for_event_with_timeout`.
- Prefer `mount_sse_once` over `mount_sse_once_match` or `mount_sse_sequence`

- Typical pattern:

  ```rust
  let mock = responses::mount_sse_once(&server, responses::sse(vec![
      responses::ev_response_created("resp-1"),
      responses::ev_function_call(call_id, "shell", &serde_json::to_string(&args)?),
      responses::ev_completed("resp-1"),
  ])).await;

  codex.submit(Op::UserTurn { ... }).await?;

  // Assert request body if needed.
  let request = mock.single_request();
  // assert using request.function_call_output(call_id) or request.json_body() or other helpers.
  ```

## App-server API Development Best Practices

These guidelines apply to app-server protocol work in `codex-rs`, especially:

- `app-server-protocol/src/protocol/common.rs`
- `app-server-protocol/src/protocol/v2.rs`
- `app-server/README.md`

### Core Rules

- All active API development should happen in app-server v2. Do not add new API surface area to v1.
- Follow payload naming consistently:
  `*Params` for request payloads, `*Response` for responses, and `*Notification` for notifications.
- Expose RPC methods as `<resource>/<method>` and keep `<resource>` singular (for example, `thread/read`, `app/list`).
- Always expose fields as camelCase on the wire with `#[serde(rename_all = "camelCase")]` unless a tagged union or explicit compatibility requirement needs a targeted rename.
- Always set `#[ts(export_to = "v2/")]` on v2 request/response/notification types so generated TypeScript lands in the correct namespace.
- Never use `#[serde(skip_serializing_if = "Option::is_none")]` for v2 API payload fields.
  Exception: client->server requests that intentionally have no params may use:
  `params: #[ts(type = "undefined")] #[serde(skip_serializing_if = "Option::is_none")] Option<()>`.
- For client->server JSON-RPC request payloads (`*Params`) only, every optional field must be annotated with `#[ts(optional = nullable)]`. Do not use `#[ts(optional = nullable)]` outside client->server request payloads (`*Params`).
- For client->server JSON-RPC request payloads only, and you want to express a boolean field where omission means `false`, use `#[serde(default, skip_serializing_if = "std::ops::Not::not")] pub field: bool` over `Option<bool>`.
- For new list methods, implement cursor pagination by default:
  request fields `pub cursor: Option<String>` and `pub limit: Option<u32>`,
  response fields `pub data: Vec<...>` and `pub next_cursor: Option<String>`.
- Keep Rust and TS wire renames aligned. If a field or variant uses `#[serde(rename = "...")]`, add matching `#[ts(rename = "...")]`.
- For discriminated unions, use explicit tagging in both serializers:
  `#[serde(tag = "type", ...)]` and `#[ts(tag = "type", ...)]`.
- Prefer plain `String` IDs at the API boundary (do UUID parsing/conversion internally if needed).
- Timestamps should be integer Unix seconds (`i64`) and named `*_at` (for example, `created_at`, `updated_at`, `resets_at`).
- For experimental API surface area:
  use `#[experimental("method/or/field")]`, derive `ExperimentalApi` when field-level gating is needed, and use `inspect_params: true` in `common.rs` when only some fields of a method are experimental.

### Development Workflow

- Update docs/examples when API behavior changes (at minimum `app-server/README.md`).
- Regenerate schema fixtures when API shapes change:
  `just write-app-server-schema`
  (and `just write-app-server-schema --experimental` when experimental API fixtures are affected).
- Validate with `cargo test -p codex-app-server-protocol`.

## Qubic Network (кратко)

- Базовый p2p-транспорт в этой реализации: TCP, основной порт сети `21841`.
- Формат сетевого фрейма:
  - `size` (3 байта, little-endian, максимум `0xFFFFFF`)
  - `type` (1 байт)
  - `dejavu` (4 байта, `u32`, little-endian)
  - `payload` (переменная длина)
- Handshake/peer exchange:
  - Тип `0` (`EXCHANGE_PUBLIC_PEERS`)
  - Пейлоад: 4 IPv4 адреса
  - Узлы обмениваются пирами и расширяют локальный `known_peers`
- Для request/response используется один и тот же `dejavu`:
  - запрос и ответы коррелируются по `dejavu`
  - окончание многопакетного ответа: тип `35` (`END_RESPONSE`)
- Ключевые типы сообщений, которые использует light-клиент:
  - `27` `REQUEST_CURRENT_TICK_INFO`
  - `28` `RESPOND_CURRENT_TICK_INFO`
  - `31` `REQUEST_ENTITY`
  - `32` `RESPOND_ENTITY`
  - `29` `REQUEST_TICK_TRANSACTIONS`
  - `24` `BROADCAST_TRANSACTION`
  - `35` `END_RESPONSE`

## Как работает этот клиент

- Роль: легкий relay-узел + внешний API (HTTP и gRPC).
- Сетевое ядро:
  - поддерживает входящие и исходящие TCP соединения
  - цель исходящих соединений задается `--target-outbound` (по умолчанию `8`)
  - цикл переподключения: `--reconnect-ms` (по умолчанию `2000` мс)
  - дедуп пакетов по хешу (`blake3`) через скользящее окно (`--max-seen`)
- Источники пиров:
  - `--peer <ip[:port]>`
  - DNS bootstrap (`api.qubic.global`, поле `litePeers`) если не задано ни одного `--peer`
  - входящие `EXCHANGE_PUBLIC_PEERS` от других узлов
- Ретрансляция:
  - по умолчанию ретранслируются только сообщения с `dejavu == 0`
  - режим полного relay: `--relay-all`
  - детальные логи трафика: `--traffic-log`
- Локальный HTTP API (Axum):
  - `GET /api/status`
  - `GET /api/balance/{wallet}`
  - `GET /api/ticks/{tick}/transactions`
  - bind: `--api-listen` (по умолчанию `127.0.0.1:8090`)
  - отключить: `--no-api`
- gRPC API (tonic):
  - сервис `lightnode.LightNode`
  - методы:
    - `GetStatus`
    - `GetBalance`
    - `GetTickTransactions`
  - bind: `--grpc-listen` (по умолчанию `127.0.0.1:50051`)
  - отключить: `--no-grpc`
  - reflection включен (можно использовать `grpcurl` без `-proto`)
- Поддерживаемые форматы кошелька в API:
  - Qubic identity (60 символов `A-Z`)
  - публичный ключ `0x` + 64 hex символа

## Параллелизм и блокировки в этом клиенте

- Горячий путь p2p-пересылки:
  - обработка входящих фреймов идёт конкурентно по соединениям
  - дедуп и маршрутизация получателей выполняются через общий `NodeState`
- Что уже оптимизировано:
  - в fanout больше нет копирования полного payload на каждый peer:
    - используется `Arc<[u8]>` в outbound очередях (`UnboundedSender<Arc<[u8]>>`)
  - `TX/RX` traffic-логи больше не берут `Mutex<NodeState>`:
    - для быстрого чтения epoch/tick используется атомарный кэш (`AtomicU64`, packed `epoch/tick`)
  - запросы к пирам для внешних API `balance` и `tick transactions` выполняются параллельно:
    - используется race-запрос к нескольким пирам (`JoinSet`)
    - ограничение параллелизма: `MAX_PARALLEL_PEER_QUERIES` (сейчас `3`)
    - возвращается первый успешный ответ, остальные задачи отменяются
- Где всё ещё есть блокировки:
  - изменения `sessions / known_peers / dedup` внутри общего `NodeState` через `Mutex`
  - это сознательный компромисс для простоты и корректности, но при высокой нагрузке может быть bottleneck
- Рекомендации для следующих итераций:
  - разделить `NodeState` на независимые lock-domain (например `sessions`, `known_peers`, `dedup`)
  - заменить `unbounded` outbound очереди на bounded с backpressure/policy для медленных peers
  - рассмотреть lock-free/actor модель для дедуп и fanout
