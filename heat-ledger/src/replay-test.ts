/**
 * 本地回放验证:级联重算的中途终止、追加替换单与重启恢复。
 *
 * 五套独立账库,分阶段喂同一份回放数据:
 *   ref1  不中断基准(只来第一张替换单 seq=8),作为 t1 的完整重算对照;
 *   ref2  不中断基准(seq=8 与追加的 seq=9 同轮到齐),作为 t2 的完整重算对照;
 *   t1    重算持久化 1 步后强制杀进程 → 重启续算,验证已完成且仍有效的步骤不重复计算;
 *   t2    重算持久化 1 步后强制杀进程 → 停机期间追加替换单 seq=9 → 重启,验证
 *         失效进度被丢弃、同场输入未变但上游已变的步骤被重新验证、按新输入补全。
 *
 * 最终断言:t1 与 ref1、t2 与 ref2 的账版链逐字段一致(版本号、替代关系、
 * 快照、影响清单),即中断恢复的结果与不中断的完整重算完全相同。
 *
 * 运行: npm run replay-test
 */

import { spawn, type ChildProcess } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const ROOT = path.resolve(import.meta.dirname, '..');
const RT_ROOT = path.join(ROOT, '.replay-test');
const PORT0 = 18180;
const BASE = (port: number): string => `http://127.0.0.1:${port}`;

// 停机期间追加的第二张替换单:重写 M-002(恢复场),不恢复 P5、不新增 P8,
// 关闭 P6 并新增管段 P9(C1—C6)。这样 C5 的两条上行路径(P5、P6)全断,
// 而 C6 经新管段 P9 由 S1 供热;下游 H-003 的同场输入虽未变、但其上游已变。
const EXTRA_REPLACEMENT = {
  seq: 9,
  maintenance: 'M-002',
  handover: 'H-002',
  part: 1,
  parts: 1,
  kind: 'replacement',
  records: [
    { type: 'source.start', id: 'S1', node: 'N-S1', name: '城东锅炉房' },
    { type: 'valve.close', pipe: 'P6' },
    { type: 'pipe.add', id: 'P9', a: 'C1', b: 'C6' },
  ],
};

const children = new Set<ChildProcess>();
process.on('exit', () => {
  for (const c of children) { try { c.kill('SIGKILL'); } catch { /* 忽略 */ } }
});

let failures = 0;
function check(desc: string, actual: unknown, expected: unknown): void {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) {
    console.log(`  ✓ ${desc}`);
  } else {
    failures++;
    console.log(`  ✗ ${desc}\n      期望: ${e}\n      实际: ${a}`);
  }
}

async function api(port: number, p: string): Promise<any> {
  const r = await fetch(BASE(port) + p);
  if (!r.ok) throw new Error(`GET ${p} -> ${r.status}: ${await r.text()}`);
  return r.json();
}

async function waitFor(desc: string, cond: () => Promise<boolean>, timeoutMs = 30000): Promise<void> {
  const t0 = Date.now();
  for (;;) {
    try {
      if (await cond()) return;
    } catch { /* 服务尚未就绪,继续等 */ }
    if (Date.now() - t0 > timeoutMs) throw new Error(`等待超时: ${desc}`);
    await new Promise(r => setTimeout(r, 150));
  }
}

interface Env {
  name: string;
  dir: string;
  port: number;
  db: string;
  data: string;
}

function mkEnv(name: string, port: number): Env {
  const dir = path.join(RT_ROOT, name);
  fs.mkdirSync(path.join(dir, 'data'), { recursive: true });
  return { name, dir, port, db: path.join(dir, 'heat.db'), data: path.join(dir, 'data') };
}

function dropPart(env: Env, part: 1 | 2 | 3): void {
  if (part === 1) {
    fs.copyFileSync(
      path.join(ROOT, 'data/replay/replay-part1.jsonl'),
      path.join(env.data, 'replay-part1.jsonl'),
    );
  } else if (part === 2) {
    fs.copyFileSync(
      path.join(ROOT, 'data/replay/replay-part2.jsonl'),
      path.join(env.data, 'replay-part2.jsonl'),
    );
  } else {
    fs.writeFileSync(path.join(env.data, 'replay-part3.jsonl'), `${JSON.stringify(EXTRA_REPLACEMENT)}\n`);
  }
}

function startServer(env: Env, envExtra: NodeJS.ProcessEnv = {}): ChildProcess {
  const child = spawn(
    process.execPath,
    ['--disable-warning=ExperimentalWarning', path.join(ROOT, 'dist', 'server.js')],
    {
      env: {
        ...process.env,
        PORT: String(env.port),
        DB_PATH: env.db,
        DATA_DIR: env.data,
        POLL_MS: '100',
        PAGE_SIZE: '4',
        ...envExtra,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );
  children.add(child);
  const log = (d: unknown): void => {
    for (const line of String(d).split('\n')) if (line.trim()) console.log(`    [${env.name}] ${line}`);
  };
  child.stdout?.setEncoding('utf8').on('data', log);
  child.stderr?.setEncoding('utf8').on('data', log);
  return child;
}

function killHard(child: ChildProcess): Promise<void> {
  return new Promise(resolve => {
    child.once('exit', () => { children.delete(child); resolve(); });
    child.kill('SIGKILL'); // 强制中断:不给优雅停机机会,模拟重算被强行打断
  });
}

function stopSoft(child: ChildProcess): Promise<void> {
  return new Promise(resolve => {
    const t = setTimeout(() => child.kill('SIGKILL'), 3000);
    child.once('exit', () => { clearTimeout(t); children.delete(child); resolve(); });
    child.kill('SIGTERM');
  });
}

/** 以只读方式打开账库检查重算步骤(WAL 模式下不影响服务进程)。 */
function openRo(env: Env): DatabaseSync {
  return new DatabaseSync(`file:${env.db}?mode=ro`, { readOnly: true });
}

interface StepRow {
  ord: number;
  maintenance: string;
  status: string;
  version: number | null;
  created_at: string;
}

function stepsOf(env: Env, runId: string): StepRow[] {
  const db = openRo(env);
  try {
    return db.prepare(
      `SELECT ord, maintenance, status, version, created_at
       FROM recompute_runs WHERE run_id = ? ORDER BY ord`,
    ).all(runId) as unknown as StepRow[];
  } finally {
    db.close();
  }
}

function activeRun(env: Env): string | null {
  const db = openRo(env);
  try {
    const row = db.prepare(`SELECT v FROM meta WHERE k = 'recompute_run'`).get() as { v: string } | undefined;
    return row ? String(row.v) : null;
  } finally {
    db.close();
  }
}

function maxLedgerVersion(env: Env): number {
  const db = openRo(env);
  try {
    const r = db.prepare(`SELECT MAX(version) AS m FROM ledgers`).get() as { m: number | null };
    return Number(r.m ?? 0);
  } finally {
    db.close();
  }
}

interface LedgerRowLite {
  version: number;
  handover: string;
  supersedes: number | null;
}

function ledgerRow(env: Env, version: number): LedgerRowLite | null {
  const db = openRo(env);
  try {
    const r = db.prepare(
      `SELECT version, handover, supersedes FROM ledgers WHERE version = ?`,
    ).get(version) as Record<string, unknown> | undefined;
    if (!r) return null;
    return {
      version: Number(r.version),
      handover: String(r.handover),
      supersedes: r.supersedes === null || r.supersedes === undefined ? null : Number(r.supersedes),
    };
  } finally {
    db.close();
  }
}

/** 去掉易变字段,只比较业务内容;版本号单独保留比较。 */
function normalizeLedger(o: any): any {
  return {
    handover: o.handover,
    maintenance: o.maintenance,
    supersedes: o.supersedes,
    pipes: o.pipes,
    sources: o.sources,
    communities: (o.communities ?? []).map((c: any) => ({
      node: c.node, name: c.name, supplied: c.supplied, sources: c.sources,
    })),
  };
}

async function fullLedgerDump(port: number): Promise<Array<{ version: number; body: any }>> {
  const status = await api(port, '/api/status');
  const latest = status.ledgerVersion as number;
  const out: Array<{ version: number; body: any }> = [];
  for (let v = 1; v <= latest; v++) {
    const body = await api(port, `/api/ledger?version=${v}`);
    out.push({ version: v, body: normalizeLedger(body) });
  }
  return out;
}

/** 阶段 A:只喂 part1,收敛到初始账版 v1..v4。 */
async function buildInitialLedger(env: Env): Promise<ChildProcess> {
  dropPart(env, 1);
  const srv = startServer(env);
  await waitFor(`${env.name} 初始建账到 v4`, async () => {
    const s = await api(env.port, '/api/status');
    return s.cursor === 7 && s.ledgerVersion === 4 && s.syncIdle;
  });
  return srv;
}

async function main(): Promise<void> {
  console.log('============================================================');
  console.log(' 级联重算可恢复性 · 本地回放验证(中断 / 追加替换单 / 重启)');
  console.log('============================================================');

  fs.rmSync(RT_ROOT, { recursive: true, force: true });

  const ref1 = mkEnv('ref1', PORT0);
  const ref2 = mkEnv('ref2', PORT0 + 1);
  const t1 = mkEnv('t1', PORT0 + 2);
  const t2 = mkEnv('t2', PORT0 + 3);

  // ========== 阶段 A:四套环境各自建立初始账 v1..v4 ==========
  console.log('\n■ 阶段 A:四套环境分别回放 part1,建立初始账版 v1..v4');
  for (const env of [ref1, ref2, t1, t2]) {
    const srv = await buildInitialLedger(env);
    check(`${env.name} 初始账版 = v4`, (await api(env.port, '/api/status')).ledgerVersion, 4);
    await stopSoft(srv);
  }

  // ========== 基准 ref1:只来 seq=8,不中断完整重算 ==========
  console.log('\n■ 基准 ref1:第一张替换单 seq=8 到齐,不中断完整重算');
  let ref1Dump: Array<{ version: number; body: any }> = [];
  dropPart(ref1, 2);
  {
    const srv = startServer(ref1);
    await waitFor('ref1 收敛到 v7', async () => {
      const s = await api(ref1.port, '/api/status');
      return s.cursor === 8 && s.ledgerVersion === 7 && s.syncIdle;
    });
    check('ref1 无进行中的重算运行', (await api(ref1.port, '/api/status')).recompute, null);
    ref1Dump = await fullLedgerDump(ref1.port);
    await stopSoft(srv);
  }

  // ========== 基准 ref2:seq=8 与 seq=9 同轮到齐,不中断完整重算 ==========
  console.log('\n■ 基准 ref2:seq=8 与追加替换单 seq=9 同轮到齐,不中断完整重算');
  let ref2Dump: Array<{ version: number; body: any }> = [];
  dropPart(ref2, 2);
  dropPart(ref2, 3);
  {
    const srv = startServer(ref2);
    await waitFor('ref2 收敛到 v7', async () => {
      const s = await api(ref2.port, '/api/status');
      return s.cursor === 9 && s.ledgerVersion === 7 && s.syncIdle;
    });
    ref2Dump = await fullLedgerDump(ref2.port);
    await stopSoft(srv);
  }

  // ========== 场景 t1:重算中途强制终止 → 重启续算 ==========
  console.log('\n■ 场景 t1:重算持久化 1 步后强制终止(SIGKILL),重启接着算');
  dropPart(t1, 2);
  const srv1a = startServer(t1, { HEAT_ABORT_AFTER_STEPS: '1' });
  await waitFor('t1 进程带着 1 个已持久化步骤退出', async () => srv1a.exitCode !== null);
  check('t1 进程退出码 = 2(测试钩子模拟强制中断)', srv1a.exitCode, 2);

  const run1 = activeRun(t1);
  const steps1Killed = stepsOf(t1, run1 ?? '');
  check('t1 中断后:重算运行仍标记为进行中(整段未换账)', run1 !== null, true);
  check('t1 中断后:仅 1 个步骤已持久化,后续步骤尚未计算',
    steps1Killed.map(s => [s.ord, s.maintenance, s.status]),
    [[0, 'M-001', 'pending']]);
  check('t1 中断后:账版链仍停在上一份完整账 v4(没有半成品账版)', maxLedgerVersion(t1), 4);

  console.log('  重启服务(不带中断钩子),凭 SQLite 中的运行状态续算…');
  const srv1b = startServer(t1);
  await waitFor('t1 重算收敛到 v7', async () => {
    const s = await api(t1.port, '/api/status');
    return s.cursor === 8 && s.ledgerVersion === 7 && s.syncIdle;
  });

  // 已完成且仍有效的交接不重复计算:ord=0 的 created_at 必须保持中断前的值
  const steps1Final = stepsOf(t1, run1 ?? '');
  check('t1 重启后:同一运行的 3 个步骤全部提交并写回账版号',
    steps1Final.map(s => [s.status, s.version]),
    [['committed', 5], ['committed', 6], ['committed', 7]]);
  check('t1 重启后:第 1 步沿用中断前的持久化结果(未重复计算)',
    steps1Final[0].created_at, steps1Killed[0].created_at);
  check('t1 重启后:第 2、3 步为恢复后补算(时间戳新于第 1 步)',
    [steps1Final[1].created_at > steps1Final[0].created_at,
     steps1Final[2].created_at > steps1Final[0].created_at],
    [true, true]);

  // 一次换账 + 旧版与替代关系仍可查
  const h1new = await api(t1.port, '/api/handovers/H-001/impact');
  check('t1 新账 v5 替代旧账 v2', [h1new.ledgerVersion, h1new.supersedes], [5, 2]);
  const h1old = await api(t1.port, '/api/handovers/H-001/impact?version=2');
  check('t1 旧账 v2 仍可查(误录版本保留)', h1old.ledgerVersion, 2);
  check('t1 换账后无进行中的重算运行', (await api(t1.port, '/api/status')).recompute, null);

  // 与不中断完整重算逐字段一致
  check('t1 恢复结果与完整重算 ref1 完全一致(v1..v7 逐版快照/影响/替代关系)',
    await fullLedgerDump(t1.port), ref1Dump);
  await stopSoft(srv1b);

  // ========== 场景 t2:停机期间追加替换单 → 失效丢弃 + 上游变更重新验证 ==========
  console.log('\n■ 场景 t2:重算持久化 1 步后强制终止,停机期间追加 M-002 替换单 seq=9');
  dropPart(t2, 2);
  const srv2a = startServer(t2, { HEAT_ABORT_AFTER_STEPS: '1' });
  await waitFor('t2 进程带着 1 个已持久化步骤退出', async () => srv2a.exitCode !== null);
  const run2 = activeRun(t2);
  const steps2Killed = stepsOf(t2, run2 ?? '');
  check('t2 中断现场:仅 ord=0(M-001)1 个 pending 步骤',
    steps2Killed.map(s => [s.ord, s.maintenance]), [[0, 'M-001']]);

  // 停机期间源端新输入入库(替换单文件落到回放目录,等待重启拉取)
  dropPart(t2, 3);
  console.log('  已追加替换单 seq=9(重写 M-002:不恢复 P5、不新增 P8,改为新增 P9)');

  // 重启并挂起在“持久化 2 步”处:M-001 步骤被重新验证后复用,M-002 按新输入重算,
  // M-003 尚未算;整段未换账,HTTP 仍只读旧账。
  const srv2b = startServer(t2, { HEAT_HOLD_AFTER_STEPS: '2' });
  await waitFor('t2 重启后挂起(M-001 复用、M-002 已按新输入重算、M-003 待补)', async () => {
    const s = await api(t2.port, '/api/status');
    return s.recompute !== null && s.recompute.stepsDone === 2 && s.ledgerVersion === 4;
  });
  const st2Hold = await api(t2.port, '/api/status');
  check('t2 挂起期间:值班员默认查询仍只看到上一份完整账 v4', st2Hold.ledgerVersion, 4);
  check('t2 挂起期间:重算运行仍是中断前那一轮(runId 不变)',
    st2Hold.recompute.runId, run2);
  check('t2 挂起期间:已落库步骤为 M-001(复用)+ M-002(重算)',
    st2Hold.recompute.maintenances, ['M-001', 'M-002']);
  const heldSteps = stepsOf(t2, run2 ?? '');
  check('t2:M-001 同场输入未变、上游未变 → 重新验证通过后原样复用(不重复计算)',
    heldSteps[0].created_at, steps2Killed[0].created_at);
  check('t2:M-002 受新替换单影响 → 失效进度丢弃后按新输入重算(时间戳更新)',
    heldSteps[1].created_at > steps2Killed[0].created_at, true);

  // 整段完成前,按交接选择的查询也只能看到旧账
  const c5Hold = await api(t2.port, '/api/communities/C5/sources?handover=H-002');
  check('t2 挂起期间:按交接 H-002 查询仍是旧账(v3,C5 有热)',
    [c5Hold.ledgerVersion, c5Hold.supplied], [3, true]);

  // 放开挂起:强制结束后再次重启,让整段算完并一次换账
  await killHard(srv2b);
  const srv2c = startServer(t2);
  await waitFor('t2 收敛到 v7', async () => {
    const s = await api(t2.port, '/api/status');
    return s.cursor === 9 && s.ledgerVersion === 7 && s.syncIdle;
  });

  const steps2Final = stepsOf(t2, run2 ?? '');
  check('t2 最终:3 个步骤在一次换账中全部提交(v5、v6、v7)',
    steps2Final.map(s => [s.status, s.version]),
    [['committed', 5], ['committed', 6], ['committed', 7]]);
  check('t2:M-001 复用中断前结果(全程只算过一次)',
    steps2Final[0].created_at, steps2Killed[0].created_at);
  check('t2:M-002 按新替换单重算',
    steps2Final[1].created_at > steps2Killed[0].created_at, true);
  check('t2:M-003 同场输入未变但上游已变 → 重新验证不通过后重算',
    steps2Final[2].created_at > steps2Killed[0].created_at, true);

  // 与不中断完整重算逐字段完全一致(含版本号)
  check('t2 恢复结果与完整重算 ref2 完全一致(v1..v7 逐版一致)',
    await fullLedgerDump(t2.port), ref2Dump);

  // 业务语义抽查:新替换单生效,P5、P6 均关闭 → C5 两条上行路径全断;P9 接通 C6
  const h2 = await api(t2.port, '/api/handovers/H-002/impact');
  check('t2 H-002 最新账为 v6 且替代 v3', [h2.ledgerVersion, h2.supersedes], [6, 3]);
  const c5 = await api(t2.port, '/api/communities/C5/sources?handover=H-002');
  check('t2 新账下 H-002 后 C5 断供(P5、P6 均关闭,替换单内容生效)',
    [c5.ledgerVersion, c5.supplied], [6, false]);
  const c6 = await api(t2.port, '/api/communities/C6/sources?handover=H-002');
  check('t2 新账下 C6 经新管段 P9 接通 S1(替换单内容生效)',
    [c6.supplied, c6.sources.map((x: any) => x.id)], [true, ['S1']]);

  // 旧版及替代关系仍可查:v3(旧 H-002)与 v6(新 H-002)同时在册
  check('t2 旧账版 v3 保留可查(属于 H-002)', ledgerRow(t2, 3),
    { version: 3, handover: 'H-002', supersedes: null });
  check('t2 新账版 v6 supersedes 指向 v3(替代关系完整)', ledgerRow(t2, 6),
    { version: 6, handover: 'H-002', supersedes: 3 });

  await stopSoft(srv2c);

  console.log('\n============================================================');
  if (failures === 0) {
    console.log(' 回放验证全部通过 ✓');
  } else {
    console.log(` 有 ${failures} 项校验未通过 ✗`);
  }
  console.log('============================================================');
  process.exit(failures === 0 ? 0 : 1);
}

main().catch(e => {
  console.error('回放验证失败:', e instanceof Error ? e.stack ?? e.message : e);
  process.exit(1);
});
