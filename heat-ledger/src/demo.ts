/**
 * 演示与自检:使用自带回放数据 + 嵌入式 SQLite,完整走一遍业务流程。
 *
 *   阶段一  首批日志同步(乱序/重复/新旧格式混排)→ 生成账版 v1..v4
 *   阶段二  杀掉服务进程模拟重启,凭 SQLite 中的游标续传
 *   阶段三  补交替换单(纠正 M-001 误录)→ 级联重算 v5..v7,用新账复核
 *
 * 运行: npm run demo   (会先自动执行 npm run build)
 */

import { spawn, type ChildProcess } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const ROOT = path.resolve(import.meta.dirname, '..');
const DEMO_DIR = path.join(ROOT, '.demo');
const RUNTIME = path.join(DEMO_DIR, 'runtime');
const DB_PATH = path.join(DEMO_DIR, 'heat.db');
const PORT = 18080;
const BASE = `http://127.0.0.1:${PORT}`;

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

async function api(p: string): Promise<any> {
  const r = await fetch(BASE + p);
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

function startServer(tag: string): ChildProcess {
  const child = spawn(
    process.execPath,
    ['--disable-warning=ExperimentalWarning', path.join(ROOT, 'dist', 'server.js')],
    {
      env: { ...process.env, PORT: String(PORT), DB_PATH, DATA_DIR: RUNTIME, POLL_MS: '120', PAGE_SIZE: '4' },
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );
  children.add(child);
  const log = (d: unknown): void => {
    for (const line of String(d).split('\n')) if (line.trim()) console.log(`    [服务${tag}] ${line}`);
  };
  child.stdout?.setEncoding('utf8').on('data', log);
  child.stderr?.setEncoding('utf8').on('data', log);
  return child;
}

function stopServer(child: ChildProcess): Promise<void> {
  return new Promise(resolve => {
    const t = setTimeout(() => child.kill('SIGKILL'), 3000);
    child.once('exit', () => { clearTimeout(t); children.delete(child); resolve(); });
    child.kill('SIGTERM');
  });
}

function runtimeLines(): number {
  if (!fs.existsSync(RUNTIME)) return 0;
  let n = 0;
  for (const f of fs.readdirSync(RUNTIME)) {
    n += fs.readFileSync(path.join(RUNTIME, f), 'utf8').split('\n').filter(l => l.trim()).length;
  }
  return n;
}

const fmtNodes = (a: Array<{ node: string; name: string }>): string =>
  a.length ? a.map(x => `${x.name}(${x.node})`).join('、') : '(无)';
const fmtSrcs = (a: Array<{ id: string; name: string }>): string =>
  a.length ? a.map(x => `${x.name}(${x.id})`).join('、') : '(无)';
const nodeIds = (a: Array<{ node: string }>): string[] => a.map(x => x.node);
const srcIds = (a: Array<{ id: string }>): string[] => a.map(x => x.id);

function printHandover(h: any): void {
  const sup = h.supersedes ? `(替代旧账 v${h.supersedes})` : '';
  console.log(`    ${h.handover} [${h.maintenance}] 账版 v${h.ledgerVersion ?? '-'}${sup} 状态:${h.status} 批次:${h.partsReceived}/${h.partsExpected}`);
  if (h.ledgerVersion) {
    console.log(`        断供: ${fmtNodes(h.lost)}`);
    console.log(`        恢复: ${fmtNodes(h.restored)}`);
  }
}

async function main(): Promise<void> {
  console.log('============================================================');
  console.log(' 城市供热调度中心 · 检修交接账同步服务 — 演示');
  console.log('============================================================');

  fs.rmSync(DEMO_DIR, { recursive: true, force: true });
  fs.mkdirSync(RUNTIME, { recursive: true });
  fs.copyFileSync(path.join(ROOT, 'data/replay/replay-part1.jsonl'), path.join(RUNTIME, 'replay-part1.jsonl'));

  // ---------- 阶段一 ----------
  console.log('\n■ 阶段一:首批日志同步(批次乱序、含重复行、新旧记录格式混排)');
  const serverA = startServer('A');
  await waitFor('账版推进到 v4', async () => {
    const s = await api('/api/status');
    return s.cursor === 7 && s.ledgerVersion === 4 && s.syncIdle;
  });
  const st1 = await api('/api/status');
  console.log(`  游标已推进到 seq=${st1.cursor},当前账版 v${st1.ledgerVersion}`);
  check('阶段一游标 = 7(乱序/重复批次均已归位)', st1.cursor, 7);
  check('阶段一账版 = v4(M-000..M-003 四场交接)', st1.ledgerVersion, 4);

  const hs1 = (await api('/api/handovers')).handovers;
  console.log('  交接账(含误录的旧账):');
  for (const h of hs1) printHandover(h);
  const h1Old = await api('/api/handovers/H-001/impact');
  check('H-001 旧账(v2)断供 = C1,C2,C3,C6(误录阀门关 P6)', nodeIds(h1Old.lost), ['C1', 'C2', 'C3', 'C6']);
  const c5Old = await api('/api/communities/C5/sources?handover=H-001');
  check('旧账下 C5 在 H-001 后仍显示有热(误录所致)', srcIds(c5Old.sources), ['S2']);

  // ---------- 阶段二 ----------
  console.log('\n■ 阶段二:模拟服务重启(源端已清理确认日志,账库保留在 SQLite)');
  await stopServer(serverA);
  console.log('  服务进程已退出。');
  check('源端已确认并清理全部 7 条日志(回放目录剩余行数)', runtimeLines(), 0);
  check('账库文件仍在(重启后续传的依据)', fs.existsSync(DB_PATH), true);

  fs.copyFileSync(path.join(ROOT, 'data/replay/replay-part2.jsonl'), path.join(RUNTIME, 'replay-part2.jsonl'));
  console.log('  调度员补交替换单: M-001 第 2 批 误录 valve.close P6 → 更正为 valve.close P5');

  // ---------- 阶段三 ----------
  console.log('\n■ 阶段三:重启服务,续传替换单并级联重算');
  const serverB = startServer('B');
  await waitFor('账版推进到 v7', async () => {
    const s = await api('/api/status');
    return s.cursor === 8 && s.ledgerVersion === 7 && s.syncIdle;
  });
  const st2 = await api('/api/status');
  console.log(`  游标续传到 seq=${st2.cursor},重算产生新账版 v5..v7`);
  check('重启后游标 = 8(接着上次继续,不重复不漏)', st2.cursor, 8);
  check('重算后最新账版 = v7', st2.ledgerVersion, 7);
  check('替换单也已确认清理(回放目录剩余行数)', runtimeLines(), 0);

  console.log('\n■ 交接账复核:自 M-001 起的交接历史(新账)');
  const hs2 = (await api('/api/handovers?from=M-001')).handovers;
  for (const h of hs2) printHandover(h);
  check('自 M-001 起共 3 场交接', hs2.map((h: any) => h.handover), ['H-001', 'H-002', 'H-003']);
  check('各场交接均已换成新账版', hs2.map((h: any) => h.ledgerVersion), [5, 6, 7]);
  check('新账版替代关系', hs2.map((h: any) => h.supersedes), [2, 3, 4]);

  const h1New = await api('/api/handovers/H-001/impact');
  console.log(`\n■ H-001 断供/恢复清单(账版 v${h1New.ledgerVersion},替代 v${h1New.supersedes}):`);
  console.log(`    断供: ${fmtNodes(h1New.lost)}`);
  console.log(`    恢复: ${fmtNodes(h1New.restored)}`);
  check('H-001 新账(v5)断供 = C1,C2,C3,C5,C6(更正为关 P5 后,C5 实际断供)',
    nodeIds(h1New.lost), ['C1', 'C2', 'C3', 'C5', 'C6']);
  check('H-001 新账(v5)恢复 = 无', nodeIds(h1New.restored), []);

  const latest = await api('/api/handovers/latest/impact');
  console.log(`\n■ 最新交接 ${latest.handover}(账版 v${latest.ledgerVersion}):`);
  console.log(`    断供: ${fmtNodes(latest.lost)}    恢复: ${fmtNodes(latest.restored)}`);
  check('默认查询最新交接 = H-003', latest.handover, 'H-003');
  check('H-003(拆除退役管段 P1)无断供无恢复', [nodeIds(latest.lost), nodeIds(latest.restored)], [[], []]);

  console.log('\n■ 值班员查询:各次交接后小区可接通的在用热源');
  const queries: Array<[string, string, string[], boolean, string]> = [
    ['C2', '', ['S2'], true, '默认最新账:翠湖小区由西郊热电厂供热'],
    ['C2', 'handover=H-000', ['S1'], true, '初始交接后:翠湖小区只有城东锅炉房(止回管段 P3 挡住反向热流)'],
    ['C3', 'handover=H-000', ['S1', 'S2'], true, '初始交接后:滨河小区双热源'],
    ['C2', 'handover=H-001', [], false, 'H-001 后:翠湖小区断供'],
    ['C4', 'handover=H-001', ['S2'], true, 'H-001 后:青云小区未受影响'],
    ['C5', 'handover=H-001', [], false, 'H-001 后(新账):杏林小区断供(旧误录账曾显示有热)'],
  ];
  for (const [node, qs, expectSrcs, expectSupplied, desc] of queries) {
    const r = await api(`/api/communities/${node}/sources${qs ? `?${qs}` : ''}`);
    console.log(`    ${r.name}(${node}) @ ${r.handover} 账版v${r.ledgerVersion}: ${fmtSrcs(r.sources)} → ${r.supplied ? '有供热' : '断供'}`);
    check(desc, [srcIds(r.sources), r.supplied], [expectSrcs, expectSupplied]);
  }

  await stopServer(serverB);

  console.log('\n============================================================');
  if (failures === 0) {
    console.log(' 全部校验通过 ✓');
  } else {
    console.log(` 有 ${failures} 项校验未通过 ✗`);
  }
  console.log('============================================================');
  process.exit(failures === 0 ? 0 : 1);
}

main().catch(e => {
  console.error('演示失败:', e instanceof Error ? e.message : e);
  process.exit(1);
});
