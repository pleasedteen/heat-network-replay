/**
 * HTTP 服务:值班员查询接口。
 *
 *   GET /api/status                          同步状态与各检修进度
 *   GET /api/handovers?from=<检修单号>        交接账列表(默认全部;from 表示“自该场检修起”)
 *   GET /api/handovers/<交接号>/impact[?version=N]   该场检修的断供/恢复清单(默认最新账版;latest 表示最新交接)
 *   GET /api/communities/<节点>/sources[?handover=<交接号>|version=N]  某次交接后小区可接通的在用热源(默认最新交接)
 *   GET /api/ledger[?version=N]              账版快照(管网、热源、各小区供热状态)
 *
 * 所有响应都带 ledgerVersion,标明所用账版。
 */

import http from 'node:http';
import path from 'node:path';
import { Store, type LedgerRow } from './store.js';
import { Network } from './network.js';
import { FileSource } from './source.js';
import { SyncEngine } from './sync.js';
import { LedgerComputer } from './ledger.js';

const PORT = Number(process.env.PORT ?? 8080);
const DB_PATH = process.env.DB_PATH ?? path.resolve('heat-ledger.db');
const DATA_DIR = process.env.DATA_DIR ?? path.resolve('data/replay');
const POLL_MS = Number(process.env.POLL_MS ?? 500);
const PAGE_SIZE = Number(process.env.PAGE_SIZE ?? 100);

const store = new Store(DB_PATH);
const source = new FileSource(DATA_DIR);
const computer = new LedgerComputer(store);
const sync = new SyncEngine(store, source, computer, PAGE_SIZE, POLL_MS);
sync.start();

type Res = http.ServerResponse;

function sendJson(res: Res, status: number, obj: unknown): void {
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
}

function impactView(row: LedgerRow): unknown {
  const names = row.snapshot.communities;
  const deco = (ids: string[]): Array<{ node: string; name: string }> =>
    ids.map(id => ({ node: id, name: names[id] ?? id }));
  return {
    handover: row.handover,
    maintenance: row.maintenance,
    ledgerVersion: row.version,
    supersedes: row.supersedes,
    createdAt: row.createdAt,
    lost: deco(row.impact.lost),
    restored: deco(row.impact.restored),
  };
}

function pickLedger(q: URLSearchParams): LedgerRow | null {
  const version = q.get('version');
  if (version) return store.getLedger(Number(version));
  const handover = q.get('handover');
  if (handover) return store.latestLedgerForHandover(handover);
  return store.latestLedger();
}

function handle(req: http.IncomingMessage, res: Res): void {
  const url = new URL(req.url ?? '/', 'http://localhost');
  const p = url.pathname;
  if (req.method !== 'GET') {
    return sendJson(res, 405, { error: '仅支持 GET' });
  }

  if (p === '/') {
    return sendJson(res, 200, {
      service: 'heat-ledger',
      endpoints: [
        'GET /api/status',
        'GET /api/handovers?from=<检修单号>',
        'GET /api/handovers/<交接号>/impact?version=N',
        'GET /api/communities/<节点>/sources?handover=<交接号>|version=N',
        'GET /api/ledger?version=N',
      ],
    });
  }

  if (p === '/api/status') {
    const latest = store.latestLedger();
    const recompute = store.recomputeStatus();
    return sendJson(res, 200, {
      service: 'heat-ledger',
      time: new Date().toISOString(),
      cursor: store.getCursor(),
      ledgerVersion: latest?.version ?? null,
      syncIdle: sync.idle && recompute === null,
      lastSync: { received: sync.lastReceived, newVersions: sync.lastVersions },
      // 进行中的级联重算:整段换账前查询仍只读到上一份完整账(ledgerVersion 不变)
      recompute: recompute
        ? { runId: recompute.runId, stepsDone: recompute.stepsDone, maintenances: recompute.maintenances }
        : null,
      source: source.describe(),
      maintenances: store.listMaintenances().map(mt => {
        const received = store.partsReceived(mt.id);
        return {
          maintenance: mt.id,
          handover: mt.handover,
          partsReceived: received,
          partsExpected: mt.partsExpected,
          status: received >= mt.partsExpected ? 'complete' : 'collecting',
          ledgerVersion: store.latestLedgerForMaintenance(mt.id)?.version ?? null,
        };
      }),
    });
  }

  if (p === '/api/handovers') {
    const mts = store.listMaintenances();
    let fromOrd = Number.NEGATIVE_INFINITY;
    const from = url.searchParams.get('from');
    if (from) {
      const mt = mts.find(m => m.id === from) ?? mts.find(m => m.handover === from);
      if (!mt) return sendJson(res, 404, { error: `未找到检修/交接: ${from}` });
      fromOrd = mt.ord;
    }
    const list = mts.filter(m => m.ord >= fromOrd).map(m => {
      const l = store.latestLedgerForMaintenance(m.id);
      const received = store.partsReceived(m.id);
      const names = l?.snapshot.communities ?? {};
      const deco = (ids: string[]): Array<{ node: string; name: string }> =>
        ids.map(id => ({ node: id, name: names[id] ?? id }));
      return {
        maintenance: m.id,
        handover: m.handover,
        status: received >= m.partsExpected ? 'complete' : 'collecting',
        partsReceived: received,
        partsExpected: m.partsExpected,
        ledgerVersion: l?.version ?? null,
        supersedes: l?.supersedes ?? null,
        lost: l ? deco(l.impact.lost) : null,
        restored: l ? deco(l.impact.restored) : null,
      };
    });
    return sendJson(res, 200, { handovers: list });
  }

  const mImpact = p.match(/^\/api\/handovers\/([^/]+)\/impact$/);
  if (mImpact) {
    const id = decodeURIComponent(mImpact[1]);
    const v = url.searchParams.get('version');
    let row: LedgerRow | null;
    if (v) {
      row = store.getLedger(Number(v));
      if (row && id !== 'latest' && row.handover !== id) {
        return sendJson(res, 400, { error: `账版 v${v} 属于交接 ${row.handover},与 ${id} 不符` });
      }
    } else {
      row = id === 'latest' ? store.latestLedger() : store.latestLedgerForHandover(id);
    }
    if (!row) return sendJson(res, 404, { error: `未找到交接 ${id} 的账版` });
    return sendJson(res, 200, impactView(row));
  }

  const mSources = p.match(/^\/api\/communities\/([^/]+)\/sources$/);
  if (mSources) {
    const node = decodeURIComponent(mSources[1]);
    const row = pickLedger(url.searchParams);
    if (!row) return sendJson(res, 404, { error: '尚无已完成的交接账' });
    const net = Network.fromSnapshot(row.snapshot);
    const ids = net.reachableSources(node);
    const srcName = new Map(row.snapshot.sources.map(s => [s.id, s.name ?? s.id]));
    return sendJson(res, 200, {
      node,
      name: row.snapshot.communities[node] ?? node,
      handover: row.handover,
      ledgerVersion: row.version,
      supplied: ids.length > 0,
      sources: ids.map(id => ({ id, name: srcName.get(id) ?? id })),
    });
  }

  if (p === '/api/ledger') {
    const row = pickLedger(url.searchParams);
    if (!row) return sendJson(res, 404, { error: '尚无已完成的交接账' });
    const net = Network.fromSnapshot(row.snapshot);
    return sendJson(res, 200, {
      ledgerVersion: row.version,
      handover: row.handover,
      maintenance: row.maintenance,
      supersedes: row.supersedes,
      createdAt: row.createdAt,
      pipes: row.snapshot.pipes,
      sources: row.snapshot.sources,
      communities: Object.entries(row.snapshot.communities).map(([node, name]) => ({
        node,
        name,
        supplied: net.supplied(node),
        sources: net.reachableSources(node),
      })),
    });
  }

  return sendJson(res, 404, { error: `未知路径: ${p}` });
}

const server = http.createServer((req, res) => {
  try {
    handle(req, res);
  } catch (e) {
    sendJson(res, 500, { error: e instanceof Error ? e.message : String(e) });
  }
});

server.listen(PORT, () => {
  console.log(`[server] 账库: ${DB_PATH}`);
  console.log(`[server] 源目录: ${DATA_DIR}`);
  console.log(`[server] 同步: 每 ${POLL_MS}ms 一轮, 每轮最多 ${PAGE_SIZE} 批`);
  console.log(`[server] HTTP: http://127.0.0.1:${PORT}`);
});

function shutdown(): void {
  console.log('[server] 收到退出信号,正在关闭…');
  sync.stop();
  server.close(() => {
    store.close();
    process.exit(0);
  });
  setTimeout(() => process.exit(0), 1500).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
