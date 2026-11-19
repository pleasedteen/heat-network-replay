/**
 * SQLite 持久化:同步游标、已收批次(去重/替换)、检修台账、账版快照。
 * 服务重启后从库中恢复游标与账版,接着同步并补全交接账。
 */

import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import type { Batch } from './model.js';
import type { Snapshot } from './network.js';

export interface MaintenanceRow {
  id: string;
  handover: string;
  partsExpected: number;
  ord: number; // 检修在链上的先后顺序(取该检修各批次的最小 seq)
}

export interface Impact {
  lost: string[];     // 断供小区节点
  restored: string[]; // 恢复小区节点
}

export interface LedgerRow {
  version: number;
  handover: string;
  maintenance: string;
  supersedes: number | null; // 被本账版替代的旧账版
  inputHash: string;
  impact: Impact;
  snapshot: Snapshot;
  createdAt: string;
}

export interface NewLedger {
  handover: string;
  maintenance: string;
  supersedes: number | null;
  inputHash: string;
  impact: Impact;
  snapshot: Snapshot;
}

/**
 * 级联重算的一个持久化步骤(对应检修链上的一场交接)。
 * 算完一步立即落库:进程被强制中断后,重启可凭这些步骤续算。
 * status=pending 表示本次换账尚未整体提交;committed 为已换账的审计留痕
 * (含最终账版号与替代关系,旧账版本身仍在 ledgers 表中可查)。
 */
export interface RecomputeStep extends NewLedger {
  runId: string;
  ord: number;
  status: 'pending' | 'committed';
  version: number | null;
  createdAt: string;
  committedAt: string | null;
}

export class Store {
  private db: DatabaseSync;

  constructor(dbPath: string) {
    this.db = new DatabaseSync(dbPath);
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      CREATE TABLE IF NOT EXISTS meta (
        k TEXT PRIMARY KEY,
        v TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS received (
        seq INTEGER PRIMARY KEY
      );
      CREATE TABLE IF NOT EXISTS batches (
        maintenance TEXT NOT NULL,
        part        INTEGER NOT NULL,
        seq         INTEGER NOT NULL,
        handover    TEXT NOT NULL,
        parts       INTEGER NOT NULL,
        kind        TEXT NOT NULL,
        payload     TEXT NOT NULL,
        PRIMARY KEY (maintenance, part)
      );
      CREATE TABLE IF NOT EXISTS maintenances (
        id             TEXT PRIMARY KEY,
        handover       TEXT NOT NULL,
        parts_expected INTEGER NOT NULL,
        ord            INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS ledgers (
        version      INTEGER PRIMARY KEY AUTOINCREMENT,
        handover     TEXT NOT NULL,
        maintenance  TEXT NOT NULL,
        supersedes   INTEGER,
        input_hash   TEXT NOT NULL,
        impact       TEXT NOT NULL,
        snapshot     TEXT NOT NULL,
        created_at   TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS recompute_runs (
        run_id       TEXT NOT NULL,
        ord          INTEGER NOT NULL,
        maintenance  TEXT NOT NULL,
        handover     TEXT NOT NULL,
        supersedes   INTEGER,
        input_hash   TEXT NOT NULL,
        impact       TEXT NOT NULL,
        snapshot     TEXT NOT NULL,
        status       TEXT NOT NULL,
        version      INTEGER,
        created_at   TEXT NOT NULL,
        committed_at TEXT,
        PRIMARY KEY (run_id, ord)
      );
      CREATE INDEX IF NOT EXISTS idx_ledgers_handover ON ledgers (handover, version);
      CREATE INDEX IF NOT EXISTS idx_ledgers_maintenance ON ledgers (maintenance, version);
      CREATE INDEX IF NOT EXISTS idx_runs_status ON recompute_runs (status, run_id, ord);
    `);
  }

  close(): void {
    this.db.close();
  }

  // ---------- 同步游标(已确认接收的最大连续 seq) ----------

  getCursor(): number {
    const row = this.db.prepare(`SELECT v FROM meta WHERE k = 'cursor'`).get() as { v: string } | undefined;
    return row ? Number(row.v) : 0;
  }

  private setCursor(n: number): void {
    this.db.prepare(
      `INSERT INTO meta (k, v) VALUES ('cursor', ?)
       ON CONFLICT(k) DO UPDATE SET v = excluded.v`,
    ).run(String(n));
  }

  /** 依据已收到的 seq 集合推进连续游标。 */
  advanceCursor(): number {
    const has = this.db.prepare(`SELECT 1 AS x FROM received WHERE seq = ?`);
    let c = this.getCursor();
    while (has.get(c + 1)) c++;
    this.setCursor(c);
    return c;
  }

  // ---------- 批次入库:幂等去重 + 替换单覆盖 ----------

  putBatch(b: Batch): 'duplicate' | 'new' | 'replaced' {
    this.db.exec('BEGIN');
    try {
      const dup = this.db.prepare(`SELECT 1 AS x FROM received WHERE seq = ?`).get(b.seq);
      if (dup) {
        this.db.exec('COMMIT');
        return 'duplicate';
      }
      this.db.prepare(`INSERT INTO received (seq) VALUES (?)`).run(b.seq);
      const payload = JSON.stringify(b.records);
      const existing = this.db
        .prepare(`SELECT 1 AS x FROM batches WHERE maintenance = ? AND part = ?`)
        .get(b.maintenance, b.part);
      if (existing) {
        this.db.prepare(
          `UPDATE batches SET seq = ?, handover = ?, parts = ?, kind = ?, payload = ?
           WHERE maintenance = ? AND part = ?`,
        ).run(b.seq, b.handover, b.parts, b.kind, payload, b.maintenance, b.part);
      } else {
        this.db.prepare(
          `INSERT INTO batches (maintenance, part, seq, handover, parts, kind, payload)
           VALUES (?, ?, ?, ?, ?, ?, ?)`,
        ).run(b.maintenance, b.part, b.seq, b.handover, b.parts, b.kind, payload);
      }
      const mt = this.db
        .prepare(`SELECT parts_expected AS pe, ord FROM maintenances WHERE id = ?`)
        .get(b.maintenance) as { pe: number; ord: number } | undefined;
      if (mt) {
        this.db.prepare(
          `UPDATE maintenances SET handover = ?, parts_expected = ?, ord = ? WHERE id = ?`,
        ).run(b.handover, Math.max(mt.pe, b.parts), Math.min(mt.ord, b.seq), b.maintenance);
      } else {
        this.db.prepare(
          `INSERT INTO maintenances (id, handover, parts_expected, ord) VALUES (?, ?, ?, ?)`,
        ).run(b.maintenance, b.handover, b.parts, b.seq);
      }
      this.db.exec('COMMIT');
      return existing ? 'replaced' : 'new';
    } catch (e) {
      this.db.exec('ROLLBACK');
      throw e;
    }
  }

  getBatches(maintenance: string): Batch[] {
    const rows = this.db
      .prepare(`SELECT * FROM batches WHERE maintenance = ? ORDER BY part`)
      .all(maintenance) as Array<Record<string, unknown>>;
    return rows.map(r => ({
      seq: Number(r.seq),
      maintenance: String(r.maintenance),
      handover: String(r.handover),
      part: Number(r.part),
      parts: Number(r.parts),
      kind: r.kind as 'normal' | 'replacement',
      records: JSON.parse(String(r.payload)) as Batch['records'],
    }));
  }

  partsReceived(maintenance: string): number {
    const r = this.db
      .prepare(`SELECT COUNT(*) AS c FROM batches WHERE maintenance = ?`)
      .get(maintenance) as { c: number };
    return Number(r.c);
  }

  listMaintenances(): MaintenanceRow[] {
    const rows = this.db
      .prepare(`SELECT id, handover, parts_expected AS pe, ord FROM maintenances ORDER BY ord, id`)
      .all() as Array<Record<string, unknown>>;
    return rows.map(r => ({
      id: String(r.id),
      handover: String(r.handover),
      partsExpected: Number(r.pe),
      ord: Number(r.ord),
    }));
  }

  /** 该检修当前已收批次的指纹;替换单改变指纹,从而触发该检修及其后所有交接的重算。 */
  stateHash(maintenance: string): string {
    const rows = this.db
      .prepare(`SELECT kind, payload FROM batches WHERE maintenance = ? ORDER BY part`)
      .all(maintenance) as Array<Record<string, unknown>>;
    const h = createHash('sha1');
    for (const r of rows) h.update(String(r.kind)).update('|').update(String(r.payload)).update('\n');
    return h.digest('hex');
  }

  // ---------- 账版 ----------

  // ---------- 级联重算运行(可中断、可恢复、一次换账) ----------

  private getMeta(k: string): string | null {
    const row = this.db.prepare(`SELECT v FROM meta WHERE k = ?`).get(k) as { v: string } | undefined;
    return row ? String(row.v) : null;
  }

  private putMeta(k: string, v: string): void {
    this.db.prepare(
      `INSERT INTO meta (k, v) VALUES (?, ?)
       ON CONFLICT(k) DO UPDATE SET v = excluded.v`,
    ).run(k, v);
  }

  /** 当前进行中(尚未整体换账)的重算运行号;没有则 null。 */
  getActiveRecomputeRun(): string | null {
    return this.getMeta('recompute_run');
  }

  /** 开启新一轮重算(幂等:重复开启同一 run 是安全的)。 */
  beginRecomputeRun(runId: string): void {
    this.putMeta('recompute_run', runId);
  }

  /** 清理上一轮遗留、且不属于当前运行的待提交步骤。 */
  clearRecomputeRun(runId: string): void {
    this.db.prepare(
      `DELETE FROM recompute_runs WHERE run_id <> ? AND status = 'pending'`,
    ).run(runId);
  }

  /** 放弃一轮重算:删除其全部待提交步骤并清除进行中标记(仅当标记仍指向该轮)。 */
  abandonRecomputeRun(runId: string): void {
    this.db.exec('BEGIN');
    try {
      this.db.prepare(
        `DELETE FROM recompute_runs WHERE run_id = ? AND status = 'pending'`,
      ).run(runId);
      this.db.prepare(
        `DELETE FROM meta WHERE k = 'recompute_run' AND v = ?`,
      ).run(runId);
      this.db.exec('COMMIT');
    } catch (e) {
      this.db.exec('ROLLBACK');
      throw e;
    }
  }

  private toStep(r: Record<string, unknown>): RecomputeStep {
    return {
      runId: String(r.run_id),
      ord: Number(r.ord),
      maintenance: String(r.maintenance),
      handover: String(r.handover),
      supersedes: r.supersedes === null || r.supersedes === undefined ? null : Number(r.supersedes),
      inputHash: String(r.input_hash),
      impact: JSON.parse(String(r.impact)) as Impact,
      snapshot: JSON.parse(String(r.snapshot)) as Snapshot,
      status: r.status === 'committed' ? 'committed' : 'pending',
      version: r.version === null || r.version === undefined ? null : Number(r.version),
      createdAt: String(r.created_at),
      committedAt: r.committed_at === null || r.committed_at === undefined ? null : String(r.committed_at),
    };
  }

  /** 读取某轮重算已落库的步骤,按链序(ord)升序。 */
  getRecomputeSteps(runId: string): RecomputeStep[] {
    const rows = this.db
      .prepare(`SELECT * FROM recompute_runs WHERE run_id = ? ORDER BY ord`)
      .all(runId) as Array<Record<string, unknown>>;
    return rows.map(r => this.toStep(r));
  }

  getRecomputeStep(runId: string, ord: number): RecomputeStep | null {
    const r = this.db
      .prepare(`SELECT * FROM recompute_runs WHERE run_id = ? AND ord = ?`)
      .get(runId, ord) as Record<string, unknown> | undefined;
    return r ? this.toStep(r) : null;
  }

  /** 持久化一个已算好的步骤(独立事务,立即落盘,中断不丢)。 */
  putRecomputeStep(runId: string, ord: number, row: NewLedger, createdAt: string): void {
    this.db.exec('BEGIN');
    try {
      this.db.prepare(
        `INSERT INTO recompute_runs
           (run_id, ord, maintenance, handover, supersedes, input_hash, impact, snapshot,
            status, version, created_at, committed_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, ?, NULL)
         ON CONFLICT(run_id, ord) DO UPDATE SET
           maintenance = excluded.maintenance, handover = excluded.handover,
           supersedes = excluded.supersedes, input_hash = excluded.input_hash,
           impact = excluded.impact, snapshot = excluded.snapshot,
           status = 'pending', version = NULL,
           created_at = excluded.created_at, committed_at = NULL`,
      ).run(
        runId, ord, row.maintenance, row.handover, row.supersedes, row.inputHash,
        JSON.stringify(row.impact), JSON.stringify(row.snapshot), createdAt,
      );
      this.db.exec('COMMIT');
    } catch (e) {
      this.db.exec('ROLLBACK');
      throw e;
    }
  }

  /**
   * 丢弃某轮重算中 ord >= fromOrd 的待提交步骤(失效进度)。
   * 用于:恢复时发现新替换单已入库、或上游环变化导致后续步骤不再有效。
   */
  discardRecomputeTail(runId: string, fromOrd: number): number {
    const res = this.db.prepare(
      `DELETE FROM recompute_runs WHERE run_id = ? AND ord >= ? AND status = 'pending'`,
    ).run(runId, fromOrd);
    return Number(res.changes);
  }

  /**
   * 一次换账:单事务内把整段待提交步骤写入账版链、标记步骤为已提交、
   * 清除进行中标记。任何一步失败整体回滚,HTTP 端看到的账链始终完整一致。
   * 返回各步骤按链序对应的新账版号。
   */
  commitRecomputeRun(runId: string): number[] {
    const runId0 = runId;
    const versions: number[] = [];
    this.db.exec('BEGIN');
    try {
      const active = this.db.prepare(`SELECT v FROM meta WHERE k = 'recompute_run'`).get() as { v: string } | undefined;
      if (!active || String(active.v) !== runId0) {
        throw new Error(`重算运行 ${runId0} 已不是当前进行中的运行,拒绝换账`);
      }
      const rows = this.db.prepare(
        `SELECT * FROM recompute_runs WHERE run_id = ? AND status = 'pending' ORDER BY ord`,
      ).all(runId0) as Array<Record<string, unknown>>;
      if (rows.length === 0) {
        this.db.prepare(`DELETE FROM meta WHERE k = 'recompute_run'`).run();
        this.db.exec('COMMIT');
        return [];
      }
      const now = new Date().toISOString();
      const insLedger = this.db.prepare(
        `INSERT INTO ledgers (handover, maintenance, supersedes, input_hash, impact, snapshot, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
      );
      const updStep = this.db.prepare(
        `UPDATE recompute_runs SET status = 'committed', version = ?, committed_at = ?
         WHERE run_id = ? AND ord = ?`,
      );
      for (const r of rows) {
        const res = insLedger.run(
          String(r.handover), String(r.maintenance),
          r.supersedes === null || r.supersedes === undefined ? null : Number(r.supersedes),
          String(r.input_hash), String(r.impact), String(r.snapshot), String(r.created_at),
        );
        const version = Number(res.lastInsertRowid);
        versions.push(version);
        updStep.run(version, now, runId0, Number(r.ord));
      }
      this.db.prepare(`DELETE FROM meta WHERE k = 'recompute_run'`).run();
      this.db.exec('COMMIT');
    } catch (e) {
      this.db.exec('ROLLBACK');
      throw e;
    }
    return versions;
  }

  private toLedgerRow(r: Record<string, unknown>): LedgerRow {
    return {
      version: Number(r.version),
      handover: String(r.handover),
      maintenance: String(r.maintenance),
      supersedes: r.supersedes === null || r.supersedes === undefined ? null : Number(r.supersedes),
      inputHash: String(r.input_hash),
      impact: JSON.parse(String(r.impact)) as Impact,
      snapshot: JSON.parse(String(r.snapshot)) as Snapshot,
      createdAt: String(r.created_at),
    };
  }

  getLedger(version: number): LedgerRow | null {
    const r = this.db.prepare(`SELECT * FROM ledgers WHERE version = ?`).get(version) as Record<string, unknown> | undefined;
    return r ? this.toLedgerRow(r) : null;
  }

  latestLedger(): LedgerRow | null {
    const r = this.db.prepare(`SELECT * FROM ledgers ORDER BY version DESC LIMIT 1`).get() as Record<string, unknown> | undefined;
    return r ? this.toLedgerRow(r) : null;
  }

  latestLedgerForHandover(handover: string): LedgerRow | null {
    const r = this.db
      .prepare(`SELECT * FROM ledgers WHERE handover = ? ORDER BY version DESC LIMIT 1`)
      .get(handover) as Record<string, unknown> | undefined;
    return r ? this.toLedgerRow(r) : null;
  }

  latestLedgerForMaintenance(maintenance: string): LedgerRow | null {
    const r = this.db
      .prepare(`SELECT * FROM ledgers WHERE maintenance = ? ORDER BY version DESC LIMIT 1`)
      .get(maintenance) as Record<string, unknown> | undefined;
    return r ? this.toLedgerRow(r) : null;
  }

  /** 进行中级联重算的进度(供 /api/status 展示),没有进行中的运行时返回 null。 */
  recomputeStatus(): { runId: string; stepsDone: number; maintenances: string[] } | null {
    const runId = this.getActiveRecomputeRun();
    if (!runId) return null;
    const rows = this.db
      .prepare(
        `SELECT ord, maintenance FROM recompute_runs
         WHERE run_id = ? AND status = 'pending' ORDER BY ord`,
      )
      .all(runId) as Array<{ ord: number; maintenance: string }>;
    return {
      runId,
      stepsDone: rows.length,
      maintenances: rows.map(r => String(r.maintenance)),
    };
  }
}
