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
      CREATE INDEX IF NOT EXISTS idx_ledgers_handover ON ledgers (handover, version);
      CREATE INDEX IF NOT EXISTS idx_ledgers_maintenance ON ledgers (maintenance, version);
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

  /** 同一事务内提交一整段新账版,保证 HTTP 端看到的账链始终完整一致。 */
  addLedgers(rows: NewLedger[]): number[] {
    const versions: number[] = [];
    this.db.exec('BEGIN');
    try {
      const stmt = this.db.prepare(
        `INSERT INTO ledgers (handover, maintenance, supersedes, input_hash, impact, snapshot, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
      );
      for (const r of rows) {
        const res = stmt.run(
          r.handover, r.maintenance, r.supersedes, r.inputHash,
          JSON.stringify(r.impact), JSON.stringify(r.snapshot), new Date().toISOString(),
        );
        versions.push(Number(res.lastInsertRowid));
      }
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
}
