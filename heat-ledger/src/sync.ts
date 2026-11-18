/**
 * 同步引擎:从源系统拉取批次 → 落库(去重/替换) → 推进游标 → 回执确认 → 重算账版。
 * 每轮最多处理 pageSize 条批次,等待补齐的日志只占用有界内存。
 */

import type { Batch } from './model.js';
import type { Store } from './store.js';
import type { LedgerComputer } from './ledger.js';

export interface Source {
  /** 拉取 seq > afterSeq 的批次,最多 limit 条(按 seq 升序)。 */
  fetch(afterSeq: number, limit: number): Promise<Batch[]>;
  /** 回执确认:seq <= uptoSeq 的批次服务方已安全落库,源端可清理。 */
  ack(uptoSeq: number): Promise<void>;
  describe(): string;
}

export class SyncEngine {
  idle = true;             // 上一轮既无新批次也无新账版
  lastReceived = 0;        // 上一轮新入库(非重复)的批次数
  lastVersions: number[] = [];
  private stopped = false;
  private timer: NodeJS.Timeout | null = null;
  private lastAcked = -1;

  constructor(
    private store: Store,
    private source: Source,
    private computer: LedgerComputer,
    private pageSize = 100,
    private pollMs = 500,
  ) {}

  async syncOnce(): Promise<void> {
    const batches = await this.source.fetch(this.store.getCursor(), this.pageSize);
    this.lastReceived = 0;
    for (const b of batches) {
      if (this.store.putBatch(b) !== 'duplicate') this.lastReceived++;
    }
    const cursor = this.store.advanceCursor();
    if (cursor !== this.lastAcked) {
      await this.source.ack(cursor);
      this.lastAcked = cursor;
    }
    this.lastVersions = await this.computer.compute();
    this.idle = batches.length === 0 && this.lastVersions.length === 0;
  }

  start(): void {
    const loop = async (): Promise<void> => {
      if (this.stopped) return;
      try {
        await this.syncOnce();
      } catch (e) {
        console.error('[sync] 同步出错:', e instanceof Error ? e.message : e);
      }
      if (!this.stopped) this.timer = setTimeout(() => void loop(), this.pollMs);
    };
    void loop();
  }

  stop(): void {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
  }
}
