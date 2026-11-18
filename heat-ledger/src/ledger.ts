/**
 * 交接账计算:按检修顺序链式重放变更日志,为每场完成的交接生成账版。
 *
 * - 每场检修的“输入指纹”= 自身批次指纹 + 上一环输入指纹,任一环变化都会级联
 *   触发其后所有交接的重算(替换单正是借此生效);
 * - 新账版在内存中算好后一次性提交(单事务),计算期间 HTTP 查询到的仍是
 *   上一份完整交接账;
 * - 重放分片让出事件循环,大批次计算不会卡死 HTTP 服务。
 */

import { createHash } from 'node:crypto';
import { Network, type Snapshot } from './network.js';
import type { Store, LedgerRow, NewLedger } from './store.js';

const YIELD_EVERY_RECORDS = 500;
const yieldLoop = (): Promise<void> => new Promise(resolve => setImmediate(resolve));

export class LedgerComputer {
  constructor(private store: Store) {}

  /** 重算所有输入已变化的交接账,返回新产生的账版号(可能为空)。 */
  async compute(): Promise<number[]> {
    const mts = this.store.listMaintenances();
    const pending: NewLedger[] = [];
    let prevHash = '0';
    let prevSnapshot: Snapshot | null = null;

    for (const mt of mts) {
      // 交接未齐(批次未收全)时,链在此处停住,后续检修一律等待
      if (this.store.partsReceived(mt.id) < mt.partsExpected) break;

      const inputHash = createHash('sha1')
        .update(`${mt.id}|${this.store.stateHash(mt.id)}|${prevHash}`)
        .digest('hex');
      const existing: LedgerRow | null = this.store.latestLedgerForMaintenance(mt.id);
      if (existing && existing.inputHash === inputHash && pending.length === 0) {
        prevHash = inputHash;
        prevSnapshot = existing.snapshot;
        continue; // 该环账版仍然有效
      }

      // 重算本环:从上一环账版快照出发,应用本场检修的全部记录
      const net: Network = prevSnapshot ? Network.fromSnapshot(prevSnapshot) : new Network();
      const before = new Map<string, boolean>();
      for (const node of net.communities.keys()) before.set(node, net.supplied(node));

      let n = 0;
      for (const batch of this.store.getBatches(mt.id)) {
        for (const rec of batch.records) {
          net.apply(rec);
          if (++n % YIELD_EVERY_RECORDS === 0) await yieldLoop();
        }
      }

      const lost: string[] = [];
      const restored: string[] = [];
      for (const node of net.communities.keys()) {
        const was = before.get(node) === true;
        const now = net.supplied(node);
        if (was && !now) lost.push(node);
        else if (!was && now) restored.push(node);
      }
      lost.sort();
      restored.sort();

      pending.push({
        handover: mt.handover,
        maintenance: mt.id,
        supersedes: existing?.version ?? null,
        inputHash,
        impact: { lost, restored },
        snapshot: net.snapshot(),
      });
      prevHash = inputHash;
      prevSnapshot = net.snapshot();
      await yieldLoop();
    }

    if (pending.length === 0) return [];
    return this.store.addLedgers(pending);
  }
}
