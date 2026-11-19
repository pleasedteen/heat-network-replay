/**
 * 交接账计算:按检修顺序链式重放变更日志,为每场完成的交接生成账版。
 *
 * 可中断、可恢复的级联重算:
 * - 每场检修的“输入指纹”= 自身批次指纹 + 上一环输入指纹,任一环变化都会级联
 *   触发其后所有交接的重算(替换单正是借此生效);
 * - 一次重算运行(run)的每一步算完立即持久化到 recompute_runs。进程被强制
 *   中断后重启,先逐环校验已落库步骤:同场输入未变但上游已变的步骤一律重新
 *   验证,验证不过的步骤连同其后所有“失效进度”一次性丢弃,再按当前输入补算;
 * - 已完成且仍有效的交接不重复计算(直接复用既有账版或已持久化的步骤);
 * - 整段算完前 ledgers 表保持不动,HTTP 只能读到上一份完整账;整段完成后单事务
 *   一次换账,结果与从头完整重算完全一致,旧账版与替代关系(supersedes)保留可查。
 *
 * 重放分片让出事件循环,大批次计算不会卡死 HTTP 服务。
 * 测试钩子(仅本地回放使用):
 *   HEAT_HOLD_AFTER_STEPS  持久化 N 个步骤后挂起进程(不退出,HTTP 仍可读旧账);
 *   HEAT_ABORT_AFTER_STEPS 持久化 N 个步骤后立即退出(模拟级联重算被强制中断)。
 */

import { randomUUID } from 'node:crypto';
import { createHash } from 'node:crypto';
import { Network, type Snapshot } from './network.js';
import type { Store, LedgerRow, NewLedger, RecomputeStep } from './store.js';

const YIELD_EVERY_RECORDS = 500;
const yieldLoop = (): Promise<void> => new Promise(resolve => setImmediate(resolve));

export class LedgerComputer {
  /** 同一进程内串行化重算,避免两轮同步并发操作同一运行。 */
  private running: Promise<number[]> | null = null;
  private readonly holdAfterSteps = Number(process.env.HEAT_HOLD_AFTER_STEPS ?? NaN);
  private readonly abortAfterSteps = Number(process.env.HEAT_ABORT_AFTER_STEPS ?? NaN);

  constructor(private store: Store) {}

  compute(): Promise<number[]> {
    if (this.running) return this.running;
    this.running = this.computeInner().finally(() => {
      this.running = null;
    });
    return this.running;
  }

  private async computeInner(): Promise<number[]> {
    const mts = this.store.listMaintenances();

    // 链在第一个未收齐的检修处停住,其后的检修一律等待。
    let end = mts.length;
    for (let i = 0; i < mts.length; i++) {
      if (this.store.partsReceived(mts[i].id) < mts[i].partsExpected) {
        end = i;
        break;
      }
    }

    // 当前链上各环的输入指纹(自身批次指纹 + 上一环指纹)。
    const inputHashes: string[] = [];
    let prevHash = '0';
    for (let i = 0; i < end; i++) {
      prevHash = createHash('sha1')
        .update(`${mts[i].id}|${this.store.stateHash(mts[i].id)}|${prevHash}`)
        .digest('hex');
      inputHashes.push(prevHash);
    }

    // 找到第一个与现行账版不一致的环节,即本段重算的起点。
    let start = -1;
    for (let i = 0; i < end; i++) {
      const existing: LedgerRow | null = this.store.latestLedgerForMaintenance(mts[i].id);
      if (!existing || existing.inputHash !== inputHashes[i]) {
        start = i;
        break;
      }
    }
    if (start === -1) {
      // 全部现行账版仍有效。若库中残留进行中的运行,它已无对应工作,清理掉。
      const stale = this.store.getActiveRecomputeRun();
      if (stale) this.store.abandonRecomputeRun(stale);
      return [];
    }

    const runId = this.store.getActiveRecomputeRun() ?? `run-${Date.now()}-${randomUUID()}`;
    this.store.beginRecomputeRun(runId);
    this.store.clearRecomputeRun(runId);

    // 起点之前的最后一份有效账版,作为本段重放的基础快照(没有则从空网开始)。
    const baseLedger = start > 0 ? this.store.latestLedgerForMaintenance(mts[start - 1].id) : null;
    let baseSnapshot: Snapshot | null = baseLedger?.snapshot ?? null;

    // ---------- 恢复:校验已持久化的步骤,丢弃失效进度 ----------
    let steps = this.store.getRecomputeSteps(runId);
    let resumeIdx = start;
    for (const step of steps) {
      const idx = start + step.ord;
      if (idx >= end || step.maintenance !== mts[idx].id || step.inputHash !== inputHashes[idx]) {
        break; // 同场输入未变但上游已变、或新替换单已入库:本环及后续步骤全部失效
      }
      resumeIdx = idx + 1;
    }
    if (resumeIdx < start + steps.length) {
      this.store.discardRecomputeTail(runId, resumeIdx - start);
      steps = this.store.getRecomputeSteps(runId);
    }

    // ---------- 补算缺失环节,每步立即落盘 ----------
    let persisted = steps.length;
    let prevSnapshot: Snapshot | null =
      persisted > 0 ? steps[steps.length - 1].snapshot : baseSnapshot;

    for (let idx = resumeIdx; idx < end; idx++) {
      const mt = mts[idx];
      const existing: LedgerRow | null = this.store.latestLedgerForMaintenance(mt.id);

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

      const row: NewLedger = {
        handover: mt.handover,
        maintenance: mt.id,
        supersedes: existing?.version ?? null,
        inputHash: inputHashes[idx],
        impact: { lost, restored },
        snapshot: net.snapshot(),
      };
      const ord = idx - start;
      this.store.putRecomputeStep(runId, ord, row, new Date().toISOString());
      persisted++;
      prevSnapshot = row.snapshot;
      await yieldLoop();

      // 测试钩子:模拟重算中途被强制中断 / 挂起。
      if (Number.isFinite(this.abortAfterSteps) && persisted >= this.abortAfterSteps) {
        console.warn(`[ledger] 测试钩子:已持久化 ${persisted} 个步骤,模拟强制中断退出`);
        process.exit(2);
      }
      if (Number.isFinite(this.holdAfterSteps) && persisted >= this.holdAfterSteps) {
        console.warn(`[ledger] 测试钩子:已持久化 ${persisted} 个步骤,挂起重算(HTTP 仍读旧账)`);
        await new Promise<void>(() => { /* 永不 resolve,等待进程被结束 */ });
      }
    }

    // ---------- 整段完成:单事务一次换账 ----------
    return this.store.commitRecomputeRun(runId);
  }

  /** 仅供测试/诊断使用:读取某轮重算已持久化的步骤。 */
  getSteps(runId: string): RecomputeStep[] {
    return this.store.getRecomputeSteps(runId);
  }
}
