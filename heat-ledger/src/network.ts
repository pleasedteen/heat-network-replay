/**
 * 供热管网模型:应用变更记录、计算“某小区能接通哪些在用热源”。
 *
 * 热流方向约定:
 *   - 普通管段(direction=both,旧记录缺省):两端均可送热;
 *   - 止回管段(direction=forward):只能按标定方向 A→B 送热。
 * 小区有供热 ⟺ 从该小区逆着热流方向能到达至少一个“在用”(active)热源。
 * 阀门关闭的管段不通热。
 */

import type { LogRecord } from './model.js';

export interface PipeState {
  id: string;
  a: string;
  b: string;
  direction: 'both' | 'forward';
  valveOpen: boolean;
}

export interface SourceState {
  id: string;
  node: string;
  name?: string;
  active: boolean;
}

export interface Snapshot {
  pipes: PipeState[];
  sources: SourceState[];
  communities: Record<string, string>; // 节点 -> 小区名
}

export class Network {
  readonly pipes = new Map<string, PipeState>();
  readonly sources = new Map<string, SourceState>();
  readonly communities = new Map<string, string>();

  static fromSnapshot(s: Snapshot): Network {
    const net = new Network();
    for (const p of s.pipes) net.pipes.set(p.id, { ...p });
    for (const src of s.sources) net.sources.set(src.id, { ...src });
    for (const [node, name] of Object.entries(s.communities)) net.communities.set(node, name);
    return net;
  }

  snapshot(): Snapshot {
    return {
      pipes: [...this.pipes.values()].map(p => ({ ...p })),
      sources: [...this.sources.values()].map(s => ({ ...s })),
      communities: Object.fromEntries(this.communities),
    };
  }

  /** 应用一条变更记录(重放按批次内顺序逐条执行)。 */
  apply(rec: LogRecord): void {
    switch (rec.type) {
      case 'community.add':
        this.communities.set(rec.node!, rec.name ?? rec.node!);
        break;
      case 'pipe.add':
        // 旧记录没有 direction 字段,按普通管段(双向)处理
        this.pipes.set(rec.id!, {
          id: rec.id!, a: rec.a!, b: rec.b!,
          direction: rec.direction ?? 'both',
          valveOpen: true,
        });
        break;
      case 'pipe.remove':
        this.pipes.delete(rec.id!);
        break;
      case 'valve.open': {
        const p = this.pipes.get(rec.pipe!);
        if (p) p.valveOpen = true;
        break;
      }
      case 'valve.close': {
        const p = this.pipes.get(rec.pipe!);
        if (p) p.valveOpen = false;
        break;
      }
      case 'source.start': {
        const prev = this.sources.get(rec.id!);
        this.sources.set(rec.id!, {
          id: rec.id!,
          node: rec.node ?? prev?.node ?? '',
          name: rec.name ?? prev?.name,
          active: true,
        });
        break;
      }
      case 'source.stop': {
        const s = this.sources.get(rec.id!);
        if (s) s.active = false;
        break;
      }
    }
  }

  /**
   * 从 from 节点出发,逆热流方向可达的在用热源编号列表(升序)。
   * 普通管段双向可走;止回管段 A→B 只能从 B 侧走向 A 侧(即 B 的上游是 A)。
   */
  reachableSources(from: string): string[] {
    const upstream = new Map<string, Set<string>>();
    const addUpstream = (src: string, dst: string): void => {
      let set = upstream.get(dst);
      if (!set) { set = new Set(); upstream.set(dst, set); }
      set.add(src);
    };
    for (const p of this.pipes.values()) {
      if (!p.valveOpen) continue;
      if (p.direction === 'both') {
        addUpstream(p.a, p.b);
        addUpstream(p.b, p.a);
      } else {
        addUpstream(p.a, p.b); // 热流 A→B,B 的上游是 A
      }
    }
    const seen = new Set<string>([from]);
    const queue: string[] = [from];
    while (queue.length > 0) {
      const n = queue.shift()!;
      for (const u of upstream.get(n) ?? []) {
        if (!seen.has(u)) { seen.add(u); queue.push(u); }
      }
    }
    const found: string[] = [];
    for (const s of this.sources.values()) {
      if (s.active && seen.has(s.node)) found.push(s.id);
    }
    return found.sort();
  }

  /** 小区有供热 ⟺ 能接通任一在用热源。 */
  supplied(node: string): boolean {
    return this.reachableSources(node).length > 0;
  }
}
