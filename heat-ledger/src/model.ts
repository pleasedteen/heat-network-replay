/**
 * 日志数据模型:批次(一次提交)与记录(一条变更)的定义、解析与校验。
 *
 * 源系统按“批次”提交变更日志,每个批次携带:
 *   seq         全局提交序号(先后),单调递增,重投时保持不变;
 *   maintenance 检修单号;
 *   handover    所属交接;
 *   part/parts  本批在检修内的序号与总批数(范围);
 *   kind        normal=普通批,replacement=替换单(覆盖同 maintenance+part 的旧批);
 *   records     变更记录数组。
 */

export type Direction = 'both' | 'forward';

export type RecordType =
  | 'community.add'
  | 'pipe.add'
  | 'pipe.remove'
  | 'valve.open'
  | 'valve.close'
  | 'source.start'
  | 'source.stop';

export interface LogRecord {
  type: RecordType;
  id?: string;           // 管段或热源编号
  node?: string;         // 小区节点 / 热源所在节点
  name?: string;         // 显示名(小区名、热源名)
  a?: string;            // 管段端点 A
  b?: string;            // 管段端点 B
  direction?: Direction; // 新记录:both=普通管段,forward=止回管段(A→B);旧记录缺省视为 both
  pipe?: string;         // 阀门所在管段
}

export interface Batch {
  seq: number;
  maintenance: string;
  handover: string;
  part: number;
  parts: number;
  kind: 'normal' | 'replacement';
  records: LogRecord[];
}

export class FormatError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'FormatError';
  }
}

function isObj(x: unknown): x is Record<string, unknown> {
  return typeof x === 'object' && x !== null && !Array.isArray(x);
}

function reqStr(o: Record<string, unknown>, f: string, ctx: string): string {
  const v = o[f];
  if (typeof v !== 'string' || v.length === 0) {
    throw new FormatError(`${ctx}: 字段 "${f}" 缺失或不是非空字符串`);
  }
  return v;
}

function optStr(o: Record<string, unknown>, f: string, ctx: string): string | undefined {
  const v = o[f];
  if (v === undefined || v === null) return undefined;
  if (typeof v !== 'string') throw new FormatError(`${ctx}: 字段 "${f}" 不是字符串`);
  return v;
}

function reqPosInt(v: unknown, f: string, ctx: string): number {
  if (typeof v !== 'number' || !Number.isInteger(v) || v < 1) {
    throw new FormatError(`${ctx}: 字段 "${f}" 必须是正整数`);
  }
  return v;
}

const RECORD_TYPES: readonly string[] = [
  'community.add', 'pipe.add', 'pipe.remove',
  'valve.open', 'valve.close', 'source.start', 'source.stop',
];

export function parseRecord(x: unknown, ctx: string): LogRecord {
  if (!isObj(x)) throw new FormatError(`${ctx}: 记录不是对象`);
  if (typeof x.type !== 'string' || !RECORD_TYPES.includes(x.type)) {
    throw new FormatError(`${ctx}: 未知记录类型 ${JSON.stringify(x.type)}`);
  }
  const rec: LogRecord = { type: x.type as RecordType };
  rec.id = optStr(x, 'id', ctx);
  rec.node = optStr(x, 'node', ctx);
  rec.name = optStr(x, 'name', ctx);
  rec.a = optStr(x, 'a', ctx);
  rec.b = optStr(x, 'b', ctx);
  rec.pipe = optStr(x, 'pipe', ctx);
  const dir = optStr(x, 'direction', ctx);
  if (dir !== undefined) {
    if (dir !== 'both' && dir !== 'forward') {
      throw new FormatError(`${ctx}: direction 只能是 "both"(普通管段)或 "forward"(止回管段)`);
    }
    rec.direction = dir;
  }
  const need = (v: string | undefined, f: string): void => {
    if (!v) throw new FormatError(`${ctx}: ${rec.type} 记录缺少字段 "${f}"`);
  };
  switch (rec.type) {
    case 'community.add': need(rec.node, 'node'); break;
    case 'pipe.add': need(rec.id, 'id'); need(rec.a, 'a'); need(rec.b, 'b'); break;
    case 'pipe.remove': need(rec.id, 'id'); break;
    case 'valve.open':
    case 'valve.close': need(rec.pipe, 'pipe'); break;
    case 'source.start': need(rec.id, 'id'); need(rec.node, 'node'); break;
    case 'source.stop': need(rec.id, 'id'); break;
  }
  return rec;
}

export function parseBatch(x: unknown): Batch {
  if (!isObj(x)) throw new FormatError('批次不是 JSON 对象');
  const ctx = `批次(seq=${String(x.seq)})`;
  const seq = reqPosInt(x.seq, 'seq', ctx);
  const maintenance = reqStr(x, 'maintenance', ctx);
  const handover = reqStr(x, 'handover', ctx);
  const part = reqPosInt(x.part, 'part', ctx);
  const parts = reqPosInt(x.parts, 'parts', ctx);
  if (part > parts) throw new FormatError(`${ctx}: part(${part}) 超过 parts(${parts})`);
  const kind = x.kind === undefined ? 'normal' : x.kind;
  if (kind !== 'normal' && kind !== 'replacement') {
    throw new FormatError(`${ctx}: kind 只能是 "normal" 或 "replacement"`);
  }
  if (!Array.isArray(x.records)) throw new FormatError(`${ctx}: records 必须是数组`);
  return {
    seq, maintenance, handover, part, parts, kind,
    records: x.records.map((r, i) => parseRecord(r, `${ctx} 记录#${i + 1}`)),
  };
}
