/**
 * 文件回放源:把本地目录当作源系统,读取其中的 *.jsonl(每行一个批次)。
 * - fetch:流式逐行读取,最多收集 limit 条 seq > afterSeq 的批次,内存占用有界;
 * - ack:源端清理已确认批次(重写文件剔除 seq <= uptoSeq 的行,空文件删除)。
 */

import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import { parseBatch, type Batch } from './model.js';
import type { Source } from './sync.js';

export class FileSource implements Source {
  constructor(private dir: string) {}

  describe(): string {
    return `file://${this.dir}`;
  }

  private async files(): Promise<string[]> {
    try {
      const names = await fsp.readdir(this.dir);
      return names.filter(n => n.endsWith('.jsonl')).sort().map(n => path.join(this.dir, n));
    } catch (e) {
      if ((e as NodeJS.ErrnoException)?.code === 'ENOENT') return [];
      throw e;
    }
  }

  async fetch(afterSeq: number, limit: number): Promise<Batch[]> {
    const out: Batch[] = [];
    for (const file of await this.files()) {
      if (out.length >= limit) break;
      const rl = readline.createInterface({
        input: fs.createReadStream(file, 'utf8'),
        crlfDelay: Infinity,
      });
      let lineNo = 0;
      try {
        for await (const line of rl) {
          lineNo++;
          const t = line.trim();
          if (!t) continue;
          let b: Batch;
          try {
            b = parseBatch(JSON.parse(t));
          } catch (e) {
            throw new Error(`${file}:${lineNo} ${e instanceof Error ? e.message : String(e)}`);
          }
          if (b.seq > afterSeq) {
            out.push(b);
            if (out.length >= limit) break; // 提前退出,for-await 会自动关闭流
          }
        }
      } finally {
        rl.close();
      }
    }
    out.sort((a, b) => a.seq - b.seq);
    return out;
  }

  async ack(uptoSeq: number): Promise<void> {
    for (const file of await this.files()) {
      const text = await fsp.readFile(file, 'utf8');
      const kept: string[] = [];
      let changed = false;
      for (const line of text.split('\n')) {
        const t = line.trim();
        if (!t) continue;
        let seq = Number.NaN;
        try {
          seq = Number((JSON.parse(t) as { seq?: unknown }).seq);
        } catch {
          /* 无法解析的行保留,等待人工处理 */
        }
        if (Number.isInteger(seq) && seq <= uptoSeq) {
          changed = true;
          continue;
        }
        kept.push(t);
      }
      if (!changed) continue;
      if (kept.length === 0) {
        await fsp.unlink(file);
        continue;
      }
      const tmp = `${file}.tmp`;
      await fsp.writeFile(tmp, `${kept.join('\n')}\n`, 'utf8');
      await fsp.rename(tmp, file);
    }
  }
}
