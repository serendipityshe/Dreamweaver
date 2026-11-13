import { randomUUID } from "node:crypto";
import { existsSync, lstatSync, mkdirSync, readdirSync, realpathSync, rmdirSync, unlinkSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { harnessError } from "../contract/index.js";

const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
const INDEX = ".harness-fragments-index.sqlite", LOCK = ".lease.sqlite", APPLICATION = 0x48465331;

/** Disposable file ownership, separate from business authority. The registry
 * serializes creation, quota and collection; an OS-held SQLite write lock
 * proves a live owner cannot be collected, even after coordinator fencing.
 * Neither a PID nor an expiry timestamp is evidence of process death. */
export class DiskFragmentLease {
  readonly id = randomUUID();
  readonly directory: string;
  readonly #parent: string;
  readonly #db: DatabaseSync;
  #lock?: DatabaseSync;
  #closed = false;
  #failed = false;

  constructor(parent: string, readonly quota: number) {
    this.#parent = realpathSync(parent);
    this.directory = this.#directory(this.id);
    const index = join(this.#parent, INDEX);
    if (existsSync(index) && !lstatSync(index).isFile()) throw harnessError("INVALID_ARGUMENT", "Invalid staging registry path");
    this.#db = new DatabaseSync(index, { allowExtension: false });
    try {
      this.#db.exec("PRAGMA busy_timeout=0; PRAGMA synchronous=FULL; PRAGMA trusted_schema=OFF");
      this.#transaction(() => {
        const application = this.#db.prepare("PRAGMA application_id").get()!.application_id;
        if (application === 0 && this.#db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().length === 0) {
          this.#db.exec(`PRAGMA application_id=${APPLICATION}; CREATE TABLE owners(id TEXT PRIMARY KEY,quota INTEGER NOT NULL CHECK(quota>0),reserved INTEGER NOT NULL CHECK(reserved>=0),state TEXT NOT NULL CHECK(state IN ('preparing','active'))) STRICT`);
        } else if (application !== APPLICATION) throw harnessError("INVALID_ARGUMENT", "Unrecognized staging registry");
        this.#collect();
        if ((this.#db.prepare("SELECT count(*) n FROM owners").get()!.n as number) >= 128) throw harnessError("LIMIT_EXCEEDED", "Too many staging owners");
        this.#db.prepare("INSERT INTO owners(id,quota,reserved,state) VALUES(?,?,0,'preparing')").run(this.id, quota);
      });
      // Persist intent before creating files. If startup dies in either phase,
      // a collector knows the exact owned path. Activation rechecks that intent.
      this.#transaction(() => {
        if (!this.#db.prepare("SELECT id FROM owners WHERE id=? AND state='preparing'").get(this.id)) throw harnessError("STATE_CONFLICT", "Staging activation was superseded");
        mkdirSync(this.directory, { mode: 0o700 });
        const lock = new DatabaseSync(join(this.directory, LOCK), { allowExtension: false });
        this.#lock = lock;
        lock.exec("PRAGMA busy_timeout=0; CREATE TABLE owner(singleton INTEGER PRIMARY KEY); BEGIN EXCLUSIVE");
        this.#db.prepare("UPDATE owners SET state='active' WHERE id=?").run(this.id);
      });
    } catch (error) {
      try { this.#lock?.close(); } finally { this.#db.close(); }
      throw error;
    }
  }

  reserve(bytes: number): void {
    if (!Number.isSafeInteger(bytes) || bytes <= 0) throw harnessError("INVALID_ARGUMENT", "Invalid staging reservation");
    this.#transaction(() => {
      let totals = this.#db.prepare("SELECT sum(reserved) used,min(quota) capacity FROM owners").get()!;
      if (Number.isSafeInteger(totals.used) && Number.isSafeInteger(totals.capacity) && bytes > (totals.capacity as number) - (totals.used as number)) {
        this.#collect();
        totals = this.#db.prepare("SELECT sum(reserved) used,min(quota) capacity FROM owners").get()!;
      }
      if (!Number.isSafeInteger(totals.used) || !Number.isSafeInteger(totals.capacity)
          || bytes > (totals.capacity as number) - (totals.used as number)) throw harnessError("LIMIT_EXCEEDED", "Shared disk fragment quota is full");
      if (this.#db.prepare("UPDATE owners SET reserved=reserved+? WHERE id=? AND state='active'").run(bytes, this.id).changes !== 1)
        throw harnessError("NODE_STATE_INVALID", "Staging owner is unavailable");
    });
  }

  release(bytes: number): void {
    if (!Number.isSafeInteger(bytes) || bytes <= 0) throw harnessError("INVALID_ARGUMENT", "Invalid staging release");
    this.#transaction(() => {
      if (this.#db.prepare("UPDATE owners SET reserved=reserved-? WHERE id=? AND state='active' AND reserved>=?").run(bytes, this.id, bytes).changes !== 1)
        throw harnessError("NODE_STATE_INVALID", "Staging reservation is unavailable");
    });
  }

  close(): void {
    if (this.#closed) return;
    try {
      // Caller has stopped and drained its writes before relinquishing the lock.
      this.#lock?.close(); this.#lock = undefined;
      if (!this.#failed) this.#transaction(() => this.#collectOwner(this.id, "active"));
    } finally { this.#closed = true; this.#db.close(); }
  }

  #transaction<T>(operation: () => T): T {
    if (this.#closed || this.#failed) throw harnessError("NODE_STATE_INVALID", "Staging registry is unavailable");
    this.#db.exec("BEGIN IMMEDIATE");
    let committing = false;
    try { const result = operation(); committing = true; this.#db.exec("COMMIT"); return result; }
    catch (error) {
      if (committing) this.#failed = true;
      try { this.#db.exec("ROLLBACK"); } catch { this.#failed = true; }
      throw error;
    }
  }

  #directory(id: string): string {
    if (!UUID.test(id)) throw harnessError("INTERNAL_ERROR", "Invalid owned staging directory");
    // The registry stores only an ID, never a caller-supplied path.
    return join(this.#parent, `.harness-fragments-${id}`);
  }

  #collect(): void {
    for (const row of this.#db.prepare("SELECT id,state FROM owners WHERE id<>? ORDER BY id").all(this.id))
      this.#collectOwner(row.id as string, row.state as string);
  }

  #collectOwner(id: string, state: string): void {
    const directory = this.#directory(id);
    if (!existsSync(directory)) { this.#db.prepare("DELETE FROM owners WHERE id=?").run(id); return; }
    if (!lstatSync(directory).isDirectory() || lstatSync(directory).isSymbolicLink()) return;
    const lockPath = join(directory, LOCK);
    if (!existsSync(lockPath) && state !== "preparing") return;
    if (existsSync(lockPath) && (!lstatSync(lockPath).isFile() || lstatSync(lockPath).isSymbolicLink())) return;
    let lock: DatabaseSync | undefined;
    try {
      if (existsSync(lockPath)) {
        lock = new DatabaseSync(lockPath, { allowExtension: false });
        lock.exec("PRAGMA busy_timeout=0; BEGIN EXCLUSIVE");
      }
    } catch { lock?.close(); return; } // A live OS lock, or an unreadable proof, prevents collection.
    try {
      // Inspect payload files only AFTER taking the lifetime lock. A live
      // owner can otherwise expire/unlink a payload concurrently with this scan.
      const names = readdirSync(directory);
      if (names.some(name => !UUID.test(name) && name !== LOCK && name !== `${LOCK}-journal`
          || !lstatSync(join(directory, name)).isFile() || lstatSync(join(directory, name)).isSymbolicLink())) return;
      for (const name of names) if (UUID.test(name)) unlinkSync(join(directory, name));
    }
    finally { lock?.close(); }
    for (const name of [`${LOCK}-journal`, LOCK]) if (existsSync(join(directory, name))) unlinkSync(join(directory, name));
    rmdirSync(directory);
    this.#db.prepare("DELETE FROM owners WHERE id=?").run(id);
  }
}
