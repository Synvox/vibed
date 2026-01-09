import { SQL, type TransactionSQL } from "bun";

export interface DbInterface {
  exec(query: string): Promise<void>;
  query<T extends object>(
    query: string,
    args: unknown[]
  ): Promise<{ rows: T[] }>;
  transaction<T>(fn: (tx: DbInterface) => Promise<T>): Promise<T>;
}

interface HasUnsafe {
  unsafe<T extends object>(query: string, args?: unknown[]): Promise<T[]>;
}

let savepointCounter = 0;

function wrapSql(
  sql: HasUnsafe,
  transact: <T>(
    fn: (tx: DbInterface) => Promise<T>,
    db: DbInterface
  ) => Promise<T>
): DbInterface {
  const db: DbInterface = {
    async exec(query: string) {
      await sql.unsafe(query);
    },
    async query<T extends object>(queryStr: string, args: unknown[]) {
      const result = await sql.unsafe<T>(queryStr, args);
      return { rows: result } as { rows: T[] };
    },
    async transaction<T>(fn: (tx: DbInterface) => Promise<T>) {
      return transact(fn, db);
    },
  };
  return db;
}

function createDbFromTx(tx: TransactionSQL): DbInterface {
  return wrapSql(tx, async (fn, db) => {
    const savepointName = `sp_${++savepointCounter}`;
    await tx.unsafe(`SAVEPOINT ${savepointName}`);
    try {
      const result = await fn(db);
      await tx.unsafe(`RELEASE SAVEPOINT ${savepointName}`);
      return result;
    } catch (error) {
      await tx.unsafe(`ROLLBACK TO SAVEPOINT ${savepointName}`);
      throw error;
    }
  });
}

export function createDb(connectionUrl: string): DbInterface {
  const sql = new SQL(connectionUrl);
  return wrapSql(sql, async (fn) => {
    return await sql.transaction(async (tx) => fn(createDbFromTx(tx)));
  });
}

// Default database instance
const connectionUrl =
  process.env.DATABASE_URL ?? "postgres://localhost/vibed_dev";

export const db = createDb(connectionUrl);
