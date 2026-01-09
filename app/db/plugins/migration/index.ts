import type { Sql } from "../../sql.server";
import { getSql } from "../../sql.server";

export type MigrationFn = (sql: Sql) => Promise<void>;

export interface Migration {
  name: string;
  up: MigrationFn;
}

export interface MigrationResult {
  applied: string[];
  batch: number;
}

export interface AppliedMigration {
  id: number;
  name: string;
  batch: number;
  migrationTime: Date;
}

export interface MigrationStats {
  totalMigrations: number;
  totalBatches: number;
  lastMigrationName: string | null;
  lastMigrationTime: Date | null;
  lastBatch: number;
}

export async function getNextBatch(): Promise<number> {
  const sql = getSql();
  const row = await sql`
    select
      coalesce(max(batch), 0) + 1 as next_batch
    from
      migrations.migrations
  `.first<{ nextBatch: number }>();

  return row.nextBatch;
}

export async function getCurrentBatch(): Promise<number> {
  const sql = getSql();
  const row = await sql`
    select
      coalesce(max(batch), 0) as current_batch
    from
      migrations.migrations
  `.first<{ currentBatch: number }>();

  return row.currentBatch;
}

export async function recordMigration(
  migrationName: string,
  batchNumber?: number
): Promise<AppliedMigration> {
  const sql = getSql();
  const actualBatch = batchNumber ?? (await getNextBatch());

  const row = await sql`
    insert into
      migrations.migrations (name, batch)
    values
      (
        ${migrationName},
        ${actualBatch}
      )
    returning
      id,
      name,
      batch,
      migration_time
  `.first<{
    id: number;
    name: string;
    batch: number;
    migrationTime: Date;
  }>();

  return {
    id: row.id,
    name: row.name,
    batch: row.batch,
    migrationTime: row.migrationTime as Date,
  };
}

export async function hasMigration(migrationName: string): Promise<boolean> {
  const sql = getSql();
  const row = await sql`
    select
      1
    from
      migrations.migrations
    where
      name = ${migrationName}
    limit
      1
  `.first<{ "?column?": number }>();

  return !!row;
}

export async function getAppliedMigrations(): Promise<AppliedMigration[]> {
  const sql = getSql();
  const rows = await sql`
    select
      id,
      name,
      batch,
      migration_time
    from
      migrations.migrations
    order by
      id asc
  `.all<{
    id: number;
    name: string;
    batch: number;
    migrationTime: Date;
  }>();

  return rows.map((r) => ({
    id: r.id,
    name: r.name,
    batch: r.batch,
    migrationTime: r.migrationTime as Date,
  }));
}

export async function getMigrationsByBatch(
  batchNumber: number
): Promise<AppliedMigration[]> {
  const sql = getSql();
  const rows = await sql`
    select
      id,
      name,
      batch,
      migration_time
    from
      migrations.migrations
    where
      batch = ${batchNumber}
    order by
      id asc
  `.all<{
    id: number;
    name: string;
    batch: number;
    migrationTime: Date;
  }>();

  return rows.map((r) => ({
    id: r.id,
    name: r.name,
    batch: r.batch,
    migrationTime: r.migrationTime as Date,
  }));
}

export async function getLatestBatchMigrations(): Promise<AppliedMigration[]> {
  const currentBatch = await getCurrentBatch();
  if (currentBatch === 0) {
    return [];
  }
  return getMigrationsByBatch(currentBatch);
}

export async function getPendingMigrations(
  migrationNames: string[]
): Promise<string[]> {
  const sql = getSql();
  if (migrationNames.length === 0) {
    return [];
  }

  const appliedRows = await sql`
    select
      name
    from
      migrations.migrations
    where
      name in (${sql.join(migrationNames.map((name) => sql.literal(name)))})
  `.all<{ name: string }>();

  const appliedSet = new Set(appliedRows.map((r) => r.name));

  return migrationNames.filter((name) => !appliedSet.has(name));
}

export async function getStats(): Promise<MigrationStats> {
  const sql = getSql();
  const row = await sql`
    select
      (
        select
          count(*)::integer
        from
          migrations.migrations
      ) as total_migrations,
      (
        select
          count(distinct batch)::integer
        from
          migrations.migrations
      ) as total_batches,
      (
        select
          name
        from
          migrations.migrations
        order by
          id desc
        limit
          1
      ) as last_migration_name,
      (
        select
          migration_time
        from
          migrations.migrations
        order by
          id desc
        limit
          1
      ) as last_migration_time,
      coalesce(
        (
          select
            max(batch)
          from
            migrations.migrations
        ),
        0
      ) as last_batch
  `.first<{
    totalMigrations: number;
    totalBatches: number;
    lastMigrationName: string | null;
    lastMigrationTime: Date | null;
    lastBatch: number;
  }>();

  return row
    ? {
        totalMigrations: row.totalMigrations,
        totalBatches: row.totalBatches,
        lastMigrationName: row.lastMigrationName,
        lastMigrationTime: row.lastMigrationTime as Date | null,
        lastBatch: row.lastBatch,
      }
    : {
        totalMigrations: 0,
        totalBatches: 0,
        lastMigrationName: null,
        lastMigrationTime: null,
        lastBatch: 0,
      };
}

export async function runMigrations(
  migrations: Migration[]
): Promise<MigrationResult> {
  const sql = getSql();
  return await sql.tx(async (sql) => {
    const migrationNames = migrations.map((m) => m.name);

    const pendingNames = await getPendingMigrations(migrationNames);
    const pendingNamesSet = new Set(pendingNames);
    const pendingMigrations = migrations.filter((m) =>
      pendingNamesSet.has(m.name)
    );

    if (pendingMigrations.length === 0) {
      return { applied: [], batch: 0 };
    }

    const batch = await getNextBatch();

    const applied: string[] = [];
    for (const migration of pendingMigrations) {
      await migration.up(sql);

      await recordMigration(migration.name, batch);
      applied.push(migration.name);
    }

    return { applied, batch };
  });
}

export async function getMigrationStatus(migrations: Migration[]): Promise<{
  applied: Array<{ name: string; batch: number; migrationTime: Date }>;
  pending: string[];
  stats: MigrationStats;
}> {
  const appliedMigrations = await getAppliedMigrations();

  const migrationNames = migrations.map((m) => m.name);
  const pending = await getPendingMigrations(migrationNames);

  const stats = await getStats();

  return {
    applied: appliedMigrations.map((r) => ({
      name: r.name,
      batch: r.batch,
      migrationTime: r.migrationTime,
    })),
    pending,
    stats,
  };
}
