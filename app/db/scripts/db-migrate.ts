import { type Migration, runMigrations } from "../plugins/migration/index";
import { sql } from "../sql.server";

const migrations = [await importMigration("init")] as Migration[];

export async function importMigration(name: string) {
  return {
    name,
    ...(await import(`../migrations/${name}.ts`)),
  };
}

await sql`
  create schema if not exists migrations;
  create table if not exists migrations.migrations (
    id serial primary key,
    name text not null unique,
    batch integer not null,
    migration_time timestamptz not null default now()
  );

  create index if not exists idx_migrations_name on migrations.migrations (name);
  create index if not exists idx_migrations_batch on migrations.migrations (batch);
`.exec();

await runMigrations(migrations);
