-- SQL Filesystem with Version Control (schema: fs)
-- =================================================
--
-- ## High-level model (Git-like, but linear)
--
-- - **Repositories** (`fs.repositories`) are virtual drives/projects.
-- - **Branches** (`fs.branches`) are named pointers to a single head commit
--   (or `NULL` for an empty branch).
-- - **Commits** (`fs.commits`) form a parent-linked tree where each commit has
--   **at most one parent** (`parent_commit_id`). Only the first/root commit in a
--   repository may have `parent_commit_id IS NULL`.
--   Merge commits remain linear by recording the merged source in
--   `merged_from_commit_id`.
-- - **Files** (`fs.files`) are stored as immutable *per-commit deltas*:
--   a commit writes a set of paths; reads/snapshots resolve by walking ancestors
--   to find the most recent version of each path, honoring tombstones.
--
-- Because the history remains linear, merges and rebases are implemented by
-- **replaying net file changes** onto a chosen base commit (or fast-forwarding
-- when possible). Merge commits record the merged source via
-- `merged_from_commit_id`.
--
-- ## Paths / symlinks / deletions
--
-- - All stored file paths are canonical **absolute** paths (always start with
--   `/`), use `/` separators (Windows `\` is accepted on input), collapse `//`,
--   and remove trailing slashes (except `/` itself).
-- - Paths are validated for cross-platform safety (Windows + Unix).
-- - A row with `is_symlink = TRUE` represents a symlink; `content` stores the
--   normalized absolute target path.
-- - A row with `is_deleted = TRUE` is a tombstone. A tombstoned path resolves to
--   "missing" in snapshots and reads return `NULL`.
-- ========================================
-- SCHEMA
-- ========================================
create schema if not exists fs;
-- ========================================
-- TABLE DEFINITIONS
-- ========================================
/*
 fs.repositories
 ---------------
 represents a repository (a virtual drive / project root).
 
 columns:
 - id (uuid): repository identifier (default: uuidv7()).
 - name (text): human-readable unique name.
 - default_branch_id (uuid, nullable): points to the default branch row.
 - created_at (timestamptz): creation timestamp (default: now()).
 */
create table fs.repositories (
  id uuid primary key default uuidv7(),
  name text not null unique,
  default_branch_id uuid,
  created_at timestamptz not null default now()
);
/*
 fs.commits
 ----------
 represents a commit node in a repository's commit graph.
 
 invariants:
 - each commit belongs to exactly one repository (`repository_id`).
 - each commit has **at most one parent** (`parent_commit_id`).
 - only the first/root commit per repository may have `parent_commit_id is null`.
 - parents must be in the **same repository** (enforced by a composite fk).
 
 columns:
 - id (uuid): commit identifier.
 - repository_id (uuid): owning repository (fk → fs.repositories).
 - parent_commit_id (uuid, nullable): parent commit in the same repository.
 - merged_from_commit_id (uuid, nullable): the "other" side of a merge.
 - message (text): commit message.
 - created_at (timestamptz): creation timestamp.
 */
create table fs.commits (
  id uuid primary key default uuidv7(),
  repository_id uuid not null references fs.repositories (id) on delete cascade,
  parent_commit_id uuid,
  merged_from_commit_id uuid,
  message text not null,
  created_at timestamptz not null default now(),
  constraint commits_id_repository_id_unique unique (id, repository_id),
  constraint commits_parent_same_repo_fk foreign key (parent_commit_id, repository_id) references fs.commits (id, repository_id) on delete cascade,
  constraint commits_merged_from_same_repo_fk foreign key (merged_from_commit_id, repository_id) references fs.commits (id, repository_id) on delete cascade
);
/*
 fs.branches
 -----------
 represents a named branch pointer within a repository.
 
 columns:
 - id (uuid): branch identifier.
 - repository_id (uuid): owning repository.
 - name (text): branch name, unique per repository.
 - head_commit_id (uuid, nullable): head commit for this branch.
 - created_at (timestamptz): creation timestamp.
 */
create table fs.branches (
  id uuid primary key default uuidv7(),
  repository_id uuid not null references fs.repositories (id) on delete cascade,
  name text not null,
  head_commit_id uuid,
  created_at timestamptz not null default now(),
  unique (repository_id, name),
  constraint branches_id_repository_id_unique unique (id, repository_id),
  constraint branches_head_commit_same_repo_fk foreign key (head_commit_id, repository_id) references fs.commits (id, repository_id)
);
/*
 fs.files
 --------
 stores file writes (and deletions) that occur in a given commit.
 
 file states:
 - normal file: `is_deleted = false`, `is_symlink = false`, `content` is file text.
 - tombstone delete: `is_deleted = true` (path is deleted at that commit).
 - symlink: `is_symlink = true` and `content` is the normalized absolute target.
 - rename/move: `previous_path` is set to the old path (implicitly deletes old path).
 
 columns:
 - id (uuid): file row identifier.
 - commit_id (uuid): commit containing this file delta (fk → fs.commits).
 - path (text): canonical absolute path (must be a valid file path, not root `/`).
 - previous_path (text, nullable): if set, indicates the file was renamed/moved from this path.
 - content (text): file content or (when symlink) normalized target path.
 - is_deleted (boolean): tombstone flag.
 - is_symlink (boolean): symlink flag.
 - created_at (timestamptz): insert timestamp.
 */
create table fs.files (
  id uuid primary key default uuidv7(),
  commit_id uuid not null references fs.commits (id) on delete cascade,
  path text not null,
  previous_path text,
  content text not null,
  is_deleted boolean not null default false,
  is_symlink boolean not null default false,
  created_at timestamptz not null default now(),
  unique (commit_id, path)
);
/*
 repository ↔ default branch relationship
 --------------------------------------
 we want `fs.repositories.default_branch_id` to reference a branch **in the same
 repository**. we achieve this by:
 - giving `fs.branches` a composite uniqueness target `(id, repository_id)`, and
 - referencing it from repositories using `(default_branch_id, id)`.
 */
alter table fs.repositories
add constraint repositories_default_branch_same_repo_fk foreign key (default_branch_id, id) references fs.branches (id, repository_id) on delete
set null;
-- ========================================
-- INDEXES
-- ========================================
-- Accelerates ancestor walking and merge-base queries
create index idx_commits_repository_parent on fs.commits (repository_id, parent_commit_id);
-- Accelerates merge-base lookups across linear merge commits
create index idx_commits_repository_merged_from on fs.commits (repository_id, merged_from_commit_id);
-- Only the first/root commit in a repository may have a NULL parent
create unique index commits_one_root_per_repo_idx on fs.commits (repository_id)
where parent_commit_id is null;
-- Accelerates file lookups by path within a commit's ancestry
create index idx_files_commit_path on fs.files (commit_id, path);