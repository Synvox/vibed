import * as fsp from "node:fs/promises";
import { getSql } from "../../sql.server";

// ========================================
// Types
// ========================================

/**
 * A repository in the virtual filesystem
 */
export interface Repository {
  id: string;
  name: string;
  defaultBranchId: string | null;
  createdAt: Date;
}

/**
 * A branch pointer in a repository
 */
export interface Branch {
  id: string;
  repositoryId: string;
  name: string;
  headCommitId: string | null;
  createdAt: Date;
}

/**
 * A commit in the repository
 */
export interface Commit {
  id: string;
  repositoryId: string;
  parentCommitId: string | null;
  mergedFromCommitId: string | null;
  message: string;
  createdAt: Date;
}

/**
 * A file entry in a commit
 */
export interface FileEntry {
  id: string;
  commitId: string;
  path: string;
  previousPath: string | null;
  content: string;
  isDeleted: boolean;
  isSymlink: boolean;
  createdAt: Date;
}

/**
 * File metadata in a commit delta
 */
export interface CommitDeltaEntry {
  repositoryId: string;
  repositoryName: string;
  commitId: string;
  path: string;
  previousPath: string | null;
  isDeleted: boolean;
  isSymlink: boolean;
  fileCreatedAt: Date;
  commitCreatedAt: Date;
  commitMessage: string;
}

/**
 * File metadata in a commit snapshot
 */
export interface SnapshotEntry {
  repositoryId: string;
  repositoryName: string;
  commitId: string;
  path: string;
  isSymlink: boolean;
  commitCreatedAt: Date;
  commitMessage: string;
}

/**
 * Snapshot entry with content (internal use)
 */
interface SnapshotEntryWithContent extends SnapshotEntry {
  content: string;
}

/**
 * File history entry
 */
export interface FileHistoryEntry {
  commitId: string;
  content: string | null;
  isDeleted: boolean;
  isSymlink: boolean;
}

/**
 * Conflict information for a file
 */
export interface ConflictEntry {
  mergeBaseCommitId: string;
  path: string;
  baseExists: boolean;
  baseIsSymlink: boolean;
  baseContent: string | null;
  leftExists: boolean;
  leftIsSymlink: boolean;
  leftContent: string | null;
  rightExists: boolean;
  rightIsSymlink: boolean;
  rightContent: string | null;
  conflictKind: "delete/modify" | "add/add" | "modify/modify";
}

/**
 * Result from a rebase operation
 */
export interface RebaseResult {
  operation: "noop" | "already_up_to_date" | "fast_forward" | "rebased";
  repositoryId: string;
  branchId: string;
  ontoBranchId: string;
  mergeBaseCommitId: string | null;
  previousBranchHeadCommitId: string | null;
  ontoHeadCommitId: string | null;
  rebasedCommitId: string | null;
  newBranchHeadCommitId: string | null;
  appliedFileCount: number;
}

/**
 * Result from a finalize commit operation
 */
export interface FinalizeResult {
  operation:
    | "fast_forward"
    | "merged"
    | "merged_with_conflicts_resolved"
    | "already_up_to_date";
  repositoryId: string;
  targetBranchId: string | null;
  mergeBaseCommitId: string | null;
  previousTargetHeadCommitId: string | null;
  sourceCommitId: string | null;
  mergeCommitId: string;
  newTargetHeadCommitId: string | null;
  appliedFileCount: number;
}

// ========================================
// Path Validation & Normalization
// ========================================

/**
 * Validates a path for cross-platform compatibility.
 * Throws if the path is invalid.
 */
export function validatePath(path: string): void {
  if (path == null || path.trim().length === 0) {
    throw new Error("Path cannot be null or empty");
  }

  if (path.length > 4096) {
    throw new Error("Path is too long (maximum 4096 characters)");
  }

  // Check for control characters (invalid on Windows, problematic on Unix)
  // Allow tab (\x09), newline (\x0A), carriage return (\x0D)
  for (let i = 0; i < path.length; i++) {
    const charCode = path.charCodeAt(i);
    if (charCode < 32 && charCode !== 9 && charCode !== 10 && charCode !== 13) {
      throw new Error(
        `Path contains control characters (0x${charCode.toString(16).padStart(2, "0").toUpperCase()})`
      );
    }
  }

  // Check for characters invalid on Windows: < > : " | ? *
  if (/[<>"|?*:]/.test(path)) {
    throw new Error(
      'Path contains characters invalid on Windows: < > : " | ? *'
    );
  }
}

/**
 * Normalizes a path to canonical form:
 * - Validates the path
 * - Converts backslashes to forward slashes
 * - Ensures path starts with /
 * - Collapses duplicate slashes
 * - Removes trailing slash (except for root /)
 */
export function normalizePath(path: string): string {
  validatePath(path);

  // Normalize path separators (accept Windows-style "\" input)
  let normalized = path.replace(/\\/g, "/");

  // Ensure path starts with /
  if (!normalized.startsWith("/")) {
    normalized = "/" + normalized;
  }

  // Remove duplicate slashes
  while (normalized.includes("//")) {
    normalized = normalized.replace(/\/\//g, "/");
  }

  // Remove trailing slash unless it's just "/"
  if (normalized.length > 1 && normalized.endsWith("/")) {
    normalized = normalized.slice(0, -1);
  }

  // Ensure the normalized output stays within max length
  if (normalized.length > 4096) {
    throw new Error("Path is too long (maximum 4096 characters)");
  }

  return normalized;
}

/**
 * Normalizes and validates a file path (not a directory).
 * - All normalizePath rules apply
 * - Additionally rejects root "/" as it's not a valid file path
 */
export function normalizeFilePath(path: string): string {
  const normalized = normalizePath(path);

  // Root "/" is not a valid file path
  if (normalized === "/") {
    throw new Error("Root '/' is not a valid file path");
  }

  return normalized;
}

/**
 * Normalizes a path prefix for prefix searches.
 * Preserves trailing slash if present in input.
 */
export function normalizePathPrefix(pathPrefix: string): string {
  validatePath(pathPrefix);

  const hasTrailingSlash =
    pathPrefix.endsWith("/") || pathPrefix.endsWith("\\");

  let normalized = pathPrefix.replace(/\\/g, "/");

  // Ensure prefix starts with /
  if (!normalized.startsWith("/")) {
    normalized = "/" + normalized;
  }

  // Remove duplicate slashes
  while (normalized.includes("//")) {
    normalized = normalized.replace(/\/\//g, "/");
  }

  // Preserve explicit trailing slash (directory-style prefix matching)
  if (hasTrailingSlash && normalized !== "/" && !normalized.endsWith("/")) {
    normalized = normalized + "/";
  }

  if (normalized.length > 4096) {
    throw new Error("Path is too long (maximum 4096 characters)");
  }

  return normalized;
}

// ========================================
// Plugin Installation
// ========================================

/**
 * Installs the fs (filesystem) schema and tables.
 * Call this once before using any fs functions.
 */
export async function fsPlugin() {
  const sql = getSql();

  const sqlScript = await fsp.readFile(
    new URL("./fs.sql", import.meta.url),
    "utf-8"
  );

  const strings = Object.assign([sqlScript] as ReadonlyArray<string>, {
    raw: [sqlScript],
  });

  await sql(strings).exec();
}

// ========================================
// Repository Operations
// ========================================

/**
 * Creates a new repository with a default 'main' branch.
 *
 * @param name - Unique name for the repository
 * @returns The created repository
 */
export async function createRepository(name: string): Promise<Repository> {
  const sql = getSql();

  // Create the repository
  const repoRow = await sql`
    insert into
      fs.repositories (name)
    values
      (${name})
    returning
      id,
      name,
      default_branch_id,
      created_at
  `.first<{
    id: string;
    name: string;
    default_branch_id: string | null;
    created_at: string;
  }>();

  const repositoryId = repoRow.id;

  // Create the default 'main' branch with null head
  const branchRow = await sql`
    insert into
      fs.branches (repository_id, name, head_commit_id)
    values
      (
        ${repositoryId}::UUID,
        'main',
        null
      )
    returning
      id
  `.first<{ id: string }>();

  // Set the default branch on the repository
  await sql`
    update fs.repositories
    set
      default_branch_id = ${branchRow.id}::UUID
    where
      id = ${repositoryId}::UUID
  `.exec();

  return {
    id: repositoryId,
    name: repoRow.name,
    defaultBranchId: branchRow.id,
    createdAt: new Date(repoRow.createdAt),
  };
}

/**
 * Gets a repository by name.
 *
 * @param name - Repository name
 * @returns The repository or null if not found
 */
export async function getRepository(name: string): Promise<Repository | null> {
  const sql = getSql();
  const row = await sql`
    select
      id,
      name,
      default_branch_id,
      created_at
    from
      fs.repositories
    where
      name = ${name}
  `.first<{
    id: string;
    name: string;
    default_branch_id: string | null;
    created_at: string;
  }>();

  if (!row) return null;

  return {
    id: row.id,
    name: row.name,
    defaultBranchId: row.defaultBranchId,
    createdAt: new Date(row.createdAt),
  };
}

/**
 * Gets a repository by ID.
 *
 * @param id - Repository ID (UUID)
 * @returns The repository or null if not found
 */
export async function getRepositoryById(
  id: string
): Promise<Repository | null> {
  const sql = getSql();
  const row = await sql`
    select
      id,
      name,
      default_branch_id,
      created_at
    from
      fs.repositories
    where
      id = ${id}::UUID
  `.first<{
    id: string;
    name: string;
    default_branch_id: string | null;
    created_at: string;
  }>();

  if (!row) return null;

  return {
    id: row.id,
    name: row.name,
    defaultBranchId: row.defaultBranchId,
    createdAt: new Date(row.createdAt),
  };
}

/**
 * Lists all repositories.
 *
 * @returns Array of repositories
 */
export async function listRepositories(): Promise<Repository[]> {
  const sql = getSql();
  const rows = await sql`
    select
      id,
      name,
      default_branch_id,
      created_at
    from
      fs.repositories
    order by
      name
  `.all<{
    id: string;
    name: string;
    defaultBranchId: string | null;
    createdAt: string;
  }>();

  return rows.map((row) => ({
    id: row.id,
    name: row.name,
    defaultBranchId: row.defaultBranchId,
    createdAt: new Date(row.createdAt),
  }));
}

// ========================================
// Branch Operations
// ========================================

/**
 * Creates a new branch in a repository.
 *
 * @param repositoryId - Repository ID
 * @param name - Branch name
 * @param headCommitId - Optional starting commit (defaults to default branch head)
 * @returns The created branch
 */
export async function createBranch(
  repositoryId: string,
  name: string,
  headCommitId?: string | null
): Promise<Branch> {
  const sql = getSql();

  let resolvedHeadCommitId = headCommitId ?? null;

  // If no head commit specified, try to default to the repository's default branch head
  if (resolvedHeadCommitId === null) {
    const defaultHeadRow = await sql`
      select
        b.head_commit_id
      from
        fs.repositories r
        join fs.branches b on b.id = r.default_branch_id
      where
        r.id = ${repositoryId}::UUID
    `.first<{ headCommitId: string | null }>();

    if (defaultHeadRow?.headCommitId) {
      resolvedHeadCommitId = defaultHeadRow.headCommitId;
    } else {
      // Check if commits exist - if so, require explicit head_commit_id
      const commitExists = await sql`
        select
          1
        from
          fs.commits
        where
          repository_id = ${repositoryId}::UUID
        limit
          1
      `.first<{ "?column?": number }>();

      if (commitExists) {
        throw new Error(
          "head_commit_id must be specified when creating a branch"
        );
      }
      // Otherwise allow null head_commit_id (empty branch before first commit)
    }
  }

  const row = await sql`
    insert into
      fs.branches (repository_id, name, head_commit_id)
    values
      (
        ${repositoryId}::UUID,
        ${name},
        ${resolvedHeadCommitId}::UUID
      )
    returning
      id,
      repository_id,
      name,
      head_commit_id,
      created_at
  `.first<{
    id: string;
    repositoryId: string;
    name: string;
    headCommitId: string | null;
    createdAt: string;
  }>();

  return {
    id: row.id,
    repositoryId: row.repositoryId,
    name: row.name,
    headCommitId: row.headCommitId,
    createdAt: new Date(row.createdAt),
  };
}

/**
 * Gets a branch by repository ID and name.
 *
 * @param repositoryId - Repository ID
 * @param name - Branch name
 * @returns The branch or null if not found
 */
export async function getBranch(
  repositoryId: string,
  name: string
): Promise<Branch | null> {
  const sql = getSql();
  const row = await sql`
    select
      id,
      repository_id,
      name,
      head_commit_id,
      created_at
    from
      fs.branches
    where
      repository_id = ${repositoryId}::UUID
      and name = ${name}
  `.first<{
    id: string;
    repositoryId: string;
    name: string;
    headCommitId: string | null;
    createdAt: string;
  }>();

  if (!row) return null;

  return {
    id: row.id,
    repositoryId: row.repositoryId,
    name: row.name,
    headCommitId: row.headCommitId,
    createdAt: new Date(row.createdAt),
  };
}

/**
 * Gets a branch by ID.
 *
 * @param id - Branch ID (UUID)
 * @returns The branch or null if not found
 */
export async function getBranchById(id: string): Promise<Branch | null> {
  const sql = getSql();
  const row = await sql`
    select
      id,
      repository_id,
      name,
      head_commit_id,
      created_at
    from
      fs.branches
    where
      id = ${id}::UUID
  `.first<{
    id: string;
    repositoryId: string;
    name: string;
    headCommitId: string | null;
    createdAt: string;
  }>();

  if (!row) return null;

  return {
    id: row.id,
    repositoryId: row.repositoryId,
    name: row.name,
    headCommitId: row.headCommitId,
    createdAt: new Date(row.createdAt),
  };
}

/**
 * Lists all branches in a repository.
 *
 * @param repositoryId - Repository ID
 * @returns Array of branches
 */
export async function listBranches(repositoryId: string): Promise<Branch[]> {
  const sql = getSql();
  const rows = await sql`
    select
      id,
      repository_id,
      name,
      head_commit_id,
      created_at
    from
      fs.branches
    where
      repository_id = ${repositoryId}::UUID
    order by
      name
  `.all<{
    id: string;
    repositoryId: string;
    name: string;
    headCommitId: string | null;
    createdAt: string;
  }>();

  return rows.map((row) => ({
    id: row.id,
    repositoryId: row.repositoryId,
    name: row.name,
    headCommitId: row.headCommitId,
    createdAt: new Date(row.createdAt),
  }));
}

/**
 * Updates a branch's head commit.
 *
 * @param branchId - Branch ID
 * @param headCommitId - New head commit ID
 */
export async function updateBranchHead(
  branchId: string,
  headCommitId: string
): Promise<void> {
  const sql = getSql();
  await sql`
    update fs.branches
    set
      head_commit_id = ${headCommitId}::UUID
    where
      id = ${branchId}::UUID
  `.exec();
}

// ========================================
// Commit Operations
// ========================================

/**
 * Creates a new commit in a repository.
 *
 * @param repositoryId - Repository ID
 * @param message - Commit message
 * @param parentCommitId - Parent commit ID (null for root commit, or auto-resolved)
 * @param mergedFromCommitId - Optional merged-from commit for merge commits
 * @returns The created commit
 */
export async function createCommit(
  repositoryId: string,
  message: string,
  parentCommitId?: string | null,
  mergedFromCommitId?: string | null
): Promise<Commit> {
  const sql = getSql();

  let resolvedParentCommitId = parentCommitId ?? null;

  // If no parent specified, try to default to the repository's default branch head
  if (resolvedParentCommitId === null && parentCommitId === undefined) {
    const defaultHeadRow = await sql`
      select
        b.head_commit_id
      from
        fs.repositories r
        join fs.branches b on b.id = r.default_branch_id
      where
        r.id = ${repositoryId}::UUID
    `.first<{ headCommitId: string | null }>();

    if (defaultHeadRow?.headCommitId) {
      resolvedParentCommitId = defaultHeadRow.headCommitId;
    } else {
      // Check if commits already exist - if so, require explicit parent
      const commitExists = await sql`
        select
          1
        from
          fs.commits
        where
          repository_id = ${repositoryId}::UUID
        limit
          1
      `.first<{ "?column?": number }>();

      if (commitExists) {
        throw new Error(
          "parent_commit_id must be specified (repository default branch head could not be resolved)"
        );
      }
      // Otherwise allow null parent (root commit)
    }
  }

  // Validate parent belongs to same repository
  if (resolvedParentCommitId !== null) {
    const parentRow = await sql`
      select
        1
      from
        fs.commits
      where
        id = ${resolvedParentCommitId}::UUID
        and repository_id = ${repositoryId}::UUID
    `.first<{ "?column?": number }>();

    if (!parentRow) {
      throw new Error(
        "Invalid parent_commit_id: must reference a commit in the same repository"
      );
    }
  }

  const row = await sql`
    insert into
      fs.commits (
        repository_id,
        message,
        parent_commit_id,
        merged_from_commit_id
      )
    values
      (
        ${repositoryId}::UUID,
        ${message},
        ${resolvedParentCommitId}::UUID,
        ${mergedFromCommitId ?? null}::UUID
      )
    returning
      id,
      repository_id,
      parent_commit_id,
      merged_from_commit_id,
      message,
      created_at
  `.first<{
    id: string;
    repositoryId: string;
    parentCommitId: string | null;
    mergedFromCommitId: string | null;
    message: string;
    createdAt: string;
  }>();

  return {
    id: row.id,
    repositoryId: row.repositoryId,
    parentCommitId: row.parentCommitId,
    mergedFromCommitId: row.mergedFromCommitId,
    message: row.message,
    createdAt: new Date(row.createdAt),
  };
}

/**
 * Gets a commit by ID.
 *
 * @param id - Commit ID (UUID)
 * @returns The commit or null if not found
 */
export async function getCommit(id: string): Promise<Commit | null> {
  const sql = getSql();
  const row = await sql`
    select
      id,
      repository_id,
      parent_commit_id,
      merged_from_commit_id,
      message,
      created_at
    from
      fs.commits
    where
      id = ${id}::UUID
  `.first<{
    id: string;
    repositoryId: string;
    parentCommitId: string | null;
    mergedFromCommitId: string | null;
    message: string;
    createdAt: string;
  }>();

  if (!row) return null;

  return {
    id: row.id,
    repositoryId: row.repositoryId,
    parentCommitId: row.parentCommitId,
    mergedFromCommitId: row.mergedFromCommitId,
    message: row.message,
    createdAt: new Date(row.createdAt),
  };
}

// ========================================
// File Operations
// ========================================

/**
 * Writes a file to a commit.
 *
 * @param commitId - Commit ID
 * @param path - File path (will be normalized)
 * @param content - File content
 * @param options - Optional flags for symlink/deleted
 * @returns The created file entry
 */
export async function writeFile(
  commitId: string,
  path: string,
  content: string,
  options: {
    isSymlink?: boolean;
    isDeleted?: boolean;
    previousPath?: string;
  } = {}
): Promise<FileEntry> {
  const sql = getSql();

  // Normalize the path (validates it's a valid file path, not just "/")
  const normalizedPath = normalizeFilePath(path);

  // Normalize previous_path if provided
  const normalizedPreviousPath = options.previousPath
    ? normalizeFilePath(options.previousPath)
    : null;

  // Handle file state invariants
  let isDeleted = options.isDeleted ?? false;
  let isSymlink = options.isSymlink ?? false;
  let normalizedContent = content;

  if (isDeleted) {
    // Tombstones force is_symlink = false and content = ''
    isSymlink = false;
    normalizedContent = "";
  } else {
    if (content == null) {
      throw new Error(
        "Content must be specified when inserting a non-deleted file"
      );
    }
    if (isSymlink) {
      // Normalize symlink target as a file path
      normalizedContent = normalizeFilePath(content);
    }
  }

  const row = await sql`
    insert into
      fs.files (
        commit_id,
        path,
        previous_path,
        content,
        is_symlink,
        is_deleted
      )
    values
      (
        ${commitId}::UUID,
        ${normalizedPath},
        ${normalizedPreviousPath},
        ${normalizedContent},
        ${isSymlink},
        ${isDeleted}
      )
    returning
      id,
      commit_id,
      path,
      previous_path,
      content,
      is_deleted,
      is_symlink,
      created_at
  `.first<{
    id: string;
    commitId: string;
    path: string;
    previousPath: string | null;
    content: string;
    isDeleted: boolean;
    isSymlink: boolean;
    createdAt: string;
  }>();

  return {
    id: row.id,
    commitId: row.commitId,
    path: row.path,
    previousPath: row.previousPath,
    content: row.content,
    isDeleted: row.isDeleted,
    isSymlink: row.isSymlink,
    createdAt: new Date(row.createdAt),
  };
}

/**
 * Renames/moves a file by writing it to a new path with previous_path set.
 *
 * @param commitId - Commit ID
 * @param fromPath - Current file path
 * @param toPath - New file path
 * @param content - File content (required since we're creating a new entry)
 * @param options - Optional flags for symlink
 * @returns The created file entry with previousPath set
 */
export async function moveFile(
  commitId: string,
  fromPath: string,
  toPath: string,
  content: string,
  options: { isSymlink?: boolean } = {}
): Promise<FileEntry> {
  return writeFile(commitId, toPath, content, {
    ...options,
    previousPath: fromPath,
  });
}

/**
 * Writes multiple files to a commit.
 *
 * @param commitId - Commit ID
 * @param files - Array of files to write
 * @returns Array of created file entries
 */
export async function writeFiles(
  commitId: string,
  files: Array<{
    path: string;
    content: string;
    isSymlink?: boolean;
    isDeleted?: boolean;
    previousPath?: string;
  }>
): Promise<FileEntry[]> {
  const results: FileEntry[] = [];
  for (const file of files) {
    const entry = await writeFile(commitId, file.path, file.content, {
      isSymlink: file.isSymlink,
      isDeleted: file.isDeleted,
      previousPath: file.previousPath,
    });
    results.push(entry);
  }
  return results;
}

/**
 * Deletes a file by writing a tombstone.
 *
 * @param commitId - Commit ID
 * @param path - File path to delete
 * @returns The tombstone file entry
 */
export async function deleteFile(
  commitId: string,
  path: string
): Promise<FileEntry> {
  return writeFile(commitId, path, "", { isDeleted: true });
}

/**
 * Reads a file at a specific commit by resolving through ancestry.
 *
 * @param commitId - Commit ID
 * @param path - File path
 * @returns File content or null if not found/deleted
 */
export async function readFile(
  commitId: string,
  path: string
): Promise<string | null> {
  const sql = getSql();

  if (!commitId) {
    throw new Error("commit_id must be specified");
  }

  // Verify commit exists
  const commitExists = await sql`
    select
      1
    from
      fs.commits
    where
      id = ${commitId}::UUID
  `.first<{ "?column?": number }>();

  if (!commitExists) {
    throw new Error("Invalid commit_id: commit does not exist");
  }

  const normalizedPath = normalizePath(path);

  // Walk up the commit tree to find the file
  // A file is considered deleted if:
  // 1. It has is_deleted = true, OR
  // 2. Another file has previous_path = this path (file was moved away)
  const row = await sql`
    with recursive
      commit_tree as (
        select
          id,
          parent_commit_id,
          0 as depth
        from
          fs.commits
        where
          id = ${commitId}::UUID
        union all
        select
          c.id,
          c.parent_commit_id,
          ct.depth + 1
        from
          fs.commits c
          inner join commit_tree ct on c.id = ct.parent_commit_id
      ),
      -- Find the first file entry at this path OR a move-away from this path
      file_entries as (
        select
          ct.depth,
          f.content,
          f.is_deleted,
          f.path,
          f.previous_path
        from
          commit_tree ct
          join fs.files f on ct.id = f.commit_id
        where
          f.path = ${normalizedPath}
          or f.previous_path = ${normalizedPath}
      )
    select
      content,
      is_deleted,
      path,
      previous_path
    from
      file_entries
    order by
      depth asc
    limit
      1
  `.first<{
    content: string;
    isDeleted: boolean;
    path: string;
    previousPath: string | null;
  }>();

  if (!row) {
    return null;
  }

  // File is deleted explicitly
  if (row.isDeleted) {
    return null;
  }

  // File was moved away from this path (another file has previousPath = this path)
  // In this case, the row we got is for the file at the new location
  if (row.previousPath === normalizedPath && row.path !== normalizedPath) {
    return null;
  }

  return row.content;
}

/**
 * Gets the files written in a specific commit (delta).
 *
 * @param commitId - Commit ID
 * @returns Array of files written in this commit
 */
export async function getCommitDelta(
  commitId: string
): Promise<CommitDeltaEntry[]> {
  const sql = getSql();
  const rows = await sql`
    select
      r.id as repository_id,
      r.name as repository_name,
      c.id as commit_id,
      f.path,
      f.previous_path,
      f.is_deleted,
      f.is_symlink,
      f.created_at as file_created_at,
      c.created_at as commit_created_at,
      c.message as commit_message
    from
      fs.commits c
      join fs.repositories r on c.repository_id = r.id
      join fs.files f on c.id = f.commit_id
    where
      c.id = ${commitId}::UUID
  `.all<{
    repositoryId: string;
    repositoryName: string;
    commitId: string;
    path: string;
    previousPath: string | null;
    isDeleted: boolean;
    isSymlink: boolean;
    fileCreatedAt: string;
    commitCreatedAt: string;
    commitMessage: string;
  }>();

  return rows.map((row) => ({
    repositoryId: row.repositoryId,
    repositoryName: row.repositoryName,
    commitId: row.commitId,
    path: row.path,
    previousPath: row.previousPath,
    isDeleted: row.isDeleted,
    isSymlink: row.isSymlink,
    fileCreatedAt: new Date(row.fileCreatedAt),
    commitCreatedAt: new Date(row.commitCreatedAt),
    commitMessage: row.commitMessage,
  }));
}

/**
 * Gets the resolved file tree at a commit (without content).
 *
 * @param commitId - Commit ID
 * @param pathPrefix - Optional prefix to filter paths
 * @returns Array of files in the snapshot
 */
export async function getCommitSnapshot(
  commitId: string,
  pathPrefix?: string
): Promise<SnapshotEntry[]> {
  const sql = getSql();

  if (!commitId) {
    throw new Error("commit_id must be specified");
  }

  // Verify commit exists
  const commitRow = await sql`
    select
      c.id,
      c.created_at,
      c.message,
      r.id as repository_id,
      r.name as repository_name
    from
      fs.commits c
      join fs.repositories r on c.repository_id = r.id
    where
      c.id = ${commitId}::UUID
  `.first<{
    id: string;
    createdAt: string;
    message: string;
    repositoryId: string;
    repositoryName: string;
  }>();

  if (!commitRow) {
    throw new Error("Invalid commit_id: commit does not exist");
  }

  const normalizedPrefix = pathPrefix ? normalizePathPrefix(pathPrefix) : null;

  // Get resolved snapshot
  // Handles both explicit deletes (is_deleted) and implicit deletes via moves (previous_path)
  const rows = await sql`
    with recursive
      commit_tree as (
        select
          id,
          parent_commit_id,
          0 as depth
        from
          fs.commits
        where
          id = ${commitId}::UUID
        union all
        select
          c.id,
          c.parent_commit_id,
          ct.depth + 1
        from
          fs.commits c
          inner join commit_tree ct on c.id = ct.parent_commit_id
      ),
      -- All file operations: writes to a path OR moves away from a path
      all_operations as (
        select
          f.path as affected_path,
          f.is_deleted,
          f.is_symlink,
          false as is_move_away,
          ct.depth
        from
          commit_tree ct
          join fs.files f on ct.id = f.commit_id
        union all
        -- A file being moved creates an implicit delete at the old path
        select
          f.previous_path as affected_path,
          true as is_deleted,
          false as is_symlink,
          true as is_move_away,
          ct.depth
        from
          commit_tree ct
          join fs.files f on ct.id = f.commit_id
        where
          f.previous_path is not null
      ),
      -- Get the most recent operation for each path
      ranked_operations as (
        select
          affected_path,
          is_deleted,
          is_symlink,
          row_number() over (
            partition by
              affected_path
            order by
              depth asc
          ) as rn
        from
          all_operations
        where
          affected_path is not null
          and (
            ${normalizedPrefix}::text is null
            or affected_path like ${
              normalizedPrefix ? normalizedPrefix + "%" : null
            }
          )
      )
    select
      affected_path as path,
      is_symlink
    from
      ranked_operations
    where
      rn = 1
      and not is_deleted
    order by
      path
  `.all<{ path: string; isSymlink: boolean }>();

  return rows.map((row) => ({
    repositoryId: commitRow.repositoryId,
    repositoryName: commitRow.repositoryName,
    commitId: commitRow.id,
    path: row.path,
    isSymlink: row.isSymlink,
    commitCreatedAt: new Date(commitRow.createdAt),
    commitMessage: commitRow.message,
  }));
}

/**
 * Internal: Gets the resolved file tree at a commit with content.
 */
export async function getCommitSnapshotWithContent(
  commitId: string,
  pathPrefix?: string
): Promise<SnapshotEntryWithContent[]> {
  const snapshot = await getCommitSnapshot(commitId, pathPrefix);
  const results: SnapshotEntryWithContent[] = [];

  for (const entry of snapshot) {
    // @TODO don't do this.
    const content = await readFile(commitId, entry.path);
    results.push({
      ...entry,
      content: content ?? "",
    });
  }

  return results;
}

/**
 * Gets the history of changes to a file.
 *
 * @param commitId - Commit ID to start from
 * @param path - File path
 * @returns Array of historical file states
 */
export async function getFileHistory(
  commitId: string,
  path: string
): Promise<FileHistoryEntry[]> {
  const sql = getSql();

  if (!commitId) {
    throw new Error("commit_id must be specified");
  }

  // Verify commit exists
  const commitExists = await sql`
    select
      1
    from
      fs.commits
    where
      id = ${commitId}::UUID
  `.first<{ "?column?": number }>();

  if (!commitExists) {
    throw new Error("Invalid commit_id: commit does not exist");
  }

  const normalizedPath = normalizePath(path);

  const rows = await sql`
    with recursive
      commit_tree as (
        select
          id,
          parent_commit_id,
          created_at
        from
          fs.commits
        where
          id = ${commitId}::UUID
        union all
        select
          c.id,
          c.parent_commit_id,
          c.created_at
        from
          fs.commits c
          inner join commit_tree ct on c.id = ct.parent_commit_id
      )
    select
      ct.id as commit_id,
      case
        when f.is_deleted then null
        else f.content
      end as content,
      f.is_deleted,
      f.is_symlink
    from
      commit_tree ct
      join fs.files f on ct.id = f.commit_id
      and f.path = ${normalizedPath}
  `.all<{
    commitId: string;
    content: string | null;
    isDeleted: boolean;
    isSymlink: boolean;
  }>();

  return rows.map((row) => ({
    commitId: row.commitId,
    content: row.content,
    isDeleted: row.isDeleted,
    isSymlink: row.isSymlink,
  }));
}

// ========================================
// Merge & Rebase Operations
// ========================================

/**
 * Finds the merge base (common ancestor) of two commits.
 *
 * @param leftCommitId - First commit ID
 * @param rightCommitId - Second commit ID
 * @returns The merge base commit ID
 */
export async function getMergeBase(
  leftCommitId: string,
  rightCommitId: string
): Promise<string> {
  const sql = getSql();

  if (!leftCommitId || !rightCommitId) {
    throw new Error("commit_id must be specified");
  }

  // Verify commits exist and get their repository IDs
  const leftRow = await sql`
    select
      repository_id
    from
      fs.commits
    where
      id = ${leftCommitId}::UUID
  `.first<{ repositoryId: string }>();

  if (!leftRow) {
    throw new Error("Invalid commit_id (left): commit does not exist");
  }

  const rightRow = await sql`
    select
      repository_id
    from
      fs.commits
    where
      id = ${rightCommitId}::UUID
  `.first<{ repositoryId: string }>();

  if (!rightRow) {
    throw new Error("Invalid commit_id (right): commit does not exist");
  }

  if (leftRow.repositoryId !== rightRow.repositoryId) {
    throw new Error("Commits must belong to the same repository");
  }

  // Find common ancestor with minimal combined depth
  const baseRow = await sql`
    with recursive
      left_ancestors as (
        select
          id,
          parent_commit_id,
          merged_from_commit_id,
          0 as depth
        from
          fs.commits
        where
          id = ${leftCommitId}::UUID
        union all
        select
          c.id,
          c.parent_commit_id,
          c.merged_from_commit_id,
          la.depth + 1
        from
          fs.commits c
          join left_ancestors la on c.id = la.parent_commit_id
          or c.id = la.merged_from_commit_id
      ),
      right_ancestors as (
        select
          id,
          parent_commit_id,
          merged_from_commit_id,
          0 as depth
        from
          fs.commits
        where
          id = ${rightCommitId}::UUID
        union all
        select
          c.id,
          c.parent_commit_id,
          c.merged_from_commit_id,
          ra.depth + 1
        from
          fs.commits c
          join right_ancestors ra on c.id = ra.parent_commit_id
          or c.id = ra.merged_from_commit_id
      ),
      common as (
        select
          l.id,
          min(l.depth + r.depth) as total_depth
        from
          left_ancestors l
          join right_ancestors r using (id)
        group by
          l.id
      )
    select
      id
    from
      common
    order by
      total_depth asc
    limit
      1
  `.first<{ id: string }>();

  if (!baseRow) {
    throw new Error("No common ancestor found (unexpected)");
  }

  return baseRow.id;
}

/**
 * Detects file-level conflicts between two commits.
 *
 * @param leftCommitId - First commit ID
 * @param rightCommitId - Second commit ID
 * @returns Array of conflicts (empty if no conflicts)
 */
export async function getConflicts(
  leftCommitId: string,
  rightCommitId: string
): Promise<ConflictEntry[]> {
  const mergeBaseCommitId = await getMergeBase(leftCommitId, rightCommitId);

  // Get snapshots for all three commits
  const baseSnapshot = await getCommitSnapshotWithContent(mergeBaseCommitId);
  const leftSnapshot = await getCommitSnapshotWithContent(leftCommitId);
  const rightSnapshot = await getCommitSnapshotWithContent(rightCommitId);

  // Build maps for quick lookup
  const baseMap = new Map(baseSnapshot.map((e) => [e.path, e]));
  const leftMap = new Map(leftSnapshot.map((e) => [e.path, e]));
  const rightMap = new Map(rightSnapshot.map((e) => [e.path, e]));

  // Collect all paths
  const allPaths = new Set([
    ...baseSnapshot.map((e) => e.path),
    ...leftSnapshot.map((e) => e.path),
    ...rightSnapshot.map((e) => e.path),
  ]);

  const conflicts: ConflictEntry[] = [];

  for (const path of allPaths) {
    const base = baseMap.get(path);
    const left = leftMap.get(path);
    const right = rightMap.get(path);

    const baseExists = !!base;
    const leftExists = !!left;
    const rightExists = !!right;

    const baseIsSymlink = base?.isSymlink ?? false;
    const leftIsSymlink = left?.isSymlink ?? false;
    const rightIsSymlink = right?.isSymlink ?? false;

    const baseContent = base?.content ?? null;
    const leftContent = left?.content ?? null;
    const rightContent = right?.content ?? null;

    // Check if left changed from base
    const leftChanged =
      leftExists !== baseExists ||
      leftIsSymlink !== baseIsSymlink ||
      leftContent !== baseContent;

    // Check if right changed from base
    const rightChanged =
      rightExists !== baseExists ||
      rightIsSymlink !== baseIsSymlink ||
      rightContent !== baseContent;

    // Check if left and right differ
    const sidesDiffer =
      leftExists !== rightExists ||
      leftIsSymlink !== rightIsSymlink ||
      leftContent !== rightContent;

    // Conflict if both sides changed and they differ
    if (leftChanged && rightChanged && sidesDiffer) {
      let conflictKind: ConflictEntry["conflictKind"];
      if (baseExists && (!leftExists || !rightExists)) {
        conflictKind = "delete/modify";
      } else if (!baseExists && leftExists && rightExists) {
        conflictKind = "add/add";
      } else {
        conflictKind = "modify/modify";
      }

      conflicts.push({
        mergeBaseCommitId,
        path,
        baseExists,
        baseIsSymlink,
        baseContent,
        leftExists,
        leftIsSymlink,
        leftContent,
        rightExists,
        rightIsSymlink,
        rightContent,
        conflictKind,
      });
    }
  }

  return conflicts.sort((a, b) => a.path.localeCompare(b.path));
}

/**
 * Rebases a branch onto another branch.
 *
 * @param branchId - Branch to rebase
 * @param ontoBranchId - Target branch to rebase onto
 * @param message - Optional message for the rebased commit
 * @returns Result of the rebase operation
 */
export async function rebaseBranch(
  branchId: string,
  ontoBranchId: string,
  message?: string
): Promise<RebaseResult> {
  const sql = getSql();

  if (!branchId || !ontoBranchId) {
    throw new Error("branch_id must be specified");
  }

  // Get branch info
  const branchRow = await sql`
    select
      repository_id,
      head_commit_id
    from
      fs.branches
    where
      id = ${branchId}::UUID
  `.first<{ repositoryId: string; headCommitId: string | null }>();

  if (!branchRow) {
    throw new Error("Invalid branch_id: branch does not exist");
  }

  const ontoRow = await sql`
    select
      repository_id,
      head_commit_id
    from
      fs.branches
    where
      id = ${ontoBranchId}::UUID
  `.first<{ repositoryId: string; headCommitId: string | null }>();

  if (!ontoRow) {
    throw new Error("Invalid onto_branch_id: branch does not exist");
  }

  if (branchRow.repositoryId !== ontoRow.repositoryId) {
    throw new Error("Branches must belong to the same repository");
  }

  const repositoryId = branchRow.repositoryId;
  const branchHeadCommitId = branchRow.headCommitId;
  const ontoHeadCommitId = ontoRow.headCommitId;

  // Self-rebase is a noop
  if (branchId === ontoBranchId) {
    return {
      operation: "noop",
      repositoryId,
      branchId,
      ontoBranchId,
      mergeBaseCommitId: branchHeadCommitId,
      previousBranchHeadCommitId: branchHeadCommitId,
      ontoHeadCommitId,
      rebasedCommitId: null,
      newBranchHeadCommitId: branchHeadCommitId,
      appliedFileCount: 0,
    };
  }

  if (!branchHeadCommitId || !ontoHeadCommitId) {
    throw new Error("Both branches must have commits to rebase");
  }

  const mergeBaseCommitId = await getMergeBase(
    branchHeadCommitId,
    ontoHeadCommitId
  );

  // Already up to date: onto is already an ancestor of branch
  if (mergeBaseCommitId === ontoHeadCommitId) {
    return {
      operation: "already_up_to_date",
      repositoryId,
      branchId,
      ontoBranchId,
      mergeBaseCommitId,
      previousBranchHeadCommitId: branchHeadCommitId,
      ontoHeadCommitId,
      rebasedCommitId: null,
      newBranchHeadCommitId: branchHeadCommitId,
      appliedFileCount: 0,
    };
  }

  // Fast forward: branch is an ancestor of onto
  if (mergeBaseCommitId === branchHeadCommitId) {
    await updateBranchHead(branchId, ontoHeadCommitId);
    return {
      operation: "fast_forward",
      repositoryId,
      branchId,
      ontoBranchId,
      mergeBaseCommitId,
      previousBranchHeadCommitId: branchHeadCommitId,
      ontoHeadCommitId,
      rebasedCommitId: null,
      newBranchHeadCommitId: ontoHeadCommitId,
      appliedFileCount: 0,
    };
  }

  // Check for conflicts
  const conflicts = await getConflicts(branchHeadCommitId, ontoHeadCommitId);
  if (conflicts.length > 0) {
    throw new Error(
      `Rebase blocked by ${conflicts.length} conflicts. Use getConflicts(${branchHeadCommitId}, ${ontoHeadCommitId}) to inspect.`
    );
  }

  // Compute minimal patch to apply
  const baseSnapshot = await getCommitSnapshotWithContent(mergeBaseCommitId);
  const ontoSnapshot = await getCommitSnapshotWithContent(ontoHeadCommitId);
  const branchSnapshot = await getCommitSnapshotWithContent(branchHeadCommitId);

  const baseMap = new Map(baseSnapshot.map((e) => [e.path, e]));
  const ontoMap = new Map(ontoSnapshot.map((e) => [e.path, e]));
  const branchMap = new Map(branchSnapshot.map((e) => [e.path, e]));

  const allPaths = new Set([
    ...baseSnapshot.map((e) => e.path),
    ...ontoSnapshot.map((e) => e.path),
    ...branchSnapshot.map((e) => e.path),
  ]);

  // Compute what files need to be written
  const filesToWrite: Array<{
    path: string;
    content: string;
    isSymlink: boolean;
    isDeleted: boolean;
  }> = [];

  for (const path of allPaths) {
    const base = baseMap.get(path);
    const onto = ontoMap.get(path);
    const branch = branchMap.get(path);

    // Determine if branch changed from base
    const branchChanged =
      !!branch !== !!base ||
      branch?.isSymlink !== base?.isSymlink ||
      branch?.content !== base?.content;

    // Desired state: if branch changed, use branch; else use onto
    const desiredExists = branchChanged ? !!branch : !!onto;
    const desiredIsSymlink = branchChanged
      ? (branch?.isSymlink ?? false)
      : (onto?.isSymlink ?? false);
    const desiredContent = branchChanged
      ? (branch?.content ?? "")
      : (onto?.content ?? "");

    // Determine if we need to write (differs from onto)
    const ontoExists = !!onto;
    const ontoIsSymlink = onto?.isSymlink ?? false;
    const ontoContent = onto?.content ?? "";

    const needDelete = !desiredExists && ontoExists;
    const needWrite =
      desiredExists &&
      (!ontoExists ||
        ontoIsSymlink !== desiredIsSymlink ||
        ontoContent !== desiredContent);

    if (needDelete) {
      filesToWrite.push({
        path,
        content: "",
        isSymlink: false,
        isDeleted: true,
      });
    } else if (needWrite) {
      filesToWrite.push({
        path,
        content: desiredContent,
        isSymlink: desiredIsSymlink,
        isDeleted: false,
      });
    }
  }

  // If no changes needed, just fast-forward
  if (filesToWrite.length === 0) {
    await updateBranchHead(branchId, ontoHeadCommitId);
    return {
      operation: "fast_forward",
      repositoryId,
      branchId,
      ontoBranchId,
      mergeBaseCommitId,
      previousBranchHeadCommitId: branchHeadCommitId,
      ontoHeadCommitId,
      rebasedCommitId: null,
      newBranchHeadCommitId: ontoHeadCommitId,
      appliedFileCount: 0,
    };
  }

  // Create the rebased commit
  const rebasedCommit = await createCommit(
    repositoryId,
    message ?? "rebase",
    ontoHeadCommitId
  );

  // Write the files
  await writeFiles(rebasedCommit.id, filesToWrite);

  // Update branch head
  await updateBranchHead(branchId, rebasedCommit.id);

  return {
    operation: "rebased",
    repositoryId,
    branchId,
    ontoBranchId,
    mergeBaseCommitId,
    previousBranchHeadCommitId: branchHeadCommitId,
    ontoHeadCommitId,
    rebasedCommitId: rebasedCommit.id,
    newBranchHeadCommitId: rebasedCommit.id,
    appliedFileCount: filesToWrite.length,
  };
}

/**
 * Finalizes a merge commit and optionally advances a branch.
 *
 * @param commitId - The merge commit ID
 * @param targetBranchId - Optional branch to advance
 * @returns Result of the finalize operation
 */
export async function finalizeCommit(
  commitId: string,
  targetBranchId?: string
): Promise<FinalizeResult> {
  const sql = getSql();

  if (!commitId) {
    throw new Error("merge_commit_id must be specified");
  }

  // Get merge commit info
  const mergeCommitRow = await sql`
    select
      repository_id,
      parent_commit_id,
      merged_from_commit_id
    from
      fs.commits
    where
      id = ${commitId}::UUID
  `.first<{
    repositoryId: string;
    parentCommitId: string | null;
    mergedFromCommitId: string | null;
  }>();

  if (!mergeCommitRow) {
    throw new Error("Invalid merge_commit_id: commit does not exist");
  }

  const repositoryId = mergeCommitRow.repositoryId;
  const parentCommitId = mergeCommitRow.parentCommitId;
  const mergedFromCommitId = mergeCommitRow.mergedFromCommitId;

  let branchHeadCommitId: string | null = null;

  // Resolve branch context if provided
  if (targetBranchId) {
    const branchRow = await sql`
      select
        repository_id,
        head_commit_id
      from
        fs.branches
      where
        id = ${targetBranchId}::UUID
    `.first<{ repositoryId: string; headCommitId: string | null }>();

    if (!branchRow) {
      throw new Error("Invalid target_branch_id: branch does not exist");
    }

    if (branchRow.repositoryId !== repositoryId) {
      throw new Error(
        "Branch and merge commit must belong to the same repository"
      );
    }

    branchHeadCommitId = branchRow.headCommitId;

    // For root commits (null parent), branch head must also be null
    // For non-root commits, parent must match branch head
    if (parentCommitId === null) {
      if (branchHeadCommitId !== null) {
        throw new Error(
          "Root commit (null parent) can only be finalized on a branch with no head"
        );
      }
    } else if (parentCommitId !== branchHeadCommitId) {
      throw new Error(
        "Commit parent_commit_id must match the current branch head"
      );
    }
  } else {
    // No branch context; treat parent as the target snapshot
    branchHeadCommitId = parentCommitId;
  }

  // Fast-forward finalize when merged_from_commit_id is NULL
  if (mergedFromCommitId === null) {
    if (targetBranchId) {
      await updateBranchHead(targetBranchId, commitId);
    }

    return {
      operation: "fast_forward",
      repositoryId,
      targetBranchId: targetBranchId ?? null,
      mergeBaseCommitId: parentCommitId,
      previousTargetHeadCommitId: branchHeadCommitId,
      sourceCommitId: null,
      mergeCommitId: commitId,
      newTargetHeadCommitId: targetBranchId ? commitId : null,
      appliedFileCount: 0,
    };
  }

  // Full merge flow
  const mergeBaseCommitId = await getMergeBase(
    branchHeadCommitId!,
    mergedFromCommitId
  );

  // Get conflicts
  const conflicts = await getConflicts(branchHeadCommitId!, mergedFromCommitId);

  // Check that all conflicts have resolutions in the merge commit
  if (conflicts.length > 0) {
    const existingResolutions = await sql`
      select
        path
      from
        fs.files
      where
        commit_id = ${commitId}::UUID
    `.all<{ path: string }>();

    const resolutionPaths = new Set(existingResolutions.map((r) => r.path));

    const missingResolutions = conflicts.filter(
      (c) => !resolutionPaths.has(c.path)
    );
    if (missingResolutions.length > 0) {
      throw new Error(
        `Merge requires resolutions for ${missingResolutions.length} conflict paths; insert rows into fs.files for commit ${commitId}`
      );
    }
  }

  // Compute files to apply (source changes not already resolved)
  const baseSnapshot = await getCommitSnapshotWithContent(mergeBaseCommitId);
  const targetSnapshot = await getCommitSnapshotWithContent(
    branchHeadCommitId!
  );
  const sourceSnapshot = await getCommitSnapshotWithContent(mergedFromCommitId);

  const baseMap = new Map(baseSnapshot.map((e) => [e.path, e]));
  const targetMap = new Map(targetSnapshot.map((e) => [e.path, e]));
  const sourceMap = new Map(sourceSnapshot.map((e) => [e.path, e]));

  // Get user-provided resolutions
  const userWrites = await sql`
    select
      path
    from
      fs.files
    where
      commit_id = ${commitId}::UUID
  `.all<{ path: string }>();
  const userWritePaths = new Set(userWrites.map((w) => w.path));

  const allPaths = new Set([
    ...baseSnapshot.map((e) => e.path),
    ...targetSnapshot.map((e) => e.path),
    ...sourceSnapshot.map((e) => e.path),
    ...userWritePaths,
  ]);

  let appliedFileCount = 0;

  for (const path of allPaths) {
    // Skip paths already written by user
    if (userWritePaths.has(path)) continue;

    const base = baseMap.get(path);
    const target = targetMap.get(path);
    const source = sourceMap.get(path);

    // Determine if source changed from base
    const sourceChanged =
      !!source !== !!base ||
      source?.isSymlink !== base?.isSymlink ||
      source?.content !== base?.content;

    // Desired state: if source changed, use source; else use target
    const desiredExists = sourceChanged ? !!source : !!target;
    const desiredIsSymlink = sourceChanged
      ? (source?.isSymlink ?? false)
      : (target?.isSymlink ?? false);
    const desiredContent = sourceChanged
      ? (source?.content ?? "")
      : (target?.content ?? "");

    // Determine if we need to write (differs from target)
    const targetExists = !!target;
    const targetIsSymlink = target?.isSymlink ?? false;
    const targetContent = target?.content ?? "";

    const needDelete = !desiredExists && targetExists;
    const needWrite =
      desiredExists &&
      (!targetExists ||
        targetIsSymlink !== desiredIsSymlink ||
        targetContent !== desiredContent);

    if (needDelete) {
      await writeFile(commitId, path, "", { isDeleted: true });
      appliedFileCount++;
    } else if (needWrite) {
      await writeFile(commitId, path, desiredContent, {
        isSymlink: desiredIsSymlink,
      });
      appliedFileCount++;
    }
  }

  // Update branch head
  if (targetBranchId) {
    await updateBranchHead(targetBranchId, commitId);
  }

  let operation: FinalizeResult["operation"];
  if (mergeBaseCommitId === mergedFromCommitId) {
    operation = "already_up_to_date";
  } else if (conflicts.length > 0) {
    operation = "merged_with_conflicts_resolved";
  } else {
    operation = "merged";
  }

  return {
    operation,
    repositoryId,
    targetBranchId: targetBranchId ?? null,
    mergeBaseCommitId,
    previousTargetHeadCommitId: branchHeadCommitId,
    sourceCommitId: mergedFromCommitId,
    mergeCommitId: commitId,
    newTargetHeadCommitId: targetBranchId ? commitId : null,
    appliedFileCount,
  };
}

// ========================================
// Convenience Functions
// ========================================

/**
 * Creates a commit with files and advances the branch in one operation.
 * This is a convenience wrapper for the common workflow.
 *
 * @param branchId - Branch to commit to
 * @param message - Commit message
 * @param files - Files to write in this commit
 * @returns The created commit and updated branch
 */
export async function commitToBranch(
  branchId: string,
  message: string,
  files: Array<{
    path: string;
    content: string;
    isSymlink?: boolean;
    isDeleted?: boolean;
  }>
): Promise<{ commit: Commit; branch: Branch }> {
  // Get current branch state
  const branch = await getBranchById(branchId);
  if (!branch) {
    throw new Error(`Branch ${branchId} not found`);
  }

  // Create commit
  const commit = await createCommit(
    branch.repositoryId,
    message,
    branch.headCommitId
  );

  // Write files
  await writeFiles(commit.id, files);

  // Advance branch head
  await updateBranchHead(branchId, commit.id);

  // Return updated state
  const updatedBranch = await getBranchById(branchId);

  return { commit, branch: updatedBranch! };
}

/**
 * Initializes a repository with an initial commit containing files.
 *
 * @param repoName - Repository name
 * @param files - Initial files
 * @param commitMessage - Initial commit message
 * @returns The created repository, branch, and commit
 */
export async function initRepository(
  repoName: string,
  files: Array<{ path: string; content: string }>,
  commitMessage: string = "Initial commit"
): Promise<{ repository: Repository; branch: Branch; commit: Commit }> {
  // Create repository (also creates main branch)
  const repository = await createRepository(repoName);

  // Get the default branch
  const branch = await getBranchById(repository.defaultBranchId!);
  if (!branch) {
    throw new Error("Default branch not found after repository creation");
  }

  // Create initial commit (root commit with null parent)
  const commit = await createCommit(
    repository.id,
    commitMessage,
    null // root commit
  );

  // Write files
  await writeFiles(
    commit.id,
    files.map((f) => ({ ...f, isSymlink: false, isDeleted: false }))
  );

  // Advance branch head
  await updateBranchHead(branch.id, commit.id);

  // Get updated state
  const updatedRepo = await getRepositoryById(repository.id);
  const updatedBranch = await getBranchById(branch.id);

  return {
    repository: updatedRepo!,
    branch: updatedBranch!,
    commit,
  };
}
