import {
  createChatClientOptions,
  fetchServerSentEvents,
} from "@tanstack/ai-client";
import { useChat } from "@tanstack/ai-react";
import { Bash } from "just-bash";
import { useEffect, useState } from "react";
import { useHref, useLoaderData, type LoaderFunctionArgs } from "react-router";
import * as z from "zod";
import {
  createCommit,
  finalizeCommit,
  getBranchById,
  getCommitSnapshotWithContent,
  writeFile,
} from "~/db/plugins/fs";
import { getSql } from "~/db/sql.server";
import { createActions, useActions, validatedAction } from "~/util/actions";
import { toolDefs } from "./branches.$branchId.chat";

export async function loader(ctx: LoaderFunctionArgs) {
  const { branchId } = ctx.params;
  if (!branchId) throw new Error("branchId is required");

  const branch = await getBranchById(branchId);
  if (!branch) throw new Error("Branch not found");

  const files = branch.headCommitId
    ? await getCommitSnapshotWithContent(branch.headCommitId)
    : [];

  return {
    branch,
    files: Object.fromEntries(files.map((file) => [file.path, file.content])),
  };
}

export const action = createActions({
  commit: validatedAction(
    z.object({
      message: z.string(),
      files: z.record(
        z.string(),
        z.object({
          previousPath: z.string().optional().nullable(),
          content: z.string(),
          isSymlink: z.boolean().optional().default(false),
          isDeleted: z.boolean().optional().default(false),
        })
      ),
    }),
    async (ctx, data) => {
      const branch = await getBranchById(ctx.params.branchId!);
      if (!branch) throw new Error("Branch not found");

      const sql = getSql();

      await sql.tx(async () => {
        const commit = await createCommit(
          branch.repositoryId,
          data.message,
          branch.headCommitId
        );

        for (const [path, file] of Object.entries(data.files)) {
          await writeFile(commit.id, path, file.content, {
            isSymlink: file.isSymlink,
            isDeleted: file.isDeleted,
            previousPath: file.previousPath ?? undefined,
          });
        }

        await finalizeCommit(commit.id, branch.id);
      });

      return { success: true };
    }
  ),
});

export default function Component() {
  const { branch, files } = useLoaderData<typeof loader>();
  const [bash, setBash] = useState<Bash | null>(null);

  useEffect(() => {
    const bash = new Bash({
      files: Object.fromEntries(
        Object.entries(files).map(([path, content]) => [
          `/repository${path}`,
          content,
        ])
      ),
      cwd: "/repository",
    });
    setBash(bash);
  }, [files]);

  if (!bash) return null;

  return <Inner bash={bash} />;
}

function Inner({ bash }: { bash: Bash }) {
  const { branch, files } = useLoaderData<typeof loader>();
  const useAction = useActions();
  const commit = useAction("commit");

  const tools = [
    toolDefs.readFile.client(async (input: any) => {
      const { path } = input as { path: string };
      return await bash.readFile(path);
    }),
    toolDefs.writeFile.client(async (input: any) => {
      const { path, content } = input as { path: string; content: string };
      await bash.writeFile(path, content);
      return { success: true };
    }),
    toolDefs.exec.client(async (input: any) => {
      const { command } = input as { command: string };
      const { stdout, stderr, exitCode } = await bash.exec(command);
      return { stdout, stderr, exitCode };
    }),
    toolDefs.refresh.client(async (input: any) => {
      const { message } = input as { message: string };
      const fsFiles = Object.fromEntries(
        Array.from(bash.fs.data.entries())
          .filter(([key, value]) => key.startsWith("/repository/"))
          .map(([key, value]) => [
            key.replace("/repository", ""),
            new TextDecoder().decode(value.content),
          ])
      );

      const updatedFiles: Record<
        string,
        {
          content: string;
          isSymlink: boolean;
          isDeleted: boolean;
          previousPath: string | null;
        }
      > = {};

      for (let [path, content] of Object.entries(fsFiles)) {
        if (files[path] === content) continue;

        updatedFiles[path] = {
          content,
          isSymlink: false,
          isDeleted: false,
          previousPath: null,
        };
      }

      for (let path of Object.keys(files)) {
        if (fsFiles[path]) continue;

        updatedFiles[path] = {
          content: "",
          isSymlink: false,
          isDeleted: true,
          previousPath: null,
        };
      }
      console.log("updatedFiles", updatedFiles);

      if (Object.keys(updatedFiles).length === 0) return;

      console.log("committing");
      commit({
        message,
        files: updatedFiles,
      });
    }),
  ];

  const { messages, sendMessage, isLoading, error, addToolApprovalResponse } =
    useChat(
      createChatClientOptions({
        connection: fetchServerSentEvents(useHref("./chat")),
        tools,
      })
    );

  console.log(messages);

  return (
    <div className="flex h-screen">
      <div className="w-[380px] shrink-0 border-r border-gray-200 relative">
        <div className="absolute inset-0 overflow-y-auto">
          <div>
            {messages.map((message) => (
              <div key={message.id}>
                <strong>{message.role}:</strong>
                {message.parts.map((part, idx) => {
                  if (part.type === "thinking") {
                    return (
                      <div key={idx} className="text-sm text-gray-500 italic">
                        💭 Thinking: {part.content}
                      </div>
                    );
                  }
                  if (part.type === "text") {
                    return <span key={idx}>{part.content}</span>;
                  }
                  if (part.type === "tool-call") {
                    return <span key={idx}>{part.name}</span>;
                  }
                  if (part.type === "tool-result") {
                    return <span key={idx}>{part.name}</span>;
                  }
                  console.warn(part);
                  return null;
                })}
              </div>
            ))}
          </div>
          <form
            onSubmit={(e: React.FormEvent) => {
              e.preventDefault();
              const form = e.target as HTMLFormElement;
              const input = form.input.value;
              if (input.trim() && !isLoading) {
                sendMessage(input);
                requestAnimationFrame(() => {
                  form.reset();
                });
              }
            }}
            className="flex flex-col gap-2 px-5"
          >
            <textarea
              name="input"
              disabled={isLoading}
              className="border border-gray-100 rounded-sm"
            />
            <div className="flex justify-end">
              <button
                type="submit"
                disabled={isLoading}
                className="rounded-md px-5 py-2 text-white bg-blue-500"
              >
                Send
              </button>
            </div>
          </form>
        </div>
      </div>
      <div className="flex-1 relative">
        <iframe
          src={`/files/${branch.headCommitId}/index.html`}
          className="w-full h-full"
        />
      </div>
    </div>
  );
}
