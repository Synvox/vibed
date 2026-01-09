import {
  chat,
  toolDefinition,
  toServerSentEventsResponse,
  type StreamChunk,
} from "@tanstack/ai";
import { grokText } from "@tanstack/ai-grok";
import type { ActionFunctionArgs } from "react-router";
import * as z from "zod";
import { getBranchById, getCommitSnapshot } from "~/db/plugins/fs";

const readFile = toolDefinition({
  name: "read_file",
  description: "Read the content of a file",
  inputSchema: z.object({
    path: z.string(),
  }),
  outputSchema: z.object({
    content: z.string(),
  }),
});

const writeFile = toolDefinition({
  name: "write_file",
  description: "Write to a file (new or existing)",
  inputSchema: z.object({
    path: z.string(),
    content: z.string(),
  }),
  outputSchema: z.object({
    success: z.boolean(),
  }),
});

const exec = toolDefinition({
  name: "execute",
  description: "Execute Bash Command",
  inputSchema: z.object({
    command: z.string(),
  }),
  outputSchema: z.object({
    stdout: z.string(),
    stderr: z.string(),
    exitCode: z.number(),
  }),
});

export const toolDefs = {
  readFile,
  writeFile,
  exec,
};

export async function action(ctx: ActionFunctionArgs) {
  const { branchId } = ctx.params;
  if (!branchId) throw new Error("branchId is required");

  const branch = await getBranchById(branchId);
  if (!branch) throw new Error("Branch not found");

  const files = branch.headCommitId
    ? await getCommitSnapshot(branch.headCommitId)
    : [];

  const { messages } = await ctx.request.json();

  const adapter = grokText("grok-4-fast-non-reasoning");

  const result: AsyncIterable<StreamChunk> = await chat({
    adapter,
    messages,
    tools: Object.values(toolDefs),
    systemPrompts: [
      dedent`
        You are Quartermaster, an expert software developer for Sails.app. Use the read_file tool to read files,
        the write_file tool to write files, and the exec tool to execute bash commands.

        For styling, please use tailwind. Don't worry about setting up tailwind.
      `,
      dedent`
        Here are the files in the /repository directory:
${files.map((file) => `        - /repository/${file.path}`).join("\n")}

        Assume you're in the /repository directory.
      `,
    ],
  });

  return toServerSentEventsResponse(result);
}

const dedent = (strings: TemplateStringsArray, ...values: any[]) =>
  String.raw({ raw: strings }, ...values)
    .split("\n")
    .map((l, i) => (i === 0 ? l.trimStart() : l))
    .join("\n")
    .replace(/^[^\S\n]+/gm, "")
    .trim();
