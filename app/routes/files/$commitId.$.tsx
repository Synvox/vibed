import type { LoaderFunctionArgs } from "react-router";
import { readFile } from "~/db/plugins/fs";
import bun from "bun";

export async function loader({ params }: LoaderFunctionArgs) {
  const { commitId, "*": path } = params;

  if (!commitId || !path) throw new Error("commitId and path are required");

  const content = await readFile(commitId, path);

  if (!content) throw new Error("File not found");

  const type = bun.file(path).type;

  let injected = false;

  const transformedContent =
    type === "text/html"
      ? new HTMLRewriter()
          .on("head", {
            element(head) {
              head.onEndTag((endHead) => {
                if (!injected) {
                  endHead.before(SCRIPT_TO_INJECT, { html: true });
                  injected = true;
                }
              });
            },
          })
          .on("body", {
            element(body) {
              body.onEndTag((endBody) => {
                if (!injected) {
                  endBody.before(SCRIPT_TO_INJECT, { html: true });
                  injected = true;
                }
              });
            },
          })
          .onDocument({
            end(end) {
              if (!injected) {
                end.append(SCRIPT_TO_INJECT, { html: true });
                injected = true;
              }
            },
          })

          .transform(content)
      : content;

  return new Response(content, {
    headers: {
      "Content-Length": content.length.toString(),
      "Cache-Control": "public, max-age=31536000, immutable",
      "Content-Type": type,
    },
  });
}

const SCRIPT_TO_INJECT = `<script src="https://cdn.jsdelivr.net/npm/@tailwindcss/browser@4"></script>`;
