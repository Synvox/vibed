import { useCallback, useMemo, type ComponentProps } from "react";
import {
  useFetcher,
  useHref,
  useLocation,
  type ActionFunctionArgs,
} from "react-router";
import * as z from "zod";

type ValidatedActionError<Input> = {
  error: z.core.$ZodErrorTree<Input>;
};

type ValidatedActionResult<Input, Output> =
  | { data: Output }
  | ValidatedActionError<Input>;

export function validatedAction<Input, Output>(
  schema: z.ZodType<Input>,
  fn: (ctx: ActionFunctionArgs, data: Input) => Output
) {
  return Object.assign(
    async (
      ctx: ActionFunctionArgs
    ): Promise<ValidatedActionResult<Input, Output>> => {
      const contentType = ctx.request.headers.get("Content-Type") || "";

      let values: any;

      if (contentType === "application/json") {
        const json = ctx.request.json();
        ctx.request.json = () => json;
        values = await ctx.request.json();
      } else {
        const formData = ctx.request.formData();
        ctx.request.formData = () => formData;
        values = Object.fromEntries((await ctx.request.formData()).entries());
      }

      const result = await schema.safeParseAsync(values);

      if (result.error) {
        // @TODO set status
        return { error: z.treeifyError(result.error) };
      }

      const data = await fn(ctx, result.data);
      return { data };
    },
    {
      schema,
    }
  );
}

export function createActions<
  FnArg extends ActionFunctionArgs,
  A extends Record<string, (ctx: FnArg) => Promise<any>> & {
    default?: (ctx: FnArg) => Promise<any>;
  },
>(functions: A) {
  const handler = async function (ctx: FnArg): Promise<ReturnType<A[keyof A]>> {
    const url = new URL(ctx.request.url);
    const maybeKey = url.searchParams.get("intent");

    const key: keyof A =
      maybeKey && maybeKey in functions ? maybeKey : "default";

    const chosenAction = functions[key];
    if (!chosenAction) throw new Response("Not Found", { status: 404 });

    return await chosenAction(ctx);
  };

  return Object.assign(handler, { actions: functions });
}

export function useActions<T extends { actions: Record<string, any> }>(
  to = ""
) {
  const location = useLocation();
  const href = useHref(to);

  return function <K extends keyof T["actions"] & string>(actionName: K) {
    type ActionFn = T["actions"][K];
    type SchemaType = ActionFn extends { schema: z.ZodType<infer S> }
      ? S
      : never;
    type Returned = ActionFn extends (ctx: any) => Promise<infer R> ? R : never;
    type ResultType =
      Returned extends ValidatedActionResult<any, infer D>
        ? Awaited<D>
        : Returned;
    type ErrorType = ValidatedActionError<SchemaType>["error"];

    const fetcher = useFetcher<ActionFn>();

    const url = useCallback(() => {
      const searchParams = new URLSearchParams();
      searchParams.set("intent", actionName as string);
      return `${href}?${searchParams.toString()}`;
    }, [href, location.search, actionName]);

    const Form = useCallback(
      (
        props: Omit<ComponentProps<typeof fetcher.Form>, "action" | "method">
      ) => <fetcher.Form {...props} action={url()} method="POST" />,
      [fetcher.Form, url]
    );

    const error: ErrorType | undefined = fetcher.data?.error;
    const data: ResultType | undefined = fetcher?.data?.data
      ? // unwrap data if it is a ValidatedActionResult
        fetcher?.data?.data
      : fetcher?.data;

    return useMemo(() => {
      return Object.assign(
        (data: SchemaType) => {
          const searchParams = new URLSearchParams();
          searchParams.set("intent", actionName as string);
          fetcher.submit(
            // assume formData is jsonable
            data as any,
            {
              method: "POST",
              action: url(),
              encType: "application/json",
            }
          );
        },
        {
          data,
          error,
          fetcher,
          Form,
          isLoading: fetcher.state !== "idle",
          url,
        }
      );
    }, [fetcher, error, Form, url, actionName]);
  };
}
