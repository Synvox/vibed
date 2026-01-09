import { AsyncLocalStorage } from "node:async_hooks";
import { db, type DbInterface } from "./db.server";

const placeholder = Symbol();

export type Interpolable =
  | Statement<boolean>
  | number
  | string
  | boolean
  | null;
type Options<Camelize extends boolean> = { camelize: Camelize };

export class Statement<Camelize extends boolean> {
  readonly strings: (string | typeof placeholder)[] = [];
  readonly values: Interpolable[] = [];

  private db: DbInterface;
  private camelize: Camelize;

  constructor(
    db: DbInterface,
    { camelize }: Options<Camelize>,
    strings: ReadonlyArray<string>,
    values: Interpolable[]
  ) {
    this.db = db;
    this.camelize = camelize;

    if (strings.length - 1 !== values.length)
      throw new Error(
        `Invalid number of values: strings: ${JSON.stringify(strings)},  values: ${JSON.stringify(values)}`
      );

    let givenStrings: (string | typeof placeholder)[] = [...strings];
    let givenValues: Interpolable[] = [...values];

    while (true) {
      if (givenStrings.length === 0 && givenValues.length === 0) break;
      if (givenStrings.length > 0) this.strings.push(givenStrings.shift()!);
      if (givenValues.length > 0) {
        const value = givenValues.shift()!;

        if (value instanceof Statement) {
          this.strings.push(...value.strings);
          this.values.push(...value.values);
        } else {
          this.strings.push(placeholder);
          this.values.push(value);
        }
      }
    }
  }

  private transformCases<T>(res: T) {
    return (this.camelize ? toCamelCase(res) : res) as Camelize extends true
      ? DeepCamelKeys<typeof res>
      : typeof res;
  }

  parameterize(index: number) {
    return `$${index}`;
  }

  compile() {
    let result = "";
    let index = 1;

    for (let i = 0; i < this.strings.length; i++) {
      if (this.strings[i] === placeholder) {
        result += this.parameterize(index++);
      } else {
        result += this.strings[i] as string;
      }
    }

    return result;
  }

  async exec() {
    if (this.values.length === 0) await this.db.exec(this.compile());
    else await this.db.query(this.compile(), this.values as any);
  }

  async all<T extends object>() {
    const { rows } = await this.db.query<T>(this.compile(), this.values);
    return this.transformCases(rows);
  }

  async first<T extends object>() {
    const { rows } = await this.db.query<T>(this.compile(), this.values);
    return this.transformCases(rows[0]);
  }
}

function makeSql<Camelize extends boolean>(
  db: DbInterface,
  options: Options<Camelize>
) {
  const sql = Object.assign(
    function sql(query: TemplateStringsArray, ...args: Interpolable[]) {
      return new Statement(db, options, query, args);
    },
    {
      tx: async function tx<T>(
        fn: (sql: Sql<Camelize>) => Promise<T>
      ): Promise<T> {
        if (!("transaction" in db)) {
          return await fn(sql);
        } else {
          return await db.transaction(async (tx) => {
            return await provideDb(tx, async () => {
              const sql = getSql();
              return await fn(makeSql<Camelize>(tx, options));
            });
          });
        }
      },

      withOptions(options: Options<Camelize>) {
        return makeSql<Camelize>(db, options);
      },

      ref(value: string): Statement<Camelize> {
        return new Statement(
          db,
          options,
          [`"${value.replace(/"/g, '""')}"`],
          []
        );
      },

      literal(value: any): Statement<Camelize> {
        return new Statement(db, options, ["", ""], [value]);
      },

      join(
        statements: Statement<Camelize>[],
        separator = sql`,`
      ): Statement<Camelize> {
        const nonEmptyStatements = statements.filter((stmt) => {
          return (
            stmt.strings.some(
              (s) => typeof s === "string" && s.trim().length > 0
            ) || stmt.values.length > 0
          );
        });

        const returned = nonEmptyStatements.reduce(
          (returned, curr, index) => {
            if (index === 0) {
              returned = sql`${curr}`;
            } else {
              returned = sql`${returned}${separator}${curr}`;
            }
            return returned;
          },
          sql``
        );

        return returned;
      },
    }
  );

  return sql;
}

export const sql = makeSql(db, { camelize: true });

export type Sql<Camelize extends boolean = true> = ReturnType<
  typeof makeSql<Camelize>
>;

const asyncLocalStorage = new AsyncLocalStorage<DbInterface>();

export function getSql<Camelize extends boolean = true>(
  options: Options<Camelize> = { camelize: true as Camelize }
): Sql<Camelize> {
  return makeSql(asyncLocalStorage.getStore() || db, options);
}

export async function provideDb<T>(db: DbInterface, fn: () => Promise<T>) {
  return asyncLocalStorage.run(db, () => {
    return fn();
  });
}

type Simplify<T> = {
  [KeyType in keyof T]: T[KeyType];
} & {};

type SnakeToCamelCase<S extends string> = S extends `${infer P1}_${infer P2}`
  ? `${Lowercase<P1>}${Capitalize<SnakeToCamelCase<P2>>}`
  : S;

type DeepCamelKeys<T> = T extends readonly any[]
  ? { [I in keyof T]: DeepCamelKeys<T[I]> }
  : T extends object
    ? {
        [K in keyof T as K extends string
          ? SnakeToCamelCase<K>
          : K]: DeepCamelKeys<T[K]>;
      }
    : T;

function toCamelCase<T>(obj: T): Simplify<DeepCamelKeys<T>> {
  if (Array.isArray(obj)) {
    return obj.map((item) => toCamelCase(item)) as DeepCamelKeys<T>;
  } else if (obj !== null && typeof obj === "object") {
    const newObj: any = {};
    for (const key in obj) {
      const camelKey = key.replace(/_(.)/g, (_, letter) =>
        letter.toUpperCase()
      );
      newObj[camelKey] = toCamelCase((obj as any)[key]);
    }
    return newObj as DeepCamelKeys<T>;
  }
  return obj as DeepCamelKeys<T>;
}
