import { useLoaderData } from "react-router";
import { getSql } from "~/db/sql.server";

export async function loader() {
  const sql = getSql();

  return {
    result: await sql`select 1 as id_thing`.all<{ id_thing: number }>(),
  };
}

export default function Home() {
  console.log(useLoaderData());
  return (
    <div className="flex h-screen">
      <div className="w-[380px] shrink-0 border-r border-gray-200">
        <h1>Left Column</h1>
      </div>
      <div className="flex-1">
        <h1>Right Column</h1>
      </div>
    </div>
  );
}
