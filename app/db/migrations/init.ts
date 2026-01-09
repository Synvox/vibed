import { fsPlugin } from "../plugins/fs";

export async function up() {
  await fsPlugin();
}
