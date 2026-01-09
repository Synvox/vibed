import { type RouteConfig, index, route } from "@react-router/dev/routes";

export default [
  index("routes/home.tsx"),
  route("branches/:branchId", "routes/branches.$branchId.tsx"),
  route("branches/:branchId/chat", "routes/branches.$branchId.chat.tsx"),
  route("files/:commitId/*", "routes/files/$commitId.$.tsx"),
] satisfies RouteConfig;
