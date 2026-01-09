export function notFound(message: string) {
  return new Response(message, { status: 404 });
}

export function badRequest(message: string) {
  return new Response(message, { status: 400 });
}
