export async function onRequest(context) {
  const { request, env } = context;
  const backendBase = String(env.BACKEND_BASE_URL || env.RENDER_BACKEND_URL || "").trim().replace(/\/+$/, "");
  if (!backendBase) {
    return new Response("BACKEND_BASE_URL is not configured", { status: 500 });
  }

  const sourceUrl = new URL(request.url);
  const targetUrl = new URL(`${backendBase}${sourceUrl.pathname}${sourceUrl.search}`);
  const headers = new Headers(request.headers);
  headers.delete("host");
  headers.delete("content-length");
  headers.set("x-forwarded-host", sourceUrl.host);
  headers.set("x-forwarded-proto", sourceUrl.protocol.replace(":", ""));

  const init = {
    method: request.method,
    headers,
    redirect: "manual",
  };

  if (!["GET", "HEAD"].includes(request.method)) {
    init.body = await request.arrayBuffer();
  }

  const response = await fetch(targetUrl.toString(), init);
  const responseHeaders = new Headers(response.headers);
  responseHeaders.delete("content-encoding");
  responseHeaders.delete("content-length");

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers: responseHeaders,
  });
}
