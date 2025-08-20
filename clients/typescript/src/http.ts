export interface RequestOptions extends RequestInit {}

export class HttpClient {
  readonly baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  buildQuery(params: Record<string, string | number | undefined>): string {
    const usp = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) {
      if (v === undefined) continue;
      usp.set(k, String(v));
    }
    const query = usp.toString();
    return query.length ? `?${query}` : "";
  }

  async getJson<T>(path: string, init?: RequestOptions): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`, init);
    if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
    return (await res.json()) as T;
  }
}


