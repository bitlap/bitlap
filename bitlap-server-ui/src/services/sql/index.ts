// @ts-ignore
import { request } from '@umijs/max';

export async function runSql(
  params: { sql?: string },
  options?: { [key: string]: any },
) {
  return request<API.RunSqlResult>('/api/sql/run', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    data: params,
    ...(options || {}),
  });
}
