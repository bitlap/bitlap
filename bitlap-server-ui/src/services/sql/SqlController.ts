import { request } from '@umijs/max';

export async function runSql(sql: string) {
  return request<API.Result_UserInfo_>('/api/sql/run', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    data: {
      sql,
    },
  });
}
