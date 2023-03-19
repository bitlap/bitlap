// @ts-ignore
import request from '@/utils/request'

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
