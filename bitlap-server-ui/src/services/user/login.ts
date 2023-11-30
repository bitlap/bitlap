// @ts-ignore
import { request } from '@umijs/max';

export async function accountLogin(
  params: { username: string; password: string },
  options?: { [key: string]: any },
) {
  return request<API.LoginResult>('/api/user/login', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    data: params,
    ...(options || {}),
  });
}
