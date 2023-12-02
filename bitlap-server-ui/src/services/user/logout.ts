// @ts-ignore
import { request } from '@umijs/max';

export async function accountLogout(
  username: string, // with password
  options?: { [key: string]: any },
) {
  return request<API.LogoutResult>('/api/user/logout', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    data: {
      username: username,
    },
    ...(options || {}),
  });
}
