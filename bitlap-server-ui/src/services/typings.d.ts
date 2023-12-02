// @ts-ignore
/* eslint-disable */

declare namespace API {
  type RunSqlResult = {
    resultCode?: number;
    errorMessage?: string;
    data?: { list: [any] };
  };
  type CurrentUser = {
    // unique name
    username?: string;
    nickName?: string;
    avatar?: string;
    email?: string;
    gender?: string;
    signature?: string;
    title?: string;
    group?: string;
    tags?: { key?: string; label?: string }[];
    country?: string;
    access?: string;
    address?: string;
    phone?: string;
  };

  type GetUserInfoResult = {
    resultCode?: number;
    errorMessage?: string;
    data?: CurrentUser;
  };

  type LoginResult = {
    resultCode?: number;
    errorMessage?: string;
    data?: CurrentUser;
  };

  type LogoutResult = {
    resultCode?: number;
    errorMessage?: string;
  };

  type PageParams = {
    current?: number;
    pageSize?: number;
  };

  type ErrorResponse = {
    /** 业务约定的错误码 */
    errorCode: string;
    /** 业务上的错误信息 */
    errorMessage?: string;
    /** 业务上的请求是否成功 */
    success?: boolean;
  };
}
