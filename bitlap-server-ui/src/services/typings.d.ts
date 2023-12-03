// @ts-ignore
/* eslint-disable */

declare namespace API {
  type RunSqlResult = {
    code?: number;
    error?: string;
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
    code?: number;
    error?: string;
    data?: CurrentUser;
  };

  type LoginResult = {
    code?: number;
    error?: string;
    data?: CurrentUser;
  };

  type LogoutResult = {
    code?: number;
    error?: string;
  };

  type PageParams = {
    current?: number;
    pageSize?: number;
  };
}
