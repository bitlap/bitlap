declare namespace API {
  interface Result_UserInfo_ {
    resultCode?: number;
    errorMessage?: string;
    data?: { list: [UserInfo] };
  }

  type UserGenderEnum = 'MALE' | 'FEMALE';

  interface UserInfo {
    id?: string;
    name?: string;
    /** nick */
    nickName?: string;
    /** email */
    email?: string;
    gender?: UserGenderEnum;
  }
}
