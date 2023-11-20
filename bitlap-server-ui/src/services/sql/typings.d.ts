declare namespace API {
  type RunSqlResult  = {
    resultCode?: number;
    errorMessage?: string;
    data?: { list: [any] };
  }
}
