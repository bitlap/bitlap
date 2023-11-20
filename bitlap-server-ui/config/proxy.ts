/**
 * @name 代理的配置
 * @see 在生产环境 代理是无法生效的，所以这里没有生产环境的配置
 * -------------------------------
 * The agent cannot take effect in the production environment
 * so there is no configuration of the production environment
 * For details, please see
 * https://pro.ant.design/docs/deploy
 *
 * @doc https://umijs.org/docs/guides/proxy
 */
export default {
  /**
   * @name 详细的代理配置
   * @doc https://github.com/chimurai/http-proxy-middleware
   */
  local: {
    '/api/v1': {
      // 配置了这个可以从 http 代理到 https
      // 依赖 origin 的功能可能需要这个，比如 cookie
      target: 'http://localhost:8080',
      changeOrigin: true,
      // pathRewrite: {'^/prefix/v1' : ''}
    },
  },

  dev: {
    '/api/v1': {
      target: 'http://dev:8080',
      changeOrigin: true,
      // pathRewrite: {'^/prefix/v1' : ''}
    },
  },

  test: {
    '/api/v1': {
      target: 'http://test:8080',
      changeOrigin: true,
      // pathRewrite: {'^/prefix/v1' : ''}
    },
  },

  stg: {
    '/api/v1': {
      target: 'http://stg:8080',
      changeOrigin: true,
      // pathRewrite: {'^/prefix/v1' : ''}
    },
  },

  prod: {
    '/api/v1': {
      target: 'http://prod:8080',
      changeOrigin: true,
      // pathRewrite: {'^/prefix/v1' : ''}
    },
  },
};
