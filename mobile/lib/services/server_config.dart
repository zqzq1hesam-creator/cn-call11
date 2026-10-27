class ServerConfig {
  // Android must talk to the live Cloudflare Worker API. Railway's old
  // deployment has unresolved postgres.railway.internal errors.
  static const String host =
      'cn-call11.zqzq1hesam-cncall.workers.dev';

  static String get httpUrl {
    return 'https://$host';
  }

  static String websocketUrl(String userId) {
    return 'wss://$host/ws/$userId';
  }
}
