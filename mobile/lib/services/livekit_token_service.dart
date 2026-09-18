import 'dart:convert';
import 'package:http/http.dart' as http;

import 'server_config.dart';
import 'call_session.dart';

class LiveKitTokenService {
  static Future<Map<String, dynamic>> getToken({
    required String callId,
  }) async {
    final token = CallSession.instance.accessToken;

    if (token == null || token.isEmpty) {
      throw Exception('missing access token');
    }

    final userId = CallSession.instance.userId;

    final response = await http.get(
      Uri.parse(
        '${ServerConfig.httpUrl}/livekit/token?user_id=$userId&call_id=$callId',
      ),
      headers: {
        'Authorization': 'Bearer $token',
      },
    );

    if (response.statusCode != 200) {
      throw Exception(
        'LiveKit token failed: ${response.body}',
      );
    }

    return jsonDecode(response.body);
  }
}
