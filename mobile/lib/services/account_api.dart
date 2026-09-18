import 'dart:convert';

import 'package:http/http.dart' as http;

import 'server_config.dart';
import 'call_session.dart';

class AccountApi {
  static Uri _uri(String path) {
    return Uri.parse('${ServerConfig.httpUrl}$path');
  }

  static Map<String, String> _headers() {
    final token = CallSession.instance.accessToken;
    return {
      'Content-Type': 'application/json',
      if (token != null && token.isNotEmpty) 'Authorization': 'Bearer $token',
    };
  }

  static Future<Map<String, dynamic>> login({
    required String userId,
    required String password,
  }) async {
    try {
      final response = await http.post(
        _uri('/login'),
        headers: {
          'Content-Type': 'application/json',
        },
        body: jsonEncode({
          'user_id': userId,
          'password': password,
        }),
      );

      final data = jsonDecode(response.body);

      if (data is Map) {
        return Map<String, dynamic>.from(data);
      }

      return {
        'success': false,
        'message': 'استجابة غير صالحة من السيرفر',
      };
    } catch (_) {
      return {
        'success': false,
        'message': 'تعذر الاتصال بالسيرفر',
      };
    }
  }

  static Future<Map<String, dynamic>> register({
    required String userId,
    required String username,
    required String password,
  }) async {
    try {
      final response = await http.post(
        _uri('/register'),
        headers: {
          'Content-Type': 'application/json',
        },
        body: jsonEncode({
          'user_id': userId,
          'username': username,
          'password': password,
        }),
      );

      final data = jsonDecode(response.body);

      if (data is Map) {
        return Map<String, dynamic>.from(data);
      }

      return {
        'success': false,
        'message': 'استجابة غير صالحة من السيرفر',
      };
    } catch (e) {
      return {
        'success': false,
        'message': 'تعذر الاتصال بالسيرفر',
      };
    }
  }

  static Future<List<Map<String, dynamic>>> missedCalls({
    required String userId,
  }) async {
    try {
      final response = await http.get(
        _uri('/calls/missed/$userId'),
        headers: _headers(),
      );
      final data = jsonDecode(response.body);
      final calls = data is Map ? data['calls'] : null;
      if (calls is List) {
        return calls
            .whereType<Map>()
            .map((call) => Map<String, dynamic>.from(call))
            .toList();
      }
    } catch (_) {}

    return <Map<String, dynamic>>[];
  }
}
