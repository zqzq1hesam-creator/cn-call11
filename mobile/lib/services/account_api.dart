import 'dart:convert';

import 'package:http/http.dart' as http;

import 'server_config.dart';
import 'call_session.dart';

class AccountApi {
  static Uri _uri(String path) {
    return Uri.parse('${ServerConfig.httpUrl}$path');
  }

  static Map<String, dynamic> _decodeResponse(
    http.Response response, {
    required String action,
  }) {
    try {
      final data = jsonDecode(response.body);
      if (data is Map) {
        return Map<String, dynamic>.from(data);
      }
      return {
        'success': false,
        'message': 'استجابة غير صالحة من السيرفر (HTTP ${response.statusCode})',
      };
    } on FormatException {
      final compact = response.body.replaceAll(RegExp(r'\\s+'), ' ').trim();
      final preview = compact.length > 120
          ? '${compact.substring(0, 120)}…'
          : compact;
      return {
        'success': false,
        'message': 'رد غير JSON من السيرفر عند $action (HTTP ${response.statusCode}): $preview',
      };
    }
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
      ).timeout(const Duration(seconds: 15));

      return _decodeResponse(response, action: 'تسجيل الدخول');
    } catch (e) {
      return {
        'success': false,
        'message': 'تعذر الاتصال بالسيرفر [${e.runtimeType}]: $e',
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
      ).timeout(const Duration(seconds: 15));

      return _decodeResponse(response, action: 'إنشاء الحساب');
    } catch (e) {
      return {
        'success': false,
        'message': 'تعذر الاتصال بالسيرفر [${e.runtimeType}]: $e',
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
