import 'package:flutter_test/flutter_test.dart';
import 'package:mobile/services/call_session.dart';
import 'package:shared_preferences/shared_preferences.dart';

void main() {
  setUp(() {
    SharedPreferences.setMockInitialValues(<String, Object>{});
  });

  test('call session starts without an active call', () async {
    expect(await CallSession.instance.hasActiveCall(), isFalse);
  });
}
