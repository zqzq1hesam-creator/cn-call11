import 'dart:async';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import 'package:firebase_core/firebase_core.dart';
import 'package:firebase_messaging/firebase_messaging.dart';

import 'firebase_options.dart';

import 'services/call_session.dart';
import 'services/firebase_messaging_service.dart';
import 'services/account_api.dart';
import 'services/rtc_call_manager.dart';

import 'package:shared_preferences/shared_preferences.dart';

const MethodChannel _telecomChannel = MethodChannel('cn_call/call');
const MethodChannel _telecomEventsChannel =
    MethodChannel('cn_call/telecom_events');

enum CnSetupState { unconfigured, checking, ready }

const _cnSetupPrefKey = 'cn_call_setup_complete_v1';

void _installTelecomEventHandler() {
  _telecomEventsChannel.setMethodCallHandler((call) async {
    final arguments = Map<Object?, Object?>.from(call.arguments as Map);
    // Android uses this event only to wake/bring the Flutter activity forward.
    // Rendering and all call decisions remain in Flutter; it must never create
    // a Telecom/InCallUI call.
    if (call.method == 'muteChanged') {
      final callId = arguments['callId']?.toString() ?? '';
      if (callId.isNotEmpty && RtcCallManager.instance.currentCallId == callId) {
        await RtcCallManager.instance.mute(arguments['isMuted'] == true);
      }
      return;
    }
    if (call.method == 'active') {
      // Native Telecom call reached ACTIVE (media ready); keep the in-app
      // CallScreen bound to the same native call â€” no Flutter LiveKit of its
      // own for this callId.
      final callId = arguments['callId']?.toString() ?? '';
      if (callId.isNotEmpty) {
        await RtcCallManager.instance.onNativeCallActive();
      }
      return;
    }
    if (call.method == 'ended') {
      // Native Telecom call ended (setDisconnected already reached; the single
      // terminal frame was sent by the native engine). Flutter mirrors local
      // state only.
      final callId = arguments['callId']?.toString() ?? '';
      if (callId.isNotEmpty) {
        await RtcCallManager.instance.handleNativeCallEnded(callId);
      }
      return;
    }
  });
}

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();

  FirebaseMessaging.onBackgroundMessage(firebaseMessagingBackgroundHandler);

  await Firebase.initializeApp(options: DefaultFirebaseOptions.currentPlatform);

  _installTelecomEventHandler();

  RtcCallManager.instance.startListening();

  runApp(const CNCallApp());
}

class CNCallApp extends StatefulWidget {
  const CNCallApp({super.key});

  @override
  State<CNCallApp> createState() => _CNCallAppState();
}

class _CNCallAppState extends State<CNCallApp> {
  bool _loading = true;
  bool _hasSession = false;

  @override
  void initState() {
    super.initState();

    CallSession.instance.onSessionInvalidated = _handleSessionInvalidated;
    _restoreSession();
  }

  void _handleSessionInvalidated() {
    if (!mounted) return;
    setState(() {
      _hasSession = false;
    });
  }

  @override
  void dispose() {
    CallSession.instance.onSessionInvalidated = null;
    super.dispose();
  }

  Future<void> _restoreSession() async {
    // Fresh isolate: no live Flutter call can exist yet, so a lingering
    // `flutter` WS-owner marker or active-call marker from a previous process
    // (crash / OS kill mid-call) is stale. Clear it BEFORE restoreSession's
    // socket.connect() so the next native acquisition/call is never refused.
    await CallSession.instance.clearStaleStateForFreshStartup();

    final restored = await CallSession.instance.restoreSession();

    if (restored) {
      await FirebaseMessagingService.instance.refreshTokenForCurrentUser();
    }

    if (!mounted) return;

    setState(() {
      _hasSession = restored;
      _loading = false;
    });
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      title: 'CN CALL',
      theme: ThemeData(
        brightness: Brightness.dark,
        scaffoldBackgroundColor: const Color(0xFF050505),
        useMaterial3: true,
      ),
      home: _loading
          ? const Scaffold(
              body: Center(
                child: CircularProgressIndicator(color: Color(0xFF00E676)),
              ),
            )
          : _hasSession
          ? const HomeScreen()
          : const LoginScreen(),
    );
  }
}

// ============================================================
// LOGIN
// ============================================================

class LoginScreen extends StatefulWidget {
  const LoginScreen({super.key});

  @override
  State<LoginScreen> createState() => _LoginScreenState();
}

class _LoginScreenState extends State<LoginScreen>
    with WidgetsBindingObserver {
  final userIdController = TextEditingController();
  final passwordController = TextEditingController();

  bool hidePassword = true;

  CnSetupState _setupState = CnSetupState.checking;
  bool _phoneAccountEnabled = false;
  bool _hasPromptedPhoneAccount = false;
  bool _setupCheckInProgress = false;
  bool _startupPermissionFlowStarted = false;

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (!mounted || _startupPermissionFlowStarted) return;
      _startupPermissionFlowStarted = true;
      _runStartupPermissionFlow();
    });
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    userIdController.dispose();
    passwordController.dispose();
    super.dispose();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == AppLifecycleState.resumed) {
      _verifySetup();
    }
  }

  Future<void> _verifySetup() async {
    if (_setupCheckInProgress) return;
    _setupCheckInProgress = true;
    if (mounted) setState(() => _setupState = CnSetupState.checking);

    var hasPermission = true;
    try {
      hasPermission =
          await _telecomChannel.invokeMethod<bool>('hasStartupPermissions') ??
          false;
    } on PlatformException {
      hasPermission = false;
    }

    var accountEnabled = false;
    if (hasPermission) {
      try {
        await _telecomChannel.invokeMethod<bool>('registerCNCallPhoneAccount');
      } on PlatformException {
        // Registration is best-effort; enablement is what gates the login.
      }
      try {
        accountEnabled =
            await _telecomChannel.invokeMethod<bool>(
                  'isCNCallPhoneAccountEnabled',
                ) ??
                false;
      } on PlatformException {
        accountEnabled = false;
      }
    }

    _setupCheckInProgress = false;
    if (!mounted) return;

    if (hasPermission && accountEnabled) {
      final prefs = await SharedPreferences.getInstance();
      await prefs.setBool(_cnSetupPrefKey, true);
      if (!mounted) return;
      setState(() {
        _setupState = CnSetupState.ready;
        _phoneAccountEnabled = true;
      });
    } else {
      setState(() {
        _phoneAccountEnabled = accountEnabled;
        _setupState = CnSetupState.unconfigured;
      });
    }
  }

  Future<void> _runStartupPermissionFlow() async {
    try {
      await _telecomChannel.invokeMethod<bool>('requestStartupPermissions');
    } on PlatformException {
      // _verifySetup reports the incomplete permission state and setup can retry.
    }
    // Start Firebase initialization without blocking the Phone Account setup UI.
    // The initialization continues in parallel while Telecom setup is verified.
    final firebaseInitialization =
        FirebaseMessagingService.instance.initialize();

    if (mounted) {
      await _verifySetup();

      var hasPermissions = false;
      try {
        hasPermissions =
            await _telecomChannel.invokeMethod<bool>('hasStartupPermissions') ??
            false;
      } on PlatformException {
        hasPermissions = false;
      }

      if (hasPermissions && !_phoneAccountEnabled && !_hasPromptedPhoneAccount) {
        _hasPromptedPhoneAccount = true;
        _showPhoneAccountDialog();
      }
    }

    // Preserve startup initialization completion without delaying the popup.
    await firebaseInitialization;
  }
  void _showPhoneAccountDialog() {
    if (!mounted) return;
    showDialog<void>(
      context: context,
      barrierDismissible: false,
      builder: (dialogContext) {
        return AlertDialog(
          backgroundColor: const Color(0xFF1E1E1E),
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(20),
            side: BorderSide(color: Colors.grey.shade800),
          ),
          title: const Row(
            children: [
              Icon(
                Icons.phone_in_talk_outlined,
                color: Color(0xFF00E676),
                size: 24,
              ),
              SizedBox(width: 10),
              Expanded(
                child: Text(
                  'تفعيل حساب CN CALL للمكالمات',
                  style: TextStyle(
                    color: Colors.white,
                    fontSize: 17,
                    fontWeight: FontWeight.bold,
                  ),
                ),
              ),
            ],
          ),
          content: const Text(
            'يلزم تفعيل "حساب المكالمات" لـ CN CALL من إعدادات النظام لضمان استقبال وإجراء المكالمات عبر واجهة الهاتف الرسمية.',
            style: TextStyle(
              color: Colors.white70,
              fontSize: 14,
              height: 1.4,
            ),
          ),
          actions: [
            ElevatedButton(
              onPressed: () {
                Navigator.pop(dialogContext);
                _configurePhoneAccount();
              },
              style: ElevatedButton.styleFrom(
                backgroundColor: const Color(0xFF00A85A),
                foregroundColor: Colors.white,
                shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(12),
                ),
              ),
              child: const Text(
                'تفعيل الحساب',
                style: TextStyle(fontWeight: FontWeight.bold),
              ),
            ),
          ],
        );
      },
    );
  }

  Future<void> _configurePhoneAccount() async {
    try {
      await _telecomChannel.invokeMethod<bool>('registerCNCallPhoneAccount');
      final enabled =
          await _telecomChannel.invokeMethod<bool>(
                'isCNCallPhoneAccountEnabled',
              ) ??
              false;

      if (!mounted) return;
      setState(() {
        _phoneAccountEnabled = enabled;
      });

      if (!enabled) {
        await _telecomChannel.invokeMethod<bool>('openTelecomCallSettings');
      } else {
        _message('حساب CN CALL مفعّل', success: true);
      }
    } on PlatformException catch (error) {
      if (!mounted) return;
      _message('تعذر إعداد Phone Account: ${error.message ?? error.code}');
    }
  }

  void openRegister() {
    Navigator.push(
      context,
      MaterialPageRoute(builder: (_) => const RegisterScreen()),
    );
  }

  Future<void> login() async {
    if (_setupState != CnSetupState.ready) {
      _message('أكمل إعداد CN CALL أولًا ثم سجّل الدخول');
      return;
    }

    final userId = userIdController.text.trim();
    final password = passwordController.text;

    if (userId.isEmpty) {
      _message('أدخل ID المستخدم');
      return;
    }

    if (int.tryParse(userId) == null) {
      _message('ID المستخدم يجب أن يكون أرقامًا فقط');
      return;
    }

    if (password.isEmpty) {
      _message('أدخل كلمة المرور');
      return;
    }

    final result = await AccountApi.login(userId: userId, password: password);

    if (!mounted) return;

    final success = result['success'] == true;

    if (!success) {
      _message(
        result['message']?.toString() ?? 'ID المستخدم أو كلمة المرور غير صحيحة',
      );
      return;
    }

    final user = result['user'];

    if (user is! Map) {
      _message('بيانات المستخدم غير صالحة');
      return;
    }

    final loggedUserId = user['user_id']?.toString();
    final username = user['username']?.toString();
    final accessToken = result['access_token']?.toString();

    if (loggedUserId == null ||
        loggedUserId.isEmpty ||
        username == null ||
        username.isEmpty ||
        accessToken == null ||
        accessToken.isEmpty) {
      _message('بيانات المستخدم ناقصة');
      return;
    }

    await CallSession.instance.login(
      id: loggedUserId,
      name: username,
      token: accessToken,
    );

    await FirebaseMessagingService.instance.refreshTokenForCurrentUser();

    if (!mounted) return;

    Navigator.pushReplacement(
      context,
      MaterialPageRoute(builder: (_) => const HomeScreen()),
    );
  }

  void _message(String text, {bool success = false}) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text(text),
        backgroundColor: success
            ? const Color(0xFF00A85A)
            : Colors.red.shade800,
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Directionality(
      textDirection: TextDirection.rtl,
      child: Scaffold(
        backgroundColor: const Color(0xFF050505),
        body: SafeArea(
          child: Center(
            child: SingleChildScrollView(
              padding: const EdgeInsets.symmetric(
                horizontal: 24,
                vertical: 40,
              ),
              child: ConstrainedBox(
                constraints: const BoxConstraints(maxWidth: 430),
                child: Column(
                  children: [
                    _Logo(),

                    const SizedBox(height: 24),

                    const Text(
                      'CN CALL',
                      style: TextStyle(
                        fontSize: 34,
                        fontWeight: FontWeight.w800,
                        letterSpacing: 2,
                      ),
                    ),

                    const SizedBox(height: 8),

                    Text(
                      'مكالمات صوتية بدون أرقام هاتف',
                      style: TextStyle(
                        color: Colors.grey.shade500,
                        fontSize: 14,
                      ),
                    ),

                    const SizedBox(height: 42),

                    _Field(
                      controller: userIdController,
                      label: 'ID المستخدم',
                      hint: 'أدخل ID المستخدم',
                      icon: Icons.badge_outlined,
                      keyboardType: TextInputType.number,
                    ),

                    const SizedBox(height: 16),

                    _Field(
                      controller: passwordController,
                      label: 'كلمة المرور',
                      hint: 'أدخل كلمة المرور',
                      icon: Icons.lock_outline,
                      obscureText: hidePassword,
                      suffix: IconButton(
                        onPressed: () {
                          setState(() {
                            hidePassword = !hidePassword;
                          });
                        },
                        icon: Icon(
                          hidePassword
                              ? Icons.visibility_off
                              : Icons.visibility,
                        ),
                      ),
                    ),

                    const SizedBox(height: 24),

                    _PrimaryButton(
                      text: 'تسجيل الدخول',
                      onPressed: login,
                    ),

                    const SizedBox(height: 12),

                    SizedBox(
                      width: double.infinity,
                      height: 52,
                      child: OutlinedButton(
                        onPressed: openRegister,
                        style: OutlinedButton.styleFrom(
                          foregroundColor: Colors.white,
                          side: BorderSide(color: Colors.grey.shade800),
                          shape: RoundedRectangleBorder(
                            borderRadius: BorderRadius.circular(18),
                          ),
                        ),
                        child: const Text(
                          'إنشاء حساب جديد',
                          style: TextStyle(
                            fontSize: 16,
                            fontWeight: FontWeight.w600,
                          ),
                        ),
                      ),
                    ),

                    const SizedBox(height: 28),

                    _Footer(),
                  ],
                ),
              ),
            ),
          ),
        ),
      ),
    );
  }
}

// ============================================================
// HOME
// ============================================================

class HomeScreen extends StatefulWidget {
  const HomeScreen({super.key});

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  final callIdController = TextEditingController();

  final List<Map<String, String>> _contacts = [];
  final List<Map<String, dynamic>> _callHistory = [];

  bool _loadingData = true;


  @override
  void initState() {
    super.initState();

    _loadLocalData();
    _loadMissedCalls();

    final rtcManager = RtcCallManager.instance;
    rtcManager.startListening();

    rtcManager.onDisconnected = () {
      // Native Telecom handles incoming UI teardown.
    };

  }


  Future<void> _loadLocalData() async {
    final prefs = await SharedPreferences.getInstance();

    // طھظ†ط¸ظٹظپ ط¨ظٹط§ظ†ط§طھ ط§ظ„طھط¬ط§ط±ط¨ ط§ظ„ظ‚ط¯ظٹظ…ط© ظ…ط±ط© ظˆط§ط­ط¯ط©.
    const cleanupKey = 'cn_call_real_data_cleanup_v1';
    final cleaned = prefs.getBool(cleanupKey) ?? false;

    if (!cleaned) {
      await prefs.remove('cn_call_contacts');
      await prefs.remove('cn_call_history');
      await prefs.setBool(cleanupKey, true);
    }

    final contacts = prefs.getStringList('cn_call_contacts') ?? [];

    final loadedContacts = <Map<String, String>>[];

    for (final item in contacts) {
      final parts = item.split('|');

      if (parts.length >= 2) {
        loadedContacts.add({
          'id': parts[0],
          'name': parts.sublist(1).join('|'),
        });
      }
    }

    final history = prefs.getStringList('cn_call_history') ?? [];

    final loadedHistory = <Map<String, dynamic>>[];

    for (final item in history) {
      final parts = item.split('|');

      if (parts.length >= 4) {
        loadedHistory.add({
          'id': parts[0],
          'name': parts[1],
          'incoming': parts[2] == '1',
          'time': parts.sublist(3).join('|'),
        });
      }
    }

    if (!mounted) return;

    setState(() {
      _contacts
        ..clear()
        ..addAll(loadedContacts);

      _callHistory
        ..clear()
        ..addAll(loadedHistory);

      _loadingData = false;
    });
  }

  Future<void> _loadMissedCalls() async {
    final userId = CallSession.instance.userId;
    if (userId == null || userId.isEmpty) return;

    final missed = await AccountApi.missedCalls(userId: userId);
    if (!mounted || missed.isEmpty) return;

    for (final call in missed) {
      final callerId = call['caller_id']?.toString() ?? '';
      final callerName = call['caller_name']?.toString() ?? 'مستخدم CN CALL';
      if (callerId.isEmpty) continue;
      await _addHistory(name: callerName, id: callerId, incoming: true);
    }

    if (!mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(
      const SnackBar(content: Text('لديك مكالمة فائتة')),
    );
  }

  Future<void> _saveHistory() async {
    final prefs = await SharedPreferences.getInstance();

    await prefs.setStringList(
      'cn_call_history',
      _callHistory
          .map(
            (item) =>
                '${item['id']}|${item['name']}|${item['incoming'] == true ? '1' : '0'}|${item['time']}',
          )
          .toList(),
    );
  }

  Future<void> _addHistory({
    required String name,
    required String id,
    required bool incoming,
  }) async {
    final now = DateTime.now();

    final time =
        '${now.day.toString().padLeft(2, '0')}/'
        '${now.month.toString().padLeft(2, '0')} '
        '${now.hour.toString().padLeft(2, '0')}:'
        '${now.minute.toString().padLeft(2, '0')}';

    final item = <String, dynamic>{
      'id': id,
      'name': name,
      'incoming': incoming,
      'time': time,
    };

    if (mounted) {
      setState(() {
        _callHistory.insert(0, item);

        if (_callHistory.length > 50) {
          _callHistory.removeLast();
        }
      });
    }

    await _saveHistory();
  }



  @override
  void dispose() {
    callIdController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Directionality(
      textDirection: TextDirection.rtl,
      child: Scaffold(
        backgroundColor: const Color(0xFF050505),
        appBar: AppBar(
          backgroundColor: Colors.transparent,
          elevation: 0,
          title: const Text(
            'CN CALL',
            style: TextStyle(fontWeight: FontWeight.w800, letterSpacing: 1.5),
          ),
          actions: [
            IconButton(
              tooltip: 'تسجيل الخروج',
              onPressed: () async {
                final navigator = Navigator.of(context);

                final shouldLogout = await showDialog<bool>(
                  context: context,
                  builder: (dialogContext) {
                    return AlertDialog(
                      backgroundColor: const Color(0xFF151515),
                      title: const Text(
                        'تسجيل الخروج',
                        textDirection: TextDirection.rtl,
                      ),
                      content: const Text(
                        'هل تريد تسجيل الخروج من الحساب الحالي؟',
                        textDirection: TextDirection.rtl,
                      ),
                      actions: [
                        TextButton(
                          onPressed: () => Navigator.pop(dialogContext, false),
                          child: const Text('إلغاء'),
                        ),
                        FilledButton(
                          onPressed: () => Navigator.pop(dialogContext, true),
                          style: FilledButton.styleFrom(
                            backgroundColor: Colors.red,
                          ),
                          child: const Text('تسجيل الخروج'),
                        ),
                      ],
                    );
                  },
                );

                if (shouldLogout != true || !mounted) return;

                await CallSession.instance.logout();

                if (!mounted) return;

                navigator.pushAndRemoveUntil(
                  MaterialPageRoute(builder: (_) => const LoginScreen()),
                  (route) => false,
                );
              },
              icon: const Icon(Icons.logout_outlined),
            ),
          ],
        ),
        body: SafeArea(
          child: _loadingData
              ? const Center(child: CircularProgressIndicator())
              : SingleChildScrollView(
                  padding: const EdgeInsets.fromLTRB(20, 10, 20, 30),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.stretch,
                    children: [
                      Container(
                        padding: const EdgeInsets.all(22),
                        decoration: BoxDecoration(
                          color: const Color(0xFF151515),
                          borderRadius: BorderRadius.circular(24),
                          border: Border.all(
                            color: const Color(0xFF00E676)
                                .withValues(alpha: .18),
                          ),
                        ),
                        child: Row(
                          children: [
                            Container(
                              width: 58,
                              height: 58,
                              decoration: BoxDecoration(
                                shape: BoxShape.circle,
                                color: const Color(0xFF00E676)
                                    .withValues(alpha: .12),
                              ),
                              child: const Icon(
                                Icons.person,
                                color: Color(0xFF00E676),
                                size: 30,
                              ),
                            ),
                            const SizedBox(width: 16),
                            Expanded(
                              child: Column(
                                crossAxisAlignment: CrossAxisAlignment.start,
                                children: [
                                  const Text(
                                    'مرحباً بك',
                                    style: TextStyle(
                                      color: Colors.grey,
                                      fontSize: 13,
                                    ),
                                  ),
                                  const SizedBox(height: 4),
                                  Text(
                                    CallSession.instance.displayName ??
                                        'مستخدم CN CALL',
                                    style: const TextStyle(
                                      color: Colors.white,
                                      fontSize: 22,
                                      fontWeight: FontWeight.w800,
                                    ),
                                  ),
                                  const SizedBox(height: 4),
                                  Text(
                                    'ID: ${CallSession.instance.userId ?? ''}',
                                    style: const TextStyle(
                                      color: Color(0xFF00E676),
                                      fontSize: 13,
                                    ),
                                  ),
                                ],
                              ),
                            ),
                          ],
                        ),
                      ),

                      const SizedBox(height: 20),
                    ],
                  ),
                ),
          ),
        ),
      );
  }
}

// ============================================================
// REGISTER
// ============================================================

class RegisterScreen extends StatefulWidget {
  const RegisterScreen({super.key});

  @override
  State<RegisterScreen> createState() => _RegisterScreenState();
}

class _RegisterScreenState extends State<RegisterScreen> {
  final userIdController = TextEditingController();

  final usernameController = TextEditingController();
  final passwordController = TextEditingController();
  final confirmPasswordController = TextEditingController();

  bool hidePassword = true;
  bool hideConfirmPassword = true;

  @override
  void dispose() {
    userIdController.dispose();

    usernameController.dispose();
    passwordController.dispose();
    confirmPasswordController.dispose();
    super.dispose();
  }

  Future<void> createAccount() async {
    final userId = userIdController.text.trim();
    final username = usernameController.text.trim();
    final password = passwordController.text;
    final confirmPassword = confirmPasswordController.text;

    if (userId.isEmpty ||
        username.isEmpty ||
        password.isEmpty ||
        confirmPassword.isEmpty) {
      _message('أكمل جميع البيانات');
      return;
    }

    if (int.tryParse(userId) == null) {
      _message('ID المستخدم يجب أن يكون أرقامًا فقط');
      return;
    }

    if (username.length < 3) {
      _message('اسم المستخدم يجب أن يكون 3 أحرف على الأقل');
      return;
    }

    if (password.length < 6) {
      _message('كلمة المرور يجب أن تكون 6 أحرف على الأقل');
      return;
    }

    if (password != confirmPassword) {
      _message('كلمتا المرور غير متطابقتين');
      return;
    }

    final result = await AccountApi.register(
      userId: userId,
      username: username,
      password: password,
    );

    if (!mounted) return;

    final success = result['success'] == true;

    if (!success) {
      _message(
        result['message']?.toString() ?? 'تعذر إنشاء الحساب',
      );
      return;
    }

    final loginResult = await AccountApi.login(
      userId: userId,
      password: password,
    );

    if (!mounted) return;

    final loginSuccess = loginResult['success'] == true;
    if (!loginSuccess) {
      _message(
        loginResult['message']?.toString() ?? 'تم إنشاء الحساب، ولكن تعذر تسجيل الدخول التلقائي',
      );
      return;
    }

    final user = loginResult['user'];
    if (user is! Map) {
      _message('بيانات المستخدم غير صالحة');
      return;
    }

    final loggedUserId = user['user_id']?.toString();
    final loggedUsername = user['username']?.toString() ?? username;
    final accessToken = loginResult['access_token']?.toString();

    if (loggedUserId == null ||
        loggedUserId.isEmpty ||
        accessToken == null ||
        accessToken.isEmpty) {
      _message('بيانات الجلسة ناقصة');
      return;
    }

    await CallSession.instance.login(
      id: loggedUserId,
      name: loggedUsername,
      token: accessToken,
    );

    await FirebaseMessagingService.instance.refreshTokenForCurrentUser();

    if (!mounted) return;

    _message('تم إنشاء الحساب وتسجيل الدخول بنجاح', success: true);

    Navigator.pushAndRemoveUntil(
      context,
      MaterialPageRoute(builder: (_) => const HomeScreen()),
      (route) => false,
    );
  }

  void _message(String text, {bool success = false}) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text(text),
        backgroundColor: success
            ? const Color(0xFF00A85A)
            : Colors.red.shade800,
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Directionality(
      textDirection: TextDirection.rtl,
      child: Scaffold(
        backgroundColor: const Color(0xFF050505),
        appBar: AppBar(
          backgroundColor: Colors.transparent,
          elevation: 0,
          leading: IconButton(
            onPressed: () => Navigator.pop(context),
            icon: const Icon(Icons.arrow_back),
          ),
        ),
        body: SafeArea(
          child: Center(
            child: SingleChildScrollView(
              padding: const EdgeInsets.fromLTRB(24, 10, 24, 40),
              child: ConstrainedBox(
                constraints: const BoxConstraints(maxWidth: 430),
                child: Column(
                  children: [
                    _Logo(),

                    const SizedBox(height: 22),

                    const Text(
                      'إنشاء حساب',
                      style: TextStyle(
                        fontSize: 30,
                        fontWeight: FontWeight.w800,
                      ),
                    ),

                    const SizedBox(height: 8),

                    Text(
                      'أنشئ حسابك في CN CALL',
                      style: TextStyle(
                        color: Colors.grey.shade500,
                        fontSize: 14,
                      ),
                    ),

                    const SizedBox(height: 34),

                    _Field(
                      controller: userIdController,
                      label: 'ID المستخدم',
                      hint: 'الرقم الذي تستخدمه لتسجيل الدخول والتواصل',
                      icon: Icons.badge_outlined,
                      keyboardType: TextInputType.number,
                    ),

                    const SizedBox(height: 16),

                    _Field(
                      controller: usernameController,
                      label: 'اسم المستخدم',
                      hint: 'الاسم الذي سيظهر للآخرين أثناء المكالمة',
                      icon: Icons.person_outline,
                    ),

                    const SizedBox(height: 16),

                    _Field(
                      controller: passwordController,
                      label: 'كلمة المرور',
                      hint: '6 أحرف على الأقل',
                      icon: Icons.lock_outline,
                      obscureText: hidePassword,
                      suffix: IconButton(
                        onPressed: () {
                          setState(() {
                            hidePassword = !hidePassword;
                          });
                        },
                        icon: Icon(
                          hidePassword
                              ? Icons.visibility_off
                              : Icons.visibility,
                        ),
                      ),
                    ),

                    const SizedBox(height: 16),

                    _Field(
                      controller: confirmPasswordController,
                      label: 'تأكيد كلمة المرور',
                      hint: 'أعد كتابة كلمة المرور',
                      icon: Icons.lock_reset_outlined,
                      obscureText: hideConfirmPassword,
                      suffix: IconButton(
                        onPressed: () {
                          setState(() {
                            hideConfirmPassword = !hideConfirmPassword;
                          });
                        },
                        icon: Icon(
                          hideConfirmPassword
                              ? Icons.visibility_off
                              : Icons.visibility,
                        ),
                      ),
                    ),

                    const SizedBox(height: 24),

                    _PrimaryButton(
                      text: 'إنشاء الحساب',
                      onPressed: createAccount,
                    ),

                    const SizedBox(height: 16),

                    TextButton(
                      onPressed: () => Navigator.pop(context),
                      child: const Text(
                        'لدي حساب بالفعل',
                        style: TextStyle(
                          color: Color(0xFF00E676),
                        ),
                      ),
                    ),

                    const SizedBox(height: 20),

                    _Footer(),
                  ],
                ),
              ),
            ),
          ),
        ),
      ),
    );
  }
}



// ============================================================
// SHARED UI
// ============================================================

class _Logo extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Container(
      width: 88,
      height: 88,
      decoration: BoxDecoration(
        shape: BoxShape.circle,
        color: const Color(0xFF00E676).withValues(alpha: .10),
        border: Border.all(color: const Color(0xFF00E676), width: 2),
      ),
      child: const Icon(Icons.call, size: 40, color: Color(0xFF00E676)),
    );
  }
}

class _Field extends StatelessWidget {
  final TextEditingController controller;
  final String label;
  final String hint;
  final IconData icon;
  final bool obscureText;
  final TextInputType? keyboardType;
  final Widget? suffix;

  const _Field({
    required this.controller,
    required this.label,
    required this.hint,
    required this.icon,
    this.obscureText = false,
    this.keyboardType,
    this.suffix,
  });

  @override
  Widget build(BuildContext context) {
    return TextField(
      controller: controller,
      obscureText: obscureText,
      keyboardType: keyboardType,
      textDirection: TextDirection.ltr,
      decoration: InputDecoration(
        labelText: label,
        hintText: hint,
        prefixIcon: Icon(icon),
        suffixIcon: suffix,
        filled: true,
        fillColor: const Color(0xFF151515),
        border: OutlineInputBorder(
          borderRadius: BorderRadius.circular(18),
          borderSide: BorderSide.none,
        ),
        focusedBorder: OutlineInputBorder(
          borderRadius: BorderRadius.circular(18),
          borderSide: const BorderSide(color: Color(0xFF00E676)),
        ),
      ),
    );
  }
}

class _PrimaryButton extends StatelessWidget {
  final String text;
  final VoidCallback onPressed;

  const _PrimaryButton({required this.text, required this.onPressed});

  @override
  Widget build(BuildContext context) {
    return SizedBox(
      width: double.infinity,
      height: 56,
      child: FilledButton(
        onPressed: onPressed,
        style: FilledButton.styleFrom(
          backgroundColor: const Color(0xFF00E676),
          foregroundColor: Colors.black,
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(18),
          ),
        ),
        child: Text(
          text,
          style: const TextStyle(fontSize: 17, fontWeight: FontWeight.bold),
        ),
      ),
    );
  }
}

class _Footer extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Column(
      mainAxisSize: MainAxisSize.min,
      children: [
        Text(
          'CAFEE NET',
          style: TextStyle(
            color: Colors.grey.shade800.withValues(alpha: .45),
            fontSize: 12,
            fontWeight: FontWeight.w700,
            letterSpacing: 2.2,
          ),
        ),
        const SizedBox(height: 3),
        Text(
          'هشام الريمي',
          style: TextStyle(
            color: Colors.grey.shade800.withValues(alpha: .35),
            fontSize: 9,
            fontWeight: FontWeight.w500,
            letterSpacing: .8,
          ),
        ),
      ],
    );
  }
}




