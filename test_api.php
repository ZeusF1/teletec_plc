<?php
/**
 * test_api.php — Diagnose API hanging issues
 *
 * Open in browser: http://192.168.100.100:8000/rt2/test_api.php
 */
header('Content-Type: text/plain; charset=utf-8');

echo "=== RT4 API Diagnostic ===\n\n";

// 1. PHP info
echo "1. PHP: " . phpversion() . "\n";
echo "   SAPI: " . php_sapi_name() . "\n";
echo "   PID: " . getmypid() . "\n\n";

// 2. Session test
echo "2. Session test:\n";
echo "   Cookie PHPSESSID: " . (isset($_COOKIE['PHPSESSID']) ? $_COOKIE['PHPSESSID'] : '(none)') . "\n";
$t0 = microtime(true);
$ok = @session_start(['read_and_close' => true]);
$t1 = microtime(true);
echo "   session_start(read_and_close): " . ($ok ? 'OK' : 'FAIL') . " in " . round(($t1-$t0)*1000) . "ms\n";
echo "   SESSION login: " . (isset($_SESSION['login']) ? $_SESSION['login'] : '(not set)') . "\n\n";

// 3. DB open test
echo "3. Database test:\n";
$db_path = '/usr/local/rt4/mtxdb.sdb';
echo "   Path: $db_path\n";
echo "   Exists: " . (file_exists($db_path) ? 'YES' : 'NO') . "\n";
echo "   Size: " . (file_exists($db_path) ? round(filesize($db_path)/1024) . " KB" : '-') . "\n";

$t0 = microtime(true);
try {
    $db = new SQLite3($db_path, SQLITE3_OPEN_READONLY);
    $db->busyTimeout(2000);
    $t1 = microtime(true);
    echo "   Open: OK in " . round(($t1-$t0)*1000) . "ms\n";

    // WAL mode
    $mode = $db->querySingle("PRAGMA journal_mode");
    echo "   Journal mode: $mode\n";

    // Try WAL
    @$db->exec('PRAGMA journal_mode=WAL');
    $mode2 = $db->querySingle("PRAGMA journal_mode");
    echo "   After WAL set: $mode2\n";

    // Simple query
    $t0 = microtime(true);
    $cnt = $db->querySingle("SELECT COUNT(*) FROM Meters WHERE (DELETED=0 OR DELETED IS NULL)");
    $t1 = microtime(true);
    echo "   Meters count: $cnt in " . round(($t1-$t0)*1000) . "ms\n";

    // Check locks
    $t0 = microtime(true);
    $res = $db->querySingle("SELECT COUNT(*) FROM COMSchedule");
    $t1 = microtime(true);
    echo "   COMSchedule count: $res in " . round(($t1-$t0)*1000) . "ms\n";

    $db->close();
} catch (Exception $e) {
    $t1 = microtime(true);
    echo "   ERROR in " . round(($t1-$t0)*1000) . "ms: " . $e->getMessage() . "\n";
}

// 4. Session file info
echo "\n4. Session files:\n";
$sess_path = session_save_path() ?: sys_get_temp_dir();
echo "   Save path: $sess_path\n";
$files = glob($sess_path . '/sess_*');
echo "   Session files: " . count($files) . "\n";
if (isset($_COOKIE['PHPSESSID'])) {
    $sf = $sess_path . '/sess_' . $_COOKIE['PHPSESSID'];
    if (file_exists($sf)) {
        echo "   Current session file: " . filesize($sf) . " bytes, age: " . (time() - filemtime($sf)) . "s\n";
    }
}

// 5. PHP-FPM pool info
echo "\n5. PHP-FPM:\n";
$ps = @shell_exec('ps | grep php-fpm | grep -v grep 2>/dev/null');
$workers = substr_count($ps, 'pool www');
echo "   Workers: $workers\n";
echo "   Max children: " . ini_get('pm.max_children') . "\n";

// 6. Memory
echo "\n6. Memory:\n";
echo "   PHP peak: " . round(memory_get_peak_usage()/1024) . " KB\n";
$free = @shell_exec('free -m 2>/dev/null');
if ($free) echo "   System:\n$free\n";

// 7. fuser on DB
echo "7. DB lock holders:\n";
$fuser = @shell_exec("fuser $db_path 2>/dev/null");
echo "   PIDs: " . trim($fuser ?: '(none)') . "\n";

echo "\n=== Done ===\n";
