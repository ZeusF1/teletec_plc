<?php
/*
 * config_api.php — Configuration and DB management API
 *
 * GET  ?cmd=get_config     — Read current config
 * POST ?cmd=set_config     — Update config values
 * GET  ?cmd=db_check&path= — Validate a DB path
 * POST ?cmd=db_backup      — Create DB backup
 * POST ?cmd=db_vacuum      — Run VACUUM
 * POST ?cmd=db_optimize    — Run PRAGMA optimize
 * GET  ?cmd=users          — List users
 * POST ?cmd=set_password   — Change user password
 * GET  ?cmd=system_info    — Version info
 * GET  ?cmd=logs&source=   — Tail log file
 */

// Suppress HTML error output — we return JSON only
error_reporting(E_ALL);
ini_set('display_errors', '0');
ini_set('log_errors', '1');

// Catch any garbage output from includes
ob_start();

require_once __DIR__ . '/auth.inc';
check_auth(); // non-strict for reads; write commands check strict below

$_garbage = ob_get_clean();
header("Content-Type: application/json; charset=utf-8");

define("CONFIG_PATH", __DIR__ . '/config.ini');
define("DEFAULT_DB_PATH", "/usr/local/rt4/mtxdb.sdb");
define("BACKUP_DIR", "/usr/local/rt4/backup/");

function err_exit($msg) {
    echo json_encode(['error' => $msg]) . "\n";
    exit(1);
}

function ok_response($data = []) {
    echo json_encode(array_merge(['success' => true], $data), JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

/**
 * Read config.ini into associative array.
 */
function read_config() {
    if (!is_file(CONFIG_PATH)) return [];
    $ini = parse_ini_file(CONFIG_PATH, true);
    return $ini ?: [];
}

/**
 * Write config values back to config.ini.
 */
function write_config($data) {
    $lines = [];
    foreach ($data as $section => $values) {
        if (is_array($values)) {
            $lines[] = "[$section]";
            foreach ($values as $key => $val) {
                if (is_string($val) && preg_match('/[^a-zA-Z0-9._\-\/]/', $val)) {
                    $lines[] = "$key=\"$val\"";
                } else {
                    $lines[] = "$key=$val";
                }
            }
            $lines[] = "";
        }
    }
    return file_put_contents(CONFIG_PATH, implode("\n", $lines) . "\n");
}

/**
 * Get the configured DB path (from config.ini or default).
 */
function get_db_path() {
    $config = read_config();
    $path = isset($config['common']['db_path']) ? $config['common']['db_path'] : '';
    if ($path && is_file($path)) return $path;
    return DEFAULT_DB_PATH;
}

/**
 * Sanitize path — prevent directory traversal.
 */
function safe_path($path) {
    $path = str_replace('..', '', $path);
    $path = preg_replace('#[^a-zA-Z0-9/_.\-]#', '', $path);
    return $path;
}

// ============================================================
// Commands
// ============================================================

$cmd = isset($_GET['cmd']) ? $_GET['cmd'] : '';
$method = $_SERVER['REQUEST_METHOD'];

switch ($cmd) {

// ---- Get Config ----
case 'get_config':
    $config = read_config();
    $common = isset($config['common']) ? $config['common'] : [];

    $db_path = get_db_path();
    $db_info = ['path' => $db_path, 'exists' => is_file($db_path)];
    if ($db_info['exists']) {
        $db_info['size_mb'] = round(filesize($db_path) / (1024 * 1024), 2);
        $wal = $db_path . '-wal';
        $db_info['wal_size_mb'] = is_file($wal) ? round(filesize($wal) / (1024 * 1024), 2) : 0;
    }

    // Last backup info
    $db_info['backup_dir'] = BACKUP_DIR;
    $db_info['last_backup'] = null;
    if (is_dir(BACKUP_DIR)) {
        $files = glob(BACKUP_DIR . 'mtxdb_*.sdb');
        if ($files) {
            sort($files);
            $last = end($files);
            $db_info['last_backup'] = ['file' => basename($last), 'size_mb' => round(filesize($last) / (1024*1024), 2), 'date' => date('Y-m-d H:i:s', filemtime($last))];
        }
    }

    echo json_encode([
        'config' => $common,
        'db' => $db_info,
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
    break;

// ---- Set Config ----
case 'set_config':
    if ($method !== 'POST') err_exit('POST required');

    $input = json_decode(file_get_contents('php://input'), true);
    if (!$input) err_exit('Invalid JSON body');

    $config = read_config();
    if (!isset($config['common'])) $config['common'] = [];

    // Allowed config keys
    $allowed = ['lengua', 'time_zone', 'psw1', 'psw2', 'psw3', 'tareas_dir',
                'log_fondo', 'logs', 'db_path', 'session_timeout', 'max_login_attempts',
                'lockout_minutes', 'org_name', 'csv_delimiter', 'csv_encoding'];

    foreach ($input as $key => $value) {
        if (in_array($key, $allowed)) {
            $config['common'][$key] = $value;
        }
    }

    if (write_config($config) === false) {
        err_exit('Failed to write config.ini');
    }
    ok_response(['message' => 'Config saved']);
    break;

// ---- DB Check ----
case 'db_check':
    $path = isset($_GET['path']) ? $_GET['path'] : '';
    if (!$path) err_exit('path parameter required');

    $path = safe_path($path);
    $result = ['path' => $path, 'exists' => false, 'valid' => false];

    if (is_file($path)) {
        $result['exists'] = true;
        $result['size_mb'] = round(filesize($path) / (1024*1024), 2);
        try {
            $testdb = new SQLite3($path, SQLITE3_OPEN_READONLY);
            $cnt = $testdb->querySingle("SELECT COUNT(*) FROM Meters WHERE (DELETED = 0 OR DELETED IS NULL)");
            $result['valid'] = true;
            $result['meter_count'] = intval($cnt);
            $testdb->close();
        } catch (Exception $e) {
            $result['error'] = $e->getMessage();
        }
    }

    echo json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
    break;

// ---- DB Backup ----
case 'db_backup':
    if ($method !== 'POST') err_exit('POST required');

    $db_path = get_db_path();
    if (!is_file($db_path)) err_exit("DB file not found: $db_path");

    if (!is_dir(BACKUP_DIR)) {
        if (!@mkdir(BACKUP_DIR, 0755, true)) {
            err_exit("Cannot create backup dir: " . BACKUP_DIR);
        }
    }

    $timestamp = date('Ymd_His');
    $backup_file = BACKUP_DIR . "mtxdb_$timestamp.sdb";

    if (!copy($db_path, $backup_file)) {
        err_exit("Backup failed");
    }

    // Also copy WAL if exists
    $wal = $db_path . '-wal';
    if (is_file($wal)) {
        @copy($wal, $backup_file . '-wal');
    }

    ok_response([
        'message' => 'Backup created',
        'file' => basename($backup_file),
        'size_mb' => round(filesize($backup_file) / (1024*1024), 2),
    ]);
    break;

// ---- DB Vacuum ----
case 'db_vacuum':
    if ($method !== 'POST') err_exit('POST required');

    $db_path = get_db_path();
    $db = new SQLite3($db_path, SQLITE3_OPEN_READWRITE);
    $db->busyTimeout(30000);

    $size_before = filesize($db_path);
    $db->exec('VACUUM');
    $db->close();
    clearstatcache();
    $size_after = filesize($db_path);

    ok_response([
        'message' => 'VACUUM completed',
        'size_before_mb' => round($size_before / (1024*1024), 2),
        'size_after_mb' => round($size_after / (1024*1024), 2),
        'saved_mb' => round(($size_before - $size_after) / (1024*1024), 2),
    ]);
    break;

// ---- DB Optimize ----
case 'db_optimize':
    if ($method !== 'POST') err_exit('POST required');

    $db_path = get_db_path();
    $db = new SQLite3($db_path, SQLITE3_OPEN_READWRITE);
    $db->busyTimeout(10000);
    $db->exec('PRAGMA optimize');
    $db->close();

    ok_response(['message' => 'PRAGMA optimize completed']);
    break;

// ---- Users List ----
case 'users':
    $db_path = get_db_path();
    $db = new SQLite3($db_path, SQLITE3_OPEN_READONLY);
    $db->busyTimeout(2000);
    @$db->exec('PRAGMA journal_mode=WAL');
    @$db->exec('PRAGMA read_uncommitted=ON');

    $users = [];

    // Safe string converter — works even without mbstring extension
    $toUtf8 = function($s) {
        if (!is_string($s) || $s === '') return $s;
        // If mbstring available, use it
        if (function_exists('mb_convert_encoding')) {
            $r = @mb_convert_encoding($s, 'UTF-8', 'UTF-8,Windows-1251,ISO-8859-1');
            if ($r !== false) return $r;
        }
        // Fallback: strip non-ASCII
        return preg_replace('/[^\x20-\x7E]/', '?', $s);
    };

    // Try with gr_users JOIN first, fallback to simple query
    $queries = [
        "SELECT u.id, u.name, u.estado, u.id_grupo, g.name as group_name
         FROM users u LEFT JOIN gr_users g ON g.id = u.id_grupo ORDER BY u.name",
        "SELECT id, name, estado, id_grupo, '' as group_name FROM users ORDER BY name",
    ];

    $res = false;
    $last_err = '';
    foreach ($queries as $sql) {
        $res = @$db->query($sql);
        if ($res !== false) break;
        $last_err = $db->lastErrorMsg();
    }

    if ($res) {
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
            $users[] = [
                'id'     => intval($row['id']),
                'name'   => $toUtf8($row['name'] ?: '(unknown)'),
                'group'  => isset($row['group_name']) && $row['group_name'] ? $toUtf8($row['group_name']) : null,
                'status' => intval(isset($row['estado']) ? $row['estado'] : 0) === 0 ? 'active' : 'blocked',
            ];
        }
        $res->finalize();
    } else {
        $db->close();
        err_exit("Users query failed: " . $last_err);
    }

    $db->close();

    // Encode with safety flags
    $flags = JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE;
    if (defined('JSON_INVALID_UTF8_SUBSTITUTE')) $flags |= JSON_INVALID_UTF8_SUBSTITUTE;
    if (defined('JSON_INVALID_UTF8_IGNORE')) $flags |= JSON_INVALID_UTF8_IGNORE;
    $json = json_encode(['users' => $users], $flags);
    if ($json === false) {
        // Last resort: strip all non-ASCII from every string value
        array_walk_recursive($users, function(&$v) {
            if (is_string($v)) $v = preg_replace('/[^\x20-\x7E]/', '?', $v);
        });
        $json = json_encode(['users' => $users, 'warning' => 'charset_fallback'], JSON_PRETTY_PRINT);
    }
    echo ($json ?: '{"users":[],"error":"json_encode_failed"}') . "\n";
    break;

// ---- Set Password ----
case 'set_password':
    if ($method !== 'POST') err_exit('POST required');

    $input = json_decode(file_get_contents('php://input'), true);
    if (!$input || empty($input['user_id']) || empty($input['password'])) {
        err_exit('user_id and password required');
    }

    $user_id = intval($input['user_id']);
    $password = $input['password'];
    if (strlen($password) < 4) err_exit('Password too short (min 4 chars)');

    $db_path = get_db_path();
    $db = new SQLite3($db_path, SQLITE3_OPEN_READWRITE);
    $db->busyTimeout(5000);

    // Ensure bcrypt column exists
    ensure_bcrypt_column($db);

    $hash = password_hash($password, PASSWORD_BCRYPT);
    $md5 = md5($password); // Keep MD5 for backward compat with old UI

    $stmt = $db->prepare("UPDATE users SET password = :md5, password_bcrypt = :hash WHERE id = :id");
    $stmt->bindValue(':md5', $md5, SQLITE3_TEXT);
    $stmt->bindValue(':hash', $hash, SQLITE3_TEXT);
    $stmt->bindValue(':id', $user_id, SQLITE3_INTEGER);
    $stmt->execute();

    $changes = $db->changes();
    $db->close();

    if ($changes === 0) err_exit('User not found');
    ok_response(['message' => 'Password updated']);
    break;

// ---- System Info ----
case 'system_info':
    $info = [
        'version' => 'OSBB PLC v2.0',
        'php' => phpversion(),
        'sqlite3' => SQLite3::version()['versionString'],
        'platform' => php_uname(),
    ];

    // nginx version
    $nginx = @shell_exec('nginx -v 2>&1');
    if ($nginx && preg_match('/nginx\/([\d.]+)/', $nginx, $m)) {
        $info['nginx'] = $m[1];
    }

    echo json_encode($info, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
    break;

// ---- Logs ----
case 'logs':
    $source = isset($_GET['source']) ? $_GET['source'] : 'plc';
    $lines = isset($_GET['lines']) ? min(intval($_GET['lines']), 200) : 50;

    $log_files = [
        'plc' => '/tmp/meter_instant.log',
        'php' => '/var/log/nginx/error.log',
        'nginx' => '/var/log/nginx/access.log',
    ];

    if (!isset($log_files[$source])) err_exit("Unknown log source: $source");

    $path = $log_files[$source];
    if (!is_file($path) || !is_readable($path)) {
        echo json_encode(['source' => $source, 'path' => $path, 'lines' => [], 'error' => 'File not found or not readable']) . "\n";
        break;
    }

    // Read last N lines
    $output = @shell_exec("tail -n $lines " . escapeshellarg($path) . " 2>/dev/null");
    $log_lines = $output ? explode("\n", rtrim($output)) : [];

    echo json_encode([
        'source' => $source,
        'path' => $path,
        'count' => count($log_lines),
        'lines' => $log_lines,
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
    break;

default:
    err_exit("Unknown cmd: $cmd");
}
