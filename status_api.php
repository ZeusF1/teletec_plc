<?php
/*
 * status_api.php — System status and health metrics
 *
 * Usage (HTTP GET):
 *   status_api.php?cmd=system        — CPU, RAM, disk, uptime, temp
 *   status_api.php?cmd=db_health     — DB path, size, WAL size, integrity, last write
 *   status_api.php?cmd=queue_stats   — queue depth, flags breakdown, today's stats
 *   status_api.php?cmd=meter_summary — total, by type, online/offline counts
 *   status_api.php?cmd=collection    — today's collection progress per data type
 *   status_api.php?cmd=all           — all above in one call
 */

// Suppress HTML error output — we return JSON only
error_reporting(E_ALL);
ini_set('display_errors', '0');
ini_set('log_errors', '1');

// Catch any garbage output from includes (PHP warnings, BOM, etc.)
ob_start();

require_once __DIR__ . '/auth.inc';
check_auth();

// Discard any output from includes
$_garbage = ob_get_clean();

header("Content-Type: application/json; charset=utf-8");

define("DB_PATH", "/usr/local/rt4/mtxdb.sdb");

// --- Device type detection ---
define("A_TIPO_MTX1",       0x01);
define("A_TIPO_MTX1_2K",    0x17);
define("A_TIPO_MTX1_4K",    0x0B);
define("A_TIPO_MTX1_4K2",   0x15);
define("A_TIPO_MTX3_PV",    0x02);
define("A_TIPO_MTX3_TV",    0x03);
define("A_TIPO_MTX3_PV4K",  0x0C);
define("A_TIPO_MTX3_TV4K",  0x0D);
define("A_TIPO_MTX3_PV4K2", 0x16);

function is_mtx1($tipo) {
    return in_array($tipo, [A_TIPO_MTX1, A_TIPO_MTX1_2K, A_TIPO_MTX1_4K, A_TIPO_MTX1_4K2]);
}

function is_mtx3($tipo) {
    return in_array($tipo, [A_TIPO_MTX3_PV, A_TIPO_MTX3_TV, A_TIPO_MTX3_PV4K, A_TIPO_MTX3_TV4K, A_TIPO_MTX3_PV4K2]);
}

function get_device_tipo($serial) {
    $serial = trim($serial);
    if ((strlen($serial) == 16) && (strtoupper(substr($serial, 0, 6)) == "001A79")) {
        return hexdec(substr($serial, 6, 2));
    }
    return 0;
}

function err_exit($msg) {
    echo json_encode(['error' => $msg]) . "\n";
    exit(1);
}

// ============================================================
// System metrics
// ============================================================

function cmd_system() {
    $data = [];

    // Uptime
    if (is_readable('/proc/uptime')) {
        $parts = explode(' ', trim(file_get_contents('/proc/uptime')));
        $sec = floatval($parts[0]);
        $isec = intval($sec);
        $days = floor($isec / 86400);
        $hours = floor(($isec % 86400) / 3600);
        $mins = floor(($isec % 3600) / 60);
        $data['uptime_sec'] = $sec;
        $data['uptime_text'] = $days . 'д ' . $hours . 'г ' . $mins . 'хв';
    }

    // CPU load
    if (is_readable('/proc/loadavg')) {
        $parts = explode(' ', trim(file_get_contents('/proc/loadavg')));
        $data['load_1m'] = floatval($parts[0]);
        $data['load_5m'] = floatval($parts[1]);
        $data['load_15m'] = floatval($parts[2]);
    }

    // RAM
    if (is_readable('/proc/meminfo')) {
        $meminfo = file_get_contents('/proc/meminfo');
        preg_match('/MemTotal:\s+(\d+)/', $meminfo, $mt);
        preg_match('/MemAvailable:\s+(\d+)/', $meminfo, $ma);
        if ($mt && $ma) {
            $total_kb = intval($mt[1]);
            $avail_kb = intval($ma[1]);
            $used_kb = $total_kb - $avail_kb;
            $data['ram_total_mb'] = round($total_kb / 1024, 1);
            $data['ram_used_mb'] = round($used_kb / 1024, 1);
            $data['ram_percent'] = round(($used_kb / $total_kb) * 100, 1);
        }
    }

    // Disk
    $db_dir = dirname(DB_PATH);
    if (is_dir($db_dir)) {
        $total = disk_total_space($db_dir);
        $free = disk_free_space($db_dir);
        if ($total > 0) {
            $data['disk_total_mb'] = round($total / (1024 * 1024), 1);
            $data['disk_free_mb'] = round($free / (1024 * 1024), 1);
            $data['disk_used_mb'] = round(($total - $free) / (1024 * 1024), 1);
            $data['disk_percent'] = round((($total - $free) / $total) * 100, 1);
        }
    }

    // Temperature
    $temp_file = '/sys/class/thermal/thermal_zone0/temp';
    if (is_readable($temp_file)) {
        $temp = intval(trim(file_get_contents($temp_file)));
        $data['temp_c'] = round($temp / 1000, 1);
    }

    // PHP version
    $data['php_version'] = phpversion();

    return $data;
}

// ============================================================
// Database health
// ============================================================

function cmd_db_health($db) {
    $data = [];

    $data['path'] = DB_PATH;
    $data['size_mb'] = is_file(DB_PATH) ? round(filesize(DB_PATH) / (1024 * 1024), 2) : null;

    $wal_path = DB_PATH . '-wal';
    $data['wal_size_mb'] = is_file($wal_path) ? round(filesize($wal_path) / (1024 * 1024), 2) : 0;

    // Integrity check (cached in session — max 1x per hour)
    $integrity = 'unknown';
    $cache_file = '/tmp/osbb_integrity_cache.json';
    $cache_valid = false;
    if (is_file($cache_file)) {
        $cache = json_decode(file_get_contents($cache_file), true);
        if ($cache && isset($cache['time']) && (time() - $cache['time']) < 3600) {
            $integrity = $cache['result'];
            $cache_valid = true;
        }
    }
    if (!$cache_valid) {
        $res = $db->querySingle("PRAGMA integrity_check");
        $integrity = ($res === 'ok') ? 'ok' : $res;
        @file_put_contents($cache_file, json_encode(['time' => time(), 'result' => $integrity]));
    }
    $data['integrity'] = $integrity;

    // Last write — MAX fecha from indicaciones tables
    $last_write = null;
    $res1 = $db->querySingle("SELECT MAX(fecha) FROM indicaciones_mtx1");
    $res3 = $db->querySingle("SELECT MAX(fecha) FROM indicaciones_mtx3");
    if ($res1 && $res3) {
        $last_write = ($res1 > $res3) ? $res1 : $res3;
    } elseif ($res1) {
        $last_write = $res1;
    } elseif ($res3) {
        $last_write = $res3;
    }
    $data['last_write'] = $last_write;

    // Table row counts
    $data['table_counts'] = [];
    $tables = ['Meters', 'COMSchedule', 'indicaciones_mtx1', 'indicaciones_mtx3', 'mediciones_mtx1', 'mediciones_mtx3'];
    foreach ($tables as $t) {
        $cnt = $db->querySingle("SELECT COUNT(*) FROM $t");
        $data['table_counts'][$t] = ($cnt !== false) ? intval($cnt) : null;
    }

    return $data;
}

// ============================================================
// Queue stats (COMSchedule)
// ============================================================

function cmd_queue_stats($db) {
    $data = ['pending' => 0, 'idle' => 0, 'disabled' => 0, 'total' => 0, 'flags_breakdown' => []];

    // Flags bitmask labels
    $flag_names = [
        0x0001 => 'LP',
        0x0002 => 'MR',
        0x0010 => 'Ms',
        0x0020 => 'Events',
        0x0040 => 'Critical',
        0x0080 => 'Clock',
        0x0100 => 'Peak',
        0x0200 => 'Voltage',
        0x0400 => 'Current',
    ];

    $res = $db->query("
        SELECT cs.RMFlags, cs.DISABLED, cs.LastRun
        FROM COMSchedule cs
        JOIN Meters m ON m.id = cs.mid
        WHERE (m.DELETED = 0 OR m.DELETED IS NULL)
    ");

    $flags_count = [];
    $today = date('Y-m-d');
    $completed_today = 0;
    $errors_today = 0;

    while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
        $flags = intval($row['RMFlags']);
        $disabled = intval($row['DISABLED']);
        $data['total']++;

        if ($disabled) {
            $data['disabled']++;
        } elseif ($flags > 0) {
            $data['pending']++;
            foreach ($flag_names as $bit => $name) {
                if ($flags & $bit) {
                    if (!isset($flags_count[$name])) $flags_count[$name] = 0;
                    $flags_count[$name]++;
                }
            }
        } else {
            $data['idle']++;
            // Check if completed today
            if ($row['LastRun'] && strpos($row['LastRun'], $today) === 0) {
                $completed_today++;
            }
        }
    }
    $res->finalize();

    $data['flags_breakdown'] = $flags_count;
    $data['completed_today'] = $completed_today;

    return $data;
}

// ============================================================
// Meter summary
// ============================================================

function cmd_meter_summary($db) {
    $data = ['total' => 0, 'mtx1' => 0, 'mtx3' => 0, 'unknown' => 0,
             'online_24h' => 0, 'offline_24h' => 0, 'offline_7d' => 0, 'never_responded' => 0];

    $now = date('Y-m-d H:i:s');
    $t24h = date('Y-m-d H:i:s', time() - 86400);
    $t7d = date('Y-m-d H:i:s', time() - 7 * 86400);

    $res = $db->query("
        SELECT m.id, m.mSerialNumber, m.mType,
               (SELECT MAX(fecha) FROM indicaciones_mtx1 WHERE mid = m.id) as last_mtx1,
               (SELECT MAX(fecha) FROM indicaciones_mtx3 WHERE mid = m.id) as last_mtx3
        FROM Meters m
        WHERE (m.DELETED = 0 OR m.DELETED IS NULL)
    ");

    while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
        $data['total']++;
        $tipo = get_device_tipo($row['mSerialNumber']);

        if (is_mtx3($tipo)) {
            $data['mtx3']++;
            $last = $row['last_mtx3'];
        } elseif (is_mtx1($tipo)) {
            $data['mtx1']++;
            $last = $row['last_mtx1'];
        } else {
            $data['unknown']++;
            $last = $row['last_mtx1'] ?: $row['last_mtx3'];
        }

        if (!$last) {
            $data['never_responded']++;
        } elseif ($last >= $t24h) {
            $data['online_24h']++;
        } elseif ($last < $t7d) {
            $data['offline_7d']++;
        } else {
            $data['offline_24h']++;
        }
    }
    $res->finalize();

    $data['online_percent'] = ($data['total'] > 0) ? round(($data['online_24h'] / $data['total']) * 100, 1) : 0;

    return $data;
}

// ============================================================
// Collection progress (today)
// ============================================================

function cmd_collection($db) {
    $today = date('Y-m-d');
    $data = [];

    // Total active meters
    $total = intval($db->querySingle("SELECT COUNT(*) FROM Meters WHERE (DELETED = 0 OR DELETED IS NULL)"));
    $data['total_meters'] = $total;

    // MR — meter readings (indicaciones) collected today
    $mr_mtx1 = intval(@$db->querySingle("SELECT COUNT(DISTINCT mid) FROM indicaciones_mtx1 WHERE DATE(fecha) = '$today'"));
    $mr_mtx3 = intval(@$db->querySingle("SELECT COUNT(DISTINCT mid) FROM indicaciones_mtx3 WHERE DATE(fecha) = '$today'"));
    $data['mr'] = ['collected' => $mr_mtx1 + $mr_mtx3, 'total' => $total,
                    'percent' => $total > 0 ? round((($mr_mtx1 + $mr_mtx3) / $total) * 100, 1) : 0];

    // LP — load profile (archivo_mhora) collected today
    // Tables may not exist — use unified archivo_mhora or per-type tables
    $lp_mtx1 = 0;
    $lp_mtx3 = 0;
    // Try unified table first (actual RT4 schema), then per-type
    $lp_table = @$db->querySingle("SELECT name FROM sqlite_master WHERE type='table' AND name='archivo_mhora'");
    if ($lp_table) {
        $lp_total_count = intval(@$db->querySingle("SELECT COUNT(DISTINCT id_aparato) FROM archivo_mhora WHERE DATE(fecha) = '$today'"));
        $lp_mtx1 = $lp_total_count;
    } else {
        if (@$db->querySingle("SELECT name FROM sqlite_master WHERE type='table' AND name='archivo_mhora_mtx1'"))
            $lp_mtx1 = intval(@$db->querySingle("SELECT COUNT(DISTINCT mid) FROM archivo_mhora_mtx1 WHERE DATE(fecha) = '$today'"));
        if (@$db->querySingle("SELECT name FROM sqlite_master WHERE type='table' AND name='archivo_mhora_mtx3'"))
            $lp_mtx3 = intval(@$db->querySingle("SELECT COUNT(DISTINCT mid) FROM archivo_mhora_mtx3 WHERE DATE(fecha) = '$today'"));
    }
    $data['lp'] = ['collected' => $lp_mtx1 + $lp_mtx3, 'total' => $total,
                    'percent' => $total > 0 ? round((($lp_mtx1 + $lp_mtx3) / $total) * 100, 1) : 0];

    // Ms — measurements (mediciones) collected today
    $ms_mtx1 = intval(@$db->querySingle("SELECT COUNT(DISTINCT mid) FROM mediciones_mtx1 WHERE DATE(fecha) = '$today'"));
    $ms_mtx3 = intval(@$db->querySingle("SELECT COUNT(DISTINCT mid) FROM mediciones_mtx3 WHERE DATE(fecha) = '$today'"));
    $data['ms'] = ['collected' => $ms_mtx1 + $ms_mtx3, 'total' => $total,
                    'percent' => $total > 0 ? round((($ms_mtx1 + $ms_mtx3) / $total) * 100, 1) : 0];

    // Last collection timestamp
    $last_mr = @$db->querySingle("SELECT MAX(fecha) FROM indicaciones_mtx1 WHERE DATE(fecha) = '$today'");
    $last_mr3 = @$db->querySingle("SELECT MAX(fecha) FROM indicaciones_mtx3 WHERE DATE(fecha) = '$today'");
    $data['last_collection'] = ($last_mr && $last_mr3) ? (($last_mr > $last_mr3) ? $last_mr : $last_mr3) : ($last_mr ?: $last_mr3);

    return $data;
}

// ============================================================
// Main router
// ============================================================

$cmd = isset($_GET['cmd']) ? $_GET['cmd'] : 'all';

// Open DB (read-only)
$db = null;
if (in_array($cmd, ['db_health', 'queue_stats', 'meter_summary', 'collection', 'all'])) {
    if (!is_file(DB_PATH)) {
        err_exit("Database not found: " . DB_PATH);
    }
    $db = new SQLite3(DB_PATH, SQLITE3_OPEN_READONLY);
    $db->busyTimeout(2000);
    @$db->exec('PRAGMA journal_mode=WAL');
    @$db->exec('PRAGMA read_uncommitted=ON');
}

switch ($cmd) {
    case 'system':
        echo json_encode(cmd_system(), JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
        break;

    case 'db_health':
        echo json_encode(cmd_db_health($db), JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
        break;

    case 'queue_stats':
        echo json_encode(cmd_queue_stats($db), JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
        break;

    case 'meter_summary':
        echo json_encode(cmd_meter_summary($db), JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
        break;

    case 'collection':
        echo json_encode(cmd_collection($db), JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
        break;

    case 'all':
        echo json_encode([
            'system'        => cmd_system(),
            'db_health'     => cmd_db_health($db),
            'queue_stats'   => cmd_queue_stats($db),
            'meter_summary' => cmd_meter_summary($db),
            'collection'    => cmd_collection($db),
            'timestamp'     => date('Y-m-d H:i:s'),
        ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
        break;

    default:
        err_exit("Unknown cmd: $cmd. Use: system, db_health, queue_stats, meter_summary, collection, all");
}

if ($db) $db->close();
