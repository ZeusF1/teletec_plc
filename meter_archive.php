<?php
/*
 * meter_archive.php — Direct PLC archive read (bypasses PollReader)
 *
 * Reads LP (load profile) and MR (daily readings) directly from a meter
 * via PLC communication, without waiting for PollReader queue.
 *
 * Usage:
 *   GET meter_archive.php?serial=001A790D12263E6E&type=lp&date=2026-03-10
 *   GET meter_archive.php?serial=001A790D12263E6E&type=mr&date=2026-03-10
 *   GET meter_archive.php?serial=001A790D12263E6E&type=energy
 *   GET meter_archive.php?serial=001A790D12263E6E&type=lp&date_from=2026-03-01&date_to=2026-03-14
 *
 * Parameters:
 *   serial    - 16-char hex meter serial number
 *   type      - lp (30-min profile), mr (daily demand), energy (cumulative)
 *   date      - single date YYYY-MM-DD (for LP: one day = 48 intervals)
 *   date_from - range start (reads multiple days sequentially)
 *   date_to   - range end
 *   psw_level - password level (default 3)
 *   save      - 1 to save results to DB (default 0, read-only)
 *
 * Returns JSON with parsed archive data.
 * Response time: 1-3 minutes per day of LP data.
 */

error_reporting(E_ALL);
ini_set('display_errors', 0);

define('ARCHIVE_LOG', '/tmp/meter_archive.log');
function dbglog($msg) {
    @file_put_contents(ARCHIVE_LOG, date('H:i:s') . ' ' . $msg . "\n", FILE_APPEND);
}
dbglog('=== START serial=' . (isset($_GET['serial']) ? $_GET['serial'] : '?') . ' type=' . (isset($_GET['type']) ? $_GET['type'] : '?'));

ob_start();
set_time_limit(600); // 10 minutes max (deep archive reads)

$_shutdown_json_sent = false;
register_shutdown_function(function() {
    global $_shutdown_json_sent;
    if ($_shutdown_json_sent) return;
    $err = error_get_last();
    while (ob_get_level() > 0) ob_get_clean();
    header("Content-Type: application/json; charset=utf-8");
    $data = ['error' => 'Script terminated unexpectedly'];
    if ($err) $data['fatal'] = $err['message'] . ' (' . $err['file'] . ':' . $err['line'] . ')';
    echo json_encode($data, JSON_UNESCAPED_UNICODE) . "\n";
});

function json_exit($data) {
    global $_shutdown_json_sent;
    $_shutdown_json_sent = true;
    while (ob_get_level() > 0) ob_get_clean();
    header("Content-Type: application/json; charset=utf-8");
    echo json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
    exit;
}

// --- Concurrency guard (same as meter_instant.php) ---
define('ARCHIVE_LOCK', '/tmp/meter_archive.lock');
$_lock_fp = fopen(ARCHIVE_LOCK, 'c');
if (!$_lock_fp || !flock($_lock_fp, LOCK_EX | LOCK_NB)) {
    if ($_lock_fp) fclose($_lock_fp);
    json_exit(['error' => 'busy', 'message' => 'Інше зчитування архіву вже виконується. Зачекайте завершення.']);
}
register_shutdown_function(function() {
    global $_lock_fp;
    if ($_lock_fp) { flock($_lock_fp, LOCK_UN); fclose($_lock_fp); }
});

// --- Log viewer ---
if (isset($_GET['log'])) {
    $_shutdown_json_sent = true;
    while (ob_get_level() > 0) ob_get_clean();
    header("Content-Type: text/plain; charset=utf-8");
    if (is_file(ARCHIVE_LOG)) {
        $lines = file(ARCHIVE_LOG);
        echo implode('', array_slice($lines, -100));
    } else {
        echo "(no log file)";
    }
    exit;
}

// --- Parameters ---
$serial    = isset($_GET['serial']) ? trim($_GET['serial']) : '';
$type      = isset($_GET['type'])   ? strtolower(trim($_GET['type'])) : 'lp';
$date      = isset($_GET['date'])   ? trim($_GET['date']) : '';
$date_from = isset($_GET['date_from']) ? trim($_GET['date_from']) : '';
$date_to   = isset($_GET['date_to'])   ? trim($_GET['date_to'])   : '';
$psw_level = isset($_GET['psw_level']) ? intval($_GET['psw_level']) : 3;
$save_to_db = isset($_GET['save']) ? intval($_GET['save']) : 0;
// Energy direction: a+ (default), a-, q+, q- — selects which PLC command to use
$energy_dir = isset($_GET['dir']) ? strtolower(trim($_GET['dir'])) : 'a+';
if (!in_array($energy_dir, ['a+', 'a-', 'q+', 'q-'])) $energy_dir = 'a+';

if (!$serial || strlen($serial) != 16) {
    json_exit(['error' => 'Missing or invalid serial parameter (16 hex chars)']);
}

if (!in_array($type, ['lp', 'mr', 'energy', 'monthly'])) {
    json_exit(['error' => 'Invalid type. Use: lp, mr, energy, monthly']);
}

// Build date list
$dates = [];
if ($date) {
    $ts = strtotime($date);
    if ($ts === false) json_exit(['error' => 'Invalid date format. Use YYYY-MM-DD']);
    $dates[] = $ts;
} elseif ($date_from && $date_to) {
    $ts_from = strtotime($date_from);
    $ts_to   = strtotime($date_to);
    if ($ts_from === false || $ts_to === false) json_exit(['error' => 'Invalid date range']);
    if ($ts_to < $ts_from) { $tmp = $ts_from; $ts_from = $ts_to; $ts_to = $tmp; }
    // Max days depend on type: LP is slow (~20s/day), MR is fast (~5s/day)
    $max_days = ($type === 'lp') ? 7 : 30;
    $day_count = intval(($ts_to - $ts_from) / 86400) + 1;
    if ($day_count > $max_days) json_exit(['error' => "Range too large ($day_count days). Max $max_days days for $type.", 'max_days' => $max_days]);
    for ($ts = $ts_from; $ts <= $ts_to; $ts += 86400) {
        $dates[] = $ts;
    }
} elseif ($type === 'energy' || $type === 'monthly') {
    // Energy/monthly don't need dates
    $dates = [time()];
} else {
    // Default: yesterday
    $dates[] = strtotime('yesterday');
}

// --- Include RT4 infrastructure ---
$init_error = null;
try {
    require_once("../Lib/d_base.php");
    require_once("Include/app_config.inc");
    require_once("tables.php");

    $config = new App_Config();
    try { date_default_timezone_set($config->time_zone); } catch(Exception $e) {}

    $db = new DataBase(null, null, null, null, DATABASE_PATH, $db_dsr, $db_funciones, DB_SQLITE, $config->logs & DEBUG_SQL_QUERY, $config->get_tareas_dir());

    require_once("Include/aparatos.inc");
    $aparatos = new Aparatos();
} catch (Exception $ex) {
    $init_error = $ex->getMessage();
}

if ($init_error) {
    json_exit(['error' => 'Init failed: ' . $init_error]);
}

$init_garbage = ob_get_clean();
dbglog('init done, garbage_len=' . strlen($init_garbage));

// --- Determine meter type ---
$tipo = 0;
$serial_upper = strtoupper($serial);
if (strlen($serial_upper) == 16 && substr($serial_upper, 0, 6) == '001A79') {
    $tipo = hexdec(substr($serial_upper, 6, 2));
}
$is_mtx1    = in_array($tipo, [0x01, 0x17, 0x0B, 0x15]);
$is_mtx3    = in_array($tipo, [0x02, 0x03, 0x0C, 0x0D, 0x16]);
$is_mtx3_4k = in_array($tipo, [0x0C, 0x0D, 0x16]);

// Communication parameters
$frame_type = ($psw_level > 0) ? 0x50 : 0x5C;
$cifrar     = ($psw_level > 0);
$psw = null;
if ($psw_level > 0) {
    switch ($psw_level) {
        case 1: $psw = isset($config->psw1) ? $config->psw1 : null; break;
        case 2: $psw = isset($config->psw2) ? $config->psw2 : null; break;
        case 3: $psw = isset($config->psw3) ? $config->psw3 : null; break;
    }
}

// =================================================================
// Build PLC commands based on type
// =================================================================

/*
 * PLC Command mapping by energy direction:
 *   LP (30-min profile):   A+ = CMD 21,  A- = CMD 83,  Q+ = CMD 72,  Q- = CMD 73
 *   MR (daily demand):     A+ = CMD 22,  A- = CMD 79
 *   Energy (cumulative):   A+ = CMD 15,  A- = CMD 91
 *   Monthly:               A+ = CMD 23,  A- = CMD 82
 *
 * DB pType for archivo_mhora (LP):  A+=21, A-=22, Q+=23, Q-=24
 * DB pType for indicaciones (MR):   A+=1,  A-=2
 */

/** Build LP command for a specific date */
function build_lp_cmd($ts, $dir = 'a+') {
    $y = intval(date('Y', $ts)) - 2000;
    $m = intval(date('m', $ts));
    $d = intval(date('d', $ts));
    $cmds = ['a+' => 21, 'a-' => 83, 'q+' => 72, 'q-' => 73];
    $cmd = isset($cmds[$dir]) ? $cmds[$dir] : 21;
    return chr($cmd) . chr(3) . chr($y) . chr($m) . chr($d);
}

/** Build MR (daily demand) command for a specific date */
function build_mr_cmd($ts, $dir = 'a+') {
    $y = intval(date('Y', $ts)) - 2000;
    $m = intval(date('m', $ts));
    $d = intval(date('d', $ts));
    $cmd = ($dir === 'a-') ? 79 : 22;  // Only A+ and A- for MR
    return chr($cmd) . chr(3) . chr($y) . chr($m) . chr($d);
}

/** Build energy command (no date needed) */
function build_energy_cmd($dir = 'a+') {
    $cmd = ($dir === 'a-') ? 91 : 15;
    return chr($cmd) . chr(0);
}

/** Build monthly demand command */
function build_monthly_cmd($ts, $dir = 'a+') {
    $y = intval(date('Y', $ts)) - 2000;
    $m = intval(date('m', $ts));
    $cmd = ($dir === 'a-') ? 82 : 23;
    return chr($cmd) . chr(2) . chr($y) . chr($m);
}

// =================================================================
// Parse binary responses
// =================================================================

/** Parse CMD 21 response — LP (48 half-hour intervals for one day) */
function parse_lp_response($data, $len, $is_mtx1) {
    if ($len < 99) return ['error' => 'LP response too short: ' . $len . ' bytes'];

    $offset = 0;
    $year  = 2000 + hexdec(bin2hex(substr($data, $offset++, 1)));
    $month = hexdec(bin2hex(substr($data, $offset++, 1)));
    $day   = hexdec(bin2hex(substr($data, $offset++, 1)));
    $date_str = sprintf('%04d-%02d-%02d', $year, $month, $day);

    $intervals = [];
    for ($t = 0; $t < 48; $t++) {
        $value = hexdec(bin2hex(substr($data, $offset + ($t * 2), 2)));
        // MTX1: 0x3FFE means "no data"
        if ($is_mtx1 && (($value & 0x3FFF) == 0x3FFE)) {
            $value = null;
        }
        $hh = intval($t / 2);
        $mm = ($t % 2) * 30;
        $intervals[] = [
            'time'  => sprintf('%02d:%02d', $hh, $mm),
            'value' => $value,
            'kWh'   => ($value !== null) ? round($value * 0.001, 3) : null,
        ];
    }

    return [
        'date'      => $date_str,
        'intervals' => $intervals,
        'count'     => 48,
    ];
}

/** Parse CMD 22 response — MR (daily energy by tariff) */
function parse_mr_response($data, $len, $is_mtx1, $is_mtx3) {
    if ($is_mtx1 && $len < 19) return ['error' => 'MR response too short for MTX1: ' . $len];
    if ($is_mtx3 && $len < 51) return ['error' => 'MR response too short for MTX3: ' . $len];

    $year  = 2000 + hexdec(bin2hex(substr($data, 0, 1)));
    $month = hexdec(bin2hex(substr($data, 1, 1)));
    $day   = hexdec(bin2hex(substr($data, 2, 1)));
    $date_str = sprintf('%04d-%02d-%02d', $year, $month, $day);

    $tariffs = [];
    if ($is_mtx1) {
        // 4 tariffs × 4 bytes
        for ($i = 0; $i < 4; $i++) {
            $raw = hexdec(bin2hex(substr($data, 3 + ($i * 4), 4)));
            $tariffs['T' . ($i + 1)] = round($raw * 0.001, 3);
        }
    } elseif ($is_mtx3) {
        // 4 tariffs × 12 bytes (A+, VARi, VARe)
        for ($t = 0; $t < 4; $t++) {
            $a_plus = hexdec(bin2hex(substr($data, 3 + 12 * $t + 0, 4)));
            $var_i  = hexdec(bin2hex(substr($data, 3 + 12 * $t + 4, 4)));
            $var_e  = hexdec(bin2hex(substr($data, 3 + 12 * $t + 8, 4)));
            $tariffs['T' . ($t + 1)] = [
                'A_plus' => round($a_plus * 0.001, 3),
                'VARi'   => round($var_i * 0.001, 3),
                'VARe'   => round($var_e * 0.001, 3),
            ];
        }
    }

    return [
        'date'    => $date_str,
        'tariffs' => $tariffs,
    ];
}

/** Parse CMD 15 response — cumulative energy */
function parse_energy_response($data, $len, $is_mtx1) {
    if ($len < 16) return ['error' => 'Energy response too short: ' . $len];

    $result = [];
    // 4 tariffs × 4 bytes
    for ($i = 0; $i < 4; $i++) {
        $raw = hexdec(bin2hex(substr($data, $i * 4, 4)));
        $result['T' . ($i + 1)] = round($raw * 0.01, 2);
    }
    $result['total'] = round($result['T1'] + $result['T2'] + $result['T3'] + $result['T4'], 2);
    return $result;
}

/** Parse CMD 23 response — monthly demand */
function parse_monthly_response($data, $len, $is_mtx1, $is_mtx3) {
    if ($is_mtx1 && $len < 18) return ['error' => 'Monthly too short for MTX1: ' . $len];
    if ($is_mtx3 && $len < 50) return ['error' => 'Monthly too short for MTX3: ' . $len];

    $year  = 2000 + hexdec(bin2hex(substr($data, 0, 1)));
    $month = hexdec(bin2hex(substr($data, 1, 1)));

    $tariffs = [];
    if ($is_mtx1) {
        for ($i = 0; $i < 4; $i++) {
            $raw = hexdec(bin2hex(substr($data, 2 + ($i * 4), 4)));
            $tariffs['T' . ($i + 1)] = round($raw * 0.001, 3);
        }
    } elseif ($is_mtx3) {
        for ($t = 0; $t < 4; $t++) {
            $a_plus = hexdec(bin2hex(substr($data, 2 + 12 * $t + 0, 4)));
            $var_i  = hexdec(bin2hex(substr($data, 2 + 12 * $t + 4, 4)));
            $var_e  = hexdec(bin2hex(substr($data, 2 + 12 * $t + 8, 4)));
            $tariffs['T' . ($t + 1)] = [
                'A_plus' => round($a_plus * 0.001, 3),
                'VARi'   => round($var_i * 0.001, 3),
                'VARe'   => round($var_e * 0.001, 3),
            ];
        }
    }

    return [
        'month'   => sprintf('%04d-%02d', $year, $month),
        'tariffs' => $tariffs,
    ];
}

// =================================================================
// Send PLC commands and parse responses
// =================================================================

/** Send commands and parse all responses */
function do_archive_read($aparatos, $serial, $cmd_list, $psw_level, $psw, $cifrar, $frame_type) {
    $t0 = microtime(true);
    dbglog("sending " . count($cmd_list) . " commands...");

    ob_start();
    $rsp_list = null;
    try {
        $rsp_list = $aparatos->transmitPerRouterRT2(
            $serial, $cmd_list, $psw_level, $psw, $cifrar, false, $frame_type
        );
    } catch (Exception $ex) {
        ob_get_clean();
        return ['error' => 'PLC error: ' . $ex->getMessage(), 'elapsed' => round(microtime(true) - $t0, 1)];
    }
    $echo_out = ob_get_clean();
    $elapsed = round(microtime(true) - $t0, 1);
    dbglog("transmit done, count=" . (is_array($rsp_list) ? count($rsp_list) : 'N/A') . ", elapsed={$elapsed}s");

    if (!$rsp_list || !is_array($rsp_list) || count($rsp_list) == 0) {
        return ['error' => 'Немає відповіді від лічильника (таймаут)', 'elapsed' => $elapsed];
    }

    // Parse all response PDUs
    $parsed = [];
    foreach ($rsp_list as $rsp) {
        if (strlen($rsp) < 3) continue;
        if (ord($rsp[0]) == 0xFE) continue; // FRAME_ERROR

        $data = substr($rsp, 1);
        $dlen = strlen($data);
        $pos = 0;
        while ($pos < $dlen) {
            $cmd = ord($data[$pos]);
            if ($cmd == 0) break;
            $pos++;
            if ($pos >= $dlen) break;
            $cmd_data_len = ord($data[$pos]);
            $pos++;
            if ($pos + $cmd_data_len > $dlen) break;
            $cmd_data = substr($data, $pos, $cmd_data_len);
            $pos += $cmd_data_len;

            $parsed[] = ['cmd' => $cmd, 'data' => $cmd_data, 'len' => $cmd_data_len];
        }
    }

    return ['parsed' => $parsed, 'elapsed' => $elapsed, 'plc_output' => trim($echo_out) ?: null];
}

// =================================================================
// Main execution
// =================================================================

$t_start = microtime(true);
$results = [];
$errors = [];

dbglog("dir=$energy_dir");

if ($type === 'energy') {
    // Single command, no date iteration
    $cmd_list = [build_energy_cmd($energy_dir)];
    $raw = do_archive_read($aparatos, $serial, $cmd_list, $psw_level, $psw, $cifrar, $frame_type);

    if (isset($raw['error'])) {
        json_exit(['error' => $raw['error'], 'elapsed' => $raw['elapsed']]);
    }

    foreach ($raw['parsed'] as $p) {
        if ($p['cmd'] == 15 || $p['cmd'] == 91) {
            $results['energy'] = parse_energy_response($p['data'], $p['len'], $is_mtx1);
        }
    }
} elseif ($type === 'monthly') {
    // One command per date
    foreach ($dates as $ts) {
        $cmd_list = [build_monthly_cmd($ts, $energy_dir)];
        $raw = do_archive_read($aparatos, $serial, $cmd_list, $psw_level, $psw, $cifrar, $frame_type);

        if (isset($raw['error'])) {
            $errors[] = ['date' => date('Y-m', $ts), 'error' => $raw['error']];
            continue;
        }

        foreach ($raw['parsed'] as $p) {
            if ($p['cmd'] == 23 || $p['cmd'] == 82) {
                $results[] = parse_monthly_response($p['data'], $p['len'], $is_mtx1, $is_mtx3);
            }
        }
    }
} else {
    // LP or MR — one command per date (PLC router drops responses in batch)
    foreach ($dates as $ts) {
        $cmd_list = [($type === 'lp') ? build_lp_cmd($ts, $energy_dir) : build_mr_cmd($ts, $energy_dir)];

        dbglog("cmd for " . date('Y-m-d', $ts) . " dir=$energy_dir");

        $raw = do_archive_read($aparatos, $serial, $cmd_list, $psw_level, $psw, $cifrar, $frame_type);

        if (isset($raw['error'])) {
            $errors[] = ['date' => date('Y-m-d', $ts), 'error' => $raw['error']];
            continue;
        }

        foreach ($raw['parsed'] as $p) {
            if (in_array($p['cmd'], [21, 83, 72, 73, 84, 85])) {
                $results[] = parse_lp_response($p['data'], $p['len'], $is_mtx1);
            } elseif (in_array($p['cmd'], [22, 79])) {
                $results[] = parse_mr_response($p['data'], $p['len'], $is_mtx1, $is_mtx3);
            }
        }
    }
}

$total_elapsed = round(microtime(true) - $t_start, 1);
dbglog("done: " . count($results) . " results, " . count($errors) . " errors, elapsed={$total_elapsed}s");

// --- Save to DB if requested ---
$saved_count = 0;
if ($save_to_db && count($results) > 0) {
    $saved_count = save_archive_to_db($serial_upper, $type, $results, $is_mtx1, $is_mtx3, $energy_dir);
    dbglog("saved $saved_count records to DB");
}

// --- Output ---
$output = [
    'ok'      => count($results) > 0,
    'serial'  => strtoupper($serial),
    'type'    => $type,
    'dir'     => $energy_dir,
    'meterType' => $is_mtx3 ? ($is_mtx3_4k ? 'MTX3_4K' : 'MTX3') : ($is_mtx1 ? 'MTX1' : 'unknown'),
    'elapsed' => $total_elapsed,
    'count'   => count($results),
    'data'    => $results,
];
if ($save_to_db) $output['saved'] = $saved_count;
if (!empty($errors)) $output['errors'] = $errors;

json_exit($output);


// =================================================================
// Save archive data to database
// =================================================================

function save_archive_to_db($serial, $type, $results, $is_mtx1, $is_mtx3, $energy_dir = 'a+') {
    $db_path = defined('DATABASE_PATH') ? DATABASE_PATH : '/usr/local/rt4/mtxdb.sdb';

    try {
        $db = new SQLite3($db_path, SQLITE3_OPEN_READWRITE);
        $db->busyTimeout(10000);
        @$db->exec("PRAGMA journal_mode=WAL");
    } catch (Exception $e) {
        dbglog("DB open error: " . $e->getMessage());
        return 0;
    }

    // Find meter id by serial
    $serial_up = strtoupper($serial);
    dbglog("save_archive_to_db: serial=$serial_up type=$type results=" . count($results) . " mtx1=$is_mtx1 mtx3=$is_mtx3");

    $stmt = $db->prepare("SELECT id, mAddress FROM Meters WHERE UPPER(mSerialNumber) = :serial AND (DELETED = 0 OR DELETED IS NULL)");
    $stmt->bindValue(':serial', $serial_up, SQLITE3_TEXT);
    $res = $stmt->execute();
    $meter = $res->fetchArray(SQLITE3_ASSOC);
    $res->finalize();

    if (!$meter) {
        dbglog("Meter not found by serial: $serial_up");
        $db->close();
        return 0;
    }

    $mid = intval($meter['id']);
    dbglog("Found meter: id=$mid addr=" . $meter['mAddress']);
    $saved = 0;

    if ($type === 'lp') {
        $saved = save_lp_data($db, $mid, $results, $is_mtx3, $energy_dir);
    } elseif ($type === 'mr') {
        $saved = save_mr_data($db, $mid, $results, $is_mtx1, $is_mtx3, $energy_dir);
    }

    $db->close();
    return $saved;
}

/** Save LP (30-min profile) data to archivo_mhora
 *  Table schema: mid, fecha, pType, Wh_t0..Wh_t47
 *  pType: 21=A+, 22=A-, 23=Q+, 24=Q-
 */
function save_lp_data($db, $mid, $results, $is_mtx3, $energy_dir = 'a+') {
    $saved = 0;
    // pType in archivo_mhora: 21=A+, 22=A-, 23=Q+, 24=Q-
    $ptype_map = ['a+' => 21, 'a-' => 22, 'q+' => 23, 'q-' => 24];
    $pType = isset($ptype_map[$energy_dir]) ? $ptype_map[$energy_dir] : 21;

    // Build column list: Wh_t0 .. Wh_t47
    $wh_cols = [];
    $wh_placeholders = [];
    for ($i = 0; $i < 48; $i++) {
        $wh_cols[] = "Wh_t$i";
        $wh_placeholders[] = ":wh_t$i";
    }

    $sql = "INSERT OR REPLACE INTO archivo_mhora (mid, fecha, pType, " .
           implode(', ', $wh_cols) . ") VALUES (:mid, :fecha, :pType, " .
           implode(', ', $wh_placeholders) . ")";

    $stmt = $db->prepare($sql);
    if (!$stmt) {
        dbglog("LP prepare failed: " . $db->lastErrorMsg());
        return 0;
    }

    $db->exec("BEGIN TRANSACTION");

    foreach ($results as $day) {
        if (isset($day['error'])) continue;
        if (!isset($day['intervals']) || count($day['intervals']) < 48) continue;

        $stmt->bindValue(':mid', $mid, SQLITE3_INTEGER);
        $stmt->bindValue(':fecha', $day['date'] . ' 00:00:00', SQLITE3_TEXT);
        $stmt->bindValue(':pType', $pType, SQLITE3_INTEGER);

        for ($i = 0; $i < 48; $i++) {
            $val = $day['intervals'][$i]['value'];
            $stmt->bindValue(":wh_t$i", $val, $val !== null ? SQLITE3_INTEGER : SQLITE3_NULL);
        }

        if (@$stmt->execute()) {
            $saved++;
        } else {
            dbglog("LP insert failed for " . $day['date'] . ": " . $db->lastErrorMsg());
        }
        $stmt->reset();
    }

    $db->exec("COMMIT");
    return $saved;
}

/** Save MR (daily readings) data to indicaciones table
 *
 *  indicaciones_mtx3 schema:
 *    mid, fecha, r_fecha, pType,
 *    Wh_t0..Wh_t3       (A+ per tariff, in Wh)
 *    VARi_t0..VARi_t3    (reactive import per tariff)
 *    VARe_t0..VARe_t3    (reactive export per tariff)
 *    UNIQUE(mid, fecha, pType)
 *
 *  indicaciones_mtx1 schema:
 *    mid, fecha, r_fecha, pType, Wh_t0..Wh_t3
 *    UNIQUE(mid, fecha, pType)
 *
 *  pType=1 for A+ (OBIS 1.8.0). Values in Wh (kWh × 1000).
 */
function save_mr_data($db, $mid, $results, $is_mtx1, $is_mtx3, $energy_dir = 'a+') {
    $saved = 0;
    $table = $is_mtx3 ? 'indicaciones_mtx3' : 'indicaciones_mtx1';

    // Check table exists
    $exists = @$db->querySingle("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='$table'");
    if (!$exists) {
        dbglog("MR table $table does not exist");
        return 0;
    }

    $now = date('Y-m-d H:i:s');
    $db->exec("BEGIN TRANSACTION");

    foreach ($results as $day) {
        if (isset($day['error'])) continue;
        if (!isset($day['tariffs'])) continue;

        $fecha = $day['date'] . ' 00:00:00';
        $t = $day['tariffs'];

        if ($is_mtx3) {
            // MTX3: one row per date with Wh_t0-3, VARi_t0-3, VARe_t0-3
            $stmt = $db->prepare("INSERT OR REPLACE INTO $table (mid, fecha, r_fecha, pType, Wh_t0, Wh_t1, Wh_t2, Wh_t3, VARi_t0, VARi_t1, VARi_t2, VARi_t3, VARe_t0, VARe_t1, VARe_t2, VARe_t3) VALUES (:mid, :fecha, :r_fecha, :pType, :wh0, :wh1, :wh2, :wh3, :vi0, :vi1, :vi2, :vi3, :ve0, :ve1, :ve2, :ve3)");
            if (!$stmt) { dbglog("MTX3 MR prepare failed: " . $db->lastErrorMsg()); continue; }

            $stmt->bindValue(':mid', $mid, SQLITE3_INTEGER);
            $stmt->bindValue(':fecha', $fecha, SQLITE3_TEXT);
            $stmt->bindValue(':r_fecha', $now, SQLITE3_TEXT);
            $mr_ptype = ($energy_dir === 'a-') ? 2 : 1;
            $stmt->bindValue(':pType', $mr_ptype, SQLITE3_INTEGER);

            for ($i = 0; $i < 4; $i++) {
                $key = 'T' . ($i + 1);
                $wh  = isset($t[$key]['A_plus']) ? round($t[$key]['A_plus'] * 1000) : 0;
                $vi  = isset($t[$key]['VARi'])   ? round($t[$key]['VARi']   * 1000) : 0;
                $ve  = isset($t[$key]['VARe'])   ? round($t[$key]['VARe']   * 1000) : 0;
                $stmt->bindValue(":wh$i", $wh, SQLITE3_INTEGER);
                $stmt->bindValue(":vi$i", $vi, SQLITE3_INTEGER);
                $stmt->bindValue(":ve$i", $ve, SQLITE3_INTEGER);
            }

            if (@$stmt->execute()) {
                $saved++;
                dbglog("MR MTX3 saved " . $day['date'] . ": Wh=[" . round($t['T1']['A_plus']*1000) . "," . round($t['T2']['A_plus']*1000) . "," . round($t['T3']['A_plus']*1000) . "]");
            } else {
                dbglog("MR MTX3 insert failed " . $day['date'] . ": " . $db->lastErrorMsg());
            }
            $stmt->reset();
        } else {
            // MTX1: Wh_t0..Wh_t3 only
            $stmt = $db->prepare("INSERT OR REPLACE INTO $table (mid, fecha, r_fecha, pType, Wh_t0, Wh_t1, Wh_t2, Wh_t3) VALUES (:mid, :fecha, :r_fecha, :pType, :wh0, :wh1, :wh2, :wh3)");
            if (!$stmt) { dbglog("MTX1 MR prepare failed: " . $db->lastErrorMsg()); continue; }

            $stmt->bindValue(':mid', $mid, SQLITE3_INTEGER);
            $stmt->bindValue(':fecha', $fecha, SQLITE3_TEXT);
            $stmt->bindValue(':r_fecha', $now, SQLITE3_TEXT);
            $mr_ptype = ($energy_dir === 'a-') ? 2 : 1;
            $stmt->bindValue(':pType', $mr_ptype, SQLITE3_INTEGER);

            for ($i = 0; $i < 4; $i++) {
                $key = 'T' . ($i + 1);
                $val = isset($t[$key]) ? round($t[$key] * 1000) : 0;
                $stmt->bindValue(":wh$i", $val, SQLITE3_INTEGER);
            }

            if (@$stmt->execute()) {
                $saved++;
            } else {
                dbglog("MR MTX1 insert failed " . $day['date'] . ": " . $db->lastErrorMsg());
            }
            $stmt->reset();
        }
    }

    $db->exec("COMMIT");
    return $saved;
}
