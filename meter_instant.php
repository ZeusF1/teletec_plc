<?php
/*
 * meter_instant.php — Direct PLC instant meter reading (bypasses PollReader)
 *
 * Sends CMD_GET_CUR_VALUES + CMD_GET_EXTEND_CUR_VALUES directly via PLC socket.
 * Response time: 1-2 minutes (direct DLMS/COSEM communication through PLC modem).
 *
 * Usage:
 *   GET meter_instant.php?serial=001A79170012345678
 *   Optional: psw_level=0 (default, no auth) | 2 | 3
 *
 * Returns JSON with voltage, current, power, frequency, temperature.
 */

error_reporting(E_ALL);
ini_set('display_errors', 0); // don't pollute JSON output

// Debug log file (writable location on RT4)
define('INSTANT_LOG', '/tmp/meter_instant.log');
function dbglog($msg) {
    @file_put_contents(INSTANT_LOG, date('H:i:s') . ' ' . $msg . "\n", FILE_APPEND);
}
dbglog('=== START serial=' . (isset($_GET['serial']) ? $_GET['serial'] : '?'));

// Capture ALL output from the very start (aparatos includes may echo things)
ob_start();

set_time_limit(300); // 5 minutes max

// Last-resort shutdown handler — catches fatal errors, exit(), die(), etc.
$_shutdown_json_sent = false;
register_shutdown_function(function() {
    global $_shutdown_json_sent, $_php_errors;
    dbglog('shutdown handler, json_sent=' . ($_shutdown_json_sent ? 'Y' : 'N'));
    if ($_shutdown_json_sent) return; // json_out/json_exit already sent response

    $err = error_get_last();
    $data = array('error' => 'Script terminated unexpectedly');
    if ($err) {
        $data['fatal'] = $err['message'] . ' (' . $err['file'] . ':' . $err['line'] . ')';
        dbglog('FATAL: ' . $data['fatal']);
    }
    // Clean any remaining output buffers
    while (ob_get_level() > 0) {
        $garbage = ob_get_clean();
        if ($garbage && !isset($data['buffered_output'])) {
            // Sanitize to ASCII for safe JSON
            $data['buffered_output'] = mb_check_encoding($garbage, 'UTF-8') ? substr($garbage, 0, 500) : '[hex:' . substr(bin2hex($garbage), 0, 200) . ']';
        }
    }
    if (!empty($_php_errors)) $data['php_errors'] = $_php_errors;
    if (!headers_sent()) {
        header("Content-Type: application/json; charset=utf-8");
    }
    echo json_encode($data, JSON_UNESCAPED_UNICODE);
});

// Custom error handler to capture PHP warnings/notices
$_php_errors = array();
set_error_handler(function($errno, $errstr, $errfile, $errline) {
    global $_php_errors;
    $_php_errors[] = "$errstr ($errfile:$errline)";
    dbglog("PHP[$errno] $errstr ($errfile:$errline)");
    return true; // don't execute default error handler
});

// Helper: output JSON and exit (cleans any buffered output first)
function json_exit($data) {
    global $_php_errors, $_shutdown_json_sent;
    $_shutdown_json_sent = true;
    dbglog('json_exit: ' . (isset($data['error']) ? $data['error'] : 'ok'));
    // Clean ALL output buffers (may be nested)
    while (ob_get_level() > 0) {
        $garbage = ob_get_clean();
        if ($garbage && !isset($data['init_output'])) {
            $data['init_output'] = $garbage;
        }
    }
    if (!empty($_php_errors)) {
        $data['php_errors'] = $_php_errors;
    }
    header("Content-Type: application/json; charset=utf-8");
    $data = sanitize_for_json($data);
    $json = json_encode($data, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
    if ($json === false) {
        dbglog('json_encode FAILED: ' . json_last_error_msg());
        $json = json_encode(array('error' => 'JSON encode failed: ' . json_last_error_msg()));
    }
    dbglog('output len=' . strlen($json));
    echo $json;
    exit;
}

// Helper: output JSON and exit (no buffer cleanup needed — already done)
function json_out($data) {
    global $_php_errors, $_shutdown_json_sent;
    $_shutdown_json_sent = true;
    dbglog('json_out: ' . (isset($data['error']) ? $data['error'] : 'ok=' . (isset($data['ok']) ? ($data['ok']?'Y':'N') : '?')));
    // Safety: clean any remaining buffers
    while (ob_get_level() > 0) {
        ob_get_clean();
    }
    if (!empty($_php_errors)) {
        $data['php_errors'] = $_php_errors;
    }
    header("Content-Type: application/json; charset=utf-8");
    // Sanitize all string values to valid UTF-8 (PLC output may contain binary)
    $data = sanitize_for_json($data);
    $json = json_encode($data, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
    if ($json === false) {
        dbglog('json_encode FAILED: ' . json_last_error_msg());
        // Fallback: strip problematic fields and report the encoding error
        $safe = array(
            'error' => isset($data['error']) ? mb_convert_encoding((string)$data['error'], 'UTF-8', 'UTF-8') : 'JSON encode failed',
            'json_error' => json_last_error_msg(),
            'serial' => isset($data['serial']) ? $data['serial'] : '?',
            'elapsed' => isset($data['elapsed']) ? $data['elapsed'] : null,
        );
        if (!empty($_php_errors)) $safe['php_errors'] = $_php_errors;
        $json = json_encode($safe, JSON_UNESCAPED_UNICODE);
    }
    dbglog('output len=' . strlen($json));
    echo $json;
    exit;
}

/** Recursively sanitize values for JSON encoding (binary → hex, invalid UTF-8 → cleaned) */
function sanitize_for_json($v) {
    if (is_array($v)) {
        $out = array();
        foreach ($v as $k => $val) {
            $out[$k] = sanitize_for_json($val);
        }
        return $out;
    }
    if (is_string($v)) {
        // If string has non-UTF-8 bytes, convert to hex representation
        if (!mb_check_encoding($v, 'UTF-8')) {
            return '[hex:' . bin2hex($v) . ']';
        }
        return $v;
    }
    return $v;
}

// --- Debug log reader ---
if (isset($_GET['log'])) {
    while (ob_get_level() > 0) ob_get_clean();
    header("Content-Type: text/plain; charset=utf-8");
    $_shutdown_json_sent = true;
    if (file_exists(INSTANT_LOG)) {
        $lines = file(INSTANT_LOG);
        $n = isset($_GET['n']) ? intval($_GET['n']) : 50;
        echo implode('', array_slice($lines, -$n));
    } else {
        echo "(no log file)";
    }
    exit;
}

// --- Concurrency guard: only 1 instant read at a time ---
// With only 3 php-fpm workers, we MUST prevent instant reads from monopolizing them.
// A lock file ensures at most 1 worker is doing PLC communication.
define('INSTANT_LOCK', '/tmp/meter_instant.lock');
$_lock_fp = fopen(INSTANT_LOCK, 'c');
if (!$_lock_fp || !flock($_lock_fp, LOCK_EX | LOCK_NB)) {
    // Another instant read is already running — respond as SSE if stream mode
    if ($_lock_fp) fclose($_lock_fp);
    $stream_req = isset($_GET['stream']) ? intval($_GET['stream']) : 0;
    if ($stream_req > 0) {
        // Return SSE-formatted error so EventSource.onmessage can read it
        $_shutdown_json_sent = true;
        while (ob_get_level() > 0) ob_get_clean();
        header("Content-Type: text/event-stream; charset=utf-8");
        header("Cache-Control: no-cache");
        echo "data: " . json_encode(array('error' => 'busy', 'message' => 'Інше миттєве зчитування вже виконується')) . "\n\n";
        echo "event: done\ndata: {\"cycles\":0}\n\n";
        exit;
    }
    json_exit(array('error' => 'busy', 'message' => 'Інше миттєве зчитування вже виконується. Зачекайте завершення.'));
}
// Lock acquired — will be released when script ends (fclose or process exit)
register_shutdown_function(function() {
    global $_lock_fp;
    if ($_lock_fp) {
        flock($_lock_fp, LOCK_UN);
        fclose($_lock_fp);
    }
});
dbglog('lock acquired');

// --- Parameters ---
$serial    = isset($_GET['serial'])    ? trim($_GET['serial'])    : (isset($_POST['serial']) ? trim($_POST['serial']) : '');
$psw_level = isset($_GET['psw_level']) ? intval($_GET['psw_level']) : 3;

if (!$serial || strlen($serial) != 16) {
    json_exit(array('error' => 'Missing or invalid serial parameter (16 hex chars)'));
}

// --- Include RT4 aparatos infrastructure ---
// Same includes as aparatos.php transmit_data path
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
    json_exit(array('error' => 'Init failed: ' . $init_error));
}

// --- Build PLC commands ---
$cmd_list = array();
// CMD_GET_CUR_VALUES: cmd=13, param=0 (voltage, current, power, power factor)
$cmd_list[] = chr(13) . chr(0);
// CMD_GET_EXTEND_CUR_VALUES: cmd=58, param=0 (frequency, temperature, power factor for MTX3)
$cmd_list[] = chr(58) . chr(0);

// Communication parameters
$frame_type = ($psw_level > 0) ? 0x50 : 0x5C;
$cifrar     = ($psw_level > 0);

// Get encryption password from config (same as aparatos.php get_password psw_type=1)
$psw = null;
if ($psw_level > 0) {
    switch ($psw_level) {
        case 1: $psw = isset($config->psw1) ? $config->psw1 : null; break;
        case 2: $psw = isset($config->psw2) ? $config->psw2 : null; break;
        case 3: $psw = isset($config->psw3) ? $config->psw3 : null; break;
    }
    dbglog('psw_level=' . $psw_level . ' psw_len=' . ($psw !== null ? strlen($psw) : 'null'));
}

// Flush init output
$init_garbage = ob_get_clean();
dbglog('init done, garbage_len=' . strlen($init_garbage));

// --- Determine meter type from serial number ---
$tipo = 0;
$serial_upper = strtoupper($serial);
if (strlen($serial_upper) == 16 && substr($serial_upper, 0, 6) == '001A79') {
    $tipo = hexdec(substr($serial_upper, 6, 2));
}
$is_mtx1    = in_array($tipo, array(0x01, 0x17, 0x0B, 0x15));
$is_mtx3    = in_array($tipo, array(0x02, 0x03, 0x0C, 0x0D, 0x16));
$is_mtx3_4k = in_array($tipo, array(0x0C, 0x0D, 0x16));

/** Perform one PLC read cycle and return result array */
function do_plc_read($aparatos, $serial, $cmd_list, $psw_level, $psw, $cifrar, $frame_type, $is_mtx1, $is_mtx3, $is_mtx3_4k, $cycle) {
    $t0 = microtime(true);
    dbglog("cycle=$cycle calling transmitPerRouterRT2...");

    ob_start();
    $rsp_list = null;
    try {
        $rsp_list = $aparatos->transmitPerRouterRT2(
            $serial, $cmd_list, $psw_level, $psw, $cifrar, false, $frame_type
        );
        dbglog("cycle=$cycle transmit returned, count=" . (is_array($rsp_list) ? count($rsp_list) : 'N/A'));
    } catch (Exception $ex) {
        dbglog("cycle=$cycle EXCEPTION: " . $ex->getMessage());
        $echo_out = ob_get_clean();
        return array(
            'error'   => 'PLC error: ' . $ex->getMessage(),
            'serial'  => strtoupper($serial),
            'elapsed' => round(microtime(true) - $t0, 1),
            'cycle'   => $cycle,
            'plc_output' => trim($echo_out) ?: null,
        );
    }
    $echo_out = ob_get_clean();
    $elapsed  = round(microtime(true) - $t0, 1);
    dbglog("cycle=$cycle PLC done, elapsed={$elapsed}s");

    if (!$rsp_list || !is_array($rsp_list) || count($rsp_list) == 0) {
        return array(
            'error'   => 'Немає відповіді від лічильника (таймаут)',
            'serial'  => strtoupper($serial),
            'elapsed' => $elapsed,
            'cycle'   => $cycle,
            'plc_output' => trim($echo_out) ?: null,
        );
    }

    // Parse binary responses
    $cur_data = array();
    $ext_data = array();
    foreach ($rsp_list as $rsp) {
        if (strlen($rsp) < 3) continue;
        if (ord($rsp[0]) == 0xFE) continue; // FRAME_ERROR

        $data = substr($rsp, 1);
        $dlen = strlen($data);
        $pos  = 0;
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

            if ($cmd == 13 && empty($cur_data))
                $cur_data = parse_cmd13($cmd_data, $cmd_data_len, $is_mtx1, $is_mtx3, $is_mtx3_4k);
            elseif ($cmd == 58 && empty($ext_data))
                $ext_data = parse_cmd58($cmd_data, $cmd_data_len, $is_mtx3);
        }
    }

    $result = array(
        'ok'        => !empty($cur_data) && !isset($cur_data['error_cmd13']),
        'serial'    => strtoupper($serial),
        'type'      => $is_mtx3 ? ($is_mtx3_4k ? 'MTX3_4K' : 'MTX3') : ($is_mtx1 ? 'MTX1' : 'unknown(0x'.dechex($GLOBALS['tipo']).')'),
        'timestamp' => date('Y-m-d H:i:s'),
        'elapsed'   => $elapsed,
        'cycle'     => $cycle,
    );
    if (!empty($cur_data)) foreach ($cur_data as $k => $v) $result[$k] = $v;
    if (!empty($ext_data)) foreach ($ext_data as $k => $v) $result[$k] = $v;

    if (empty($cur_data) || isset($cur_data['error_cmd13'])) {
        $result['error'] = 'Не вдалося розпарсити відповідь';
        $result['rsp_hex'] = array();
        foreach ($rsp_list as $i => $r) $result['rsp_hex'][$i] = bin2hex($r);
    }
    if (trim($echo_out)) $result['plc_output'] = trim($echo_out);
    return $result;
}

// --- SSE streaming mode: ?stream=N ---
$stream_count = isset($_GET['stream']) ? intval($_GET['stream']) : 0;

if ($stream_count > 0) {
    // Server-Sent Events — PHP loops, no re-init per cycle
    $_shutdown_json_sent = true; // shutdown handler not needed for SSE
    set_time_limit(0); // unlimited

    while (ob_get_level() > 0) ob_get_clean();
    // Ensure PHP detects client disconnect ASAP
    ignore_user_abort(false);

    header("Content-Type: text/event-stream; charset=utf-8");
    header("Cache-Control: no-cache");
    header("X-Accel-Buffering: no"); // tell nginx not to buffer
    header("Connection: keep-alive");

    // Cap stream_count to avoid holding php-fpm worker for hours
    // With 3 workers on RT4, each SSE session is precious
    if ($stream_count > 50) $stream_count = 50;

    dbglog("SSE mode, stream_count=$stream_count");

    for ($cycle = 1; $cycle <= $stream_count; $cycle++) {
        if (connection_aborted()) {
            dbglog("SSE: client disconnected at cycle $cycle");
            break;
        }

        $result = do_plc_read($aparatos, $serial, $cmd_list, $psw_level, $psw, $cifrar, $frame_type, $is_mtx1, $is_mtx3, $is_mtx3_4k, $cycle);
        $result = sanitize_for_json($result);
        $json = json_encode($result, JSON_UNESCAPED_UNICODE);
        if ($json === false) {
            $json = json_encode(array('error' => 'JSON encode failed', 'cycle' => $cycle));
        }

        echo "data: " . $json . "\n\n";
        flush();

        // Check if client is still connected after flush
        if (connection_aborted()) {
            dbglog("SSE: client gone after flush at cycle $cycle");
            break;
        }

        dbglog("SSE: sent cycle $cycle, ok=" . (!empty($result['ok']) ? 'Y' : 'N'));

        // Small pause before next cycle (let PLC settle)
        if ($cycle < $stream_count) {
            usleep(500000); // 0.5s
        }
    }

    echo "event: done\ndata: {\"cycles\":$cycle}\n\n";
    flush();
    dbglog("SSE: done after $cycle cycles");
    exit;
}

// --- Single read mode (original) ---
$result = do_plc_read($aparatos, $serial, $cmd_list, $psw_level, $psw, $cifrar, $frame_type, $is_mtx1, $is_mtx3, $is_mtx3_4k, 1);
if ($init_garbage) $result['init_output'] = $init_garbage;
json_out($result);


// ===============================================================
// Binary parsing functions
// ===============================================================

/** Read signed 32-bit big-endian integer */
function ri32($d, $o) {
    $u = unpack('N', substr($d, $o, 4));
    $v = $u[1];
    // Handle 32-bit PHP where hexdec returns float for values > PHP_INT_MAX
    if (is_float($v) && $v >= 2147483648) {
        return intval($v - 4294967296);
    }
    if ($v >= 2147483648) return $v - 4294967296;
    return intval($v);
}

/** Read unsigned 32-bit big-endian integer */
function ru32($d, $o) {
    $u = unpack('N', substr($d, $o, 4));
    return $u[1];
}

/** Read signed 16-bit big-endian integer */
function ri16($d, $o) {
    $u = unpack('n', substr($d, $o, 2));
    $v = $u[1];
    if ($v >= 32768) return $v - 65536;
    return $v;
}

/** Parse CMD_GET_CUR_VALUES (cmd 13) response */
function parse_cmd13($data, $len, $is_mtx1, $is_mtx3, $is_4k) {

    // MTX1 single-phase: 32 bytes
    if ($is_mtx1 && $len == 32) {
        $power_a  = ri32($data, 0);
        $ia_rms   = ru32($data, 4);
        $vavb_rms = ru32($data, 8);
        $var_a    = ri32($data, 12);
        $pf_a     = ri16($data, 16);
        $ib_rms   = ru32($data, 18);
        $power_b  = ri32($data, 22);
        $var_b    = ri32($data, 26);
        $pf_b     = ri16($data, 30);

        return array(
            'voltage' => array(
                'AB' => round($vavb_rms * 0.001, 1),
            ),
            'current' => array(
                'A' => round($ia_rms / 1000, 3),
                'B' => round($ib_rms / 1000, 3),
                'A_mA' => $ia_rms,
                'B_mA' => $ib_rms,
            ),
            'power_active' => array(
                'A' => round($power_a * 0.0001, 3),
                'B' => round($power_b * 0.0001, 3),
            ),
            'power_reactive' => array(
                'A' => round($var_a * 0.0001, 3),
                'B' => round($var_b * 0.0001, 3),
            ),
            'power_factor' => array(
                'A' => round($pf_a * 0.001, 3),
                'B' => round($pf_b * 0.001, 3),
            ),
        );
    }

    // MTX3 three-phase: 48 bytes (2K) or 52 bytes (4K)
    if ($is_mtx3 && ($len == 48 || ($is_4k && $len == 52))) {
        // 12 values × 4 bytes: VA_rms, VB_rms, VC_rms, IA_rms, IB_rms, IC_rms,
        //                       POWER_A, POWER_B, POWER_C, VAR_A, VAR_B, VAR_C
        $vals = array();
        for ($i = 0; $i < 12; $i++) {
            $vals[] = ri32($data, $i * 4);
        }

        $result = array(
            'voltage' => array(
                'A' => round($vals[0] * 0.001, 1),
                'B' => round($vals[1] * 0.001, 1),
                'C' => round($vals[2] * 0.001, 1),
            ),
            'current' => array(
                'A' => round($vals[3] / 1000, 3),
                'B' => round($vals[4] / 1000, 3),
                'C' => round($vals[5] / 1000, 3),
                'A_mA' => $vals[3],
                'B_mA' => $vals[4],
                'C_mA' => $vals[5],
            ),
            'power_active' => array(
                'A' => round($vals[6] * 0.000001, 3),
                'B' => round($vals[7] * 0.000001, 3),
                'C' => round($vals[8] * 0.000001, 3),
            ),
            'power_reactive' => array(
                'A' => round($vals[9] * 0.000001, 3),
                'B' => round($vals[10] * 0.000001, 3),
                'C' => round($vals[11] * 0.000001, 3),
            ),
        );

        if ($is_4k && $len == 52) {
            $i_n = ru32($data, 48);
            $result['current']['N']    = round($i_n / 1000, 3);
            $result['current']['N_mA'] = $i_n;
        }

        return $result;
    }

    return array('error_cmd13' => "Unexpected data length: $len (mtx1=$is_mtx1, mtx3=$is_mtx3, 4k=$is_4k)");
}

/** Parse CMD_GET_EXTEND_CUR_VALUES (cmd 58) response */
function parse_cmd58($data, $len, $is_mtx3) {

    // Short format: 4 bytes (Temperature + Frequency) — MTX1
    if ($len >= 4) {
        $temperature = ri16($data, 0);
        $u = unpack('n', substr($data, 2, 2));
        $freq_norm = $u[1];

        $result = array(
            'frequency'   => round($freq_norm * 0.1, 1),
            'temperature' => round($temperature * 0.1, 1),
        );

        // Extended format: 38 bytes — MTX3 (includes power factor, VA, battery voltage)
        if ($len == 38 && $is_mtx3) {
            $result['power_factor'] = array(
                'A'     => round(ri16($data, 12) * 0.001, 3),
                'B'     => round(ri16($data, 14) * 0.001, 3),
                'C'     => round(ri16($data, 16) * 0.001, 3),
                'total' => round(ri16($data, 18) * 0.001, 3),
            );
            $result['apparent_power'] = array(
                'A'     => round(ru32($data, 20) * 0.000001, 3),
                'B'     => round(ru32($data, 24) * 0.000001, 3),
                'C'     => round(ru32($data, 28) * 0.000001, 3),
                'total' => round(ru32($data, 32) * 0.000001, 3),
            );
            $u_bat = unpack('n', substr($data, 36, 2));
            $result['battery'] = round($u_bat[1] * 0.001, 3);
        }

        return $result;
    }

    return array();
}
