<?php
/*
 * meter_data.php — Read meter data from SQLite database
 *
 * Usage (HTTP GET):
 *   meter_data.php?cmd=list                                — List all meters
 *   meter_data.php?addr=5&cmd=current                      — Current consumption (amps, voltage, power)
 *   meter_data.php?addr=5&cmd=monthly&month=2026-03        — Monthly energy consumption (kWh)
 *   meter_data.php?addr=5&cmd=halfhour&date=2026-03-10     — 30-min data (1 day)
 *   meter_data.php?addr=5&cmd=halfhour&date=2026-03-01&days=7 — 30-min data (7 days)
 *   meter_data.php?addr=5&cmd=schedule                       — View COMSchedule for meter
 *   meter_data.php?addr=5&cmd=request_read&flags=3           — Request reading (flags bitmask)
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

// --- Device type detection (from a_tipos_mtx.inc) ---
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

// --- RMFlags bitmask ---
define("RMF_LOAD_HH",        0x0001); // Load Profile (30-min)
define("RMF_LOAD_DAY",       0x0002); // Meter Readings (daily)
define("RMF_LOAD_MEAS",      0x0010); // Measurements (current)
define("RMF_LOAD_EVENTS",    0x0020); // Event Log
define("RMF_LOAD_CRITICAL",  0x0040); // Critical Events
define("RMF_LOAD_CLOCK",     0x0080); // Clock Sync
define("RMF_LOAD_PEAK",      0x0100); // Peak Power
define("RMF_LOAD_VOLTAGE",   0x0200); // Voltage Profile
define("RMF_LOAD_CURRENT",   0x0400); // Current Profile

function decode_rmflags($flags) {
    $flags = intval($flags);
    $result = [];
    $map = [
        RMF_LOAD_HH       => 'LP',
        RMF_LOAD_DAY      => 'MR',
        RMF_LOAD_MEAS     => 'Ms',
        RMF_LOAD_EVENTS   => 'Log',
        RMF_LOAD_CRITICAL  => 'AS',
        RMF_LOAD_CLOCK    => 'Sync',
        RMF_LOAD_PEAK     => 'MP',
        RMF_LOAD_VOLTAGE  => 'VP',
        RMF_LOAD_CURRENT  => 'CP',
    ];
    foreach ($map as $bit => $label) {
        if ($flags & $bit) $result[] = $label;
    }
    return $result;
}

// --- Open DB ---
function open_db($readonly = true) {
    if (!file_exists(DB_PATH)) {
        err_exit("Database not found: " . DB_PATH);
    }
    $db = new SQLite3(DB_PATH, $readonly ? SQLITE3_OPEN_READONLY : SQLITE3_OPEN_READWRITE);
    $db->busyTimeout(2000);
    // WAL mode allows concurrent reads while PollReader writes
    @$db->exec('PRAGMA journal_mode=WAL');
    @$db->exec('PRAGMA read_uncommitted=ON');
    return $db;
}

// --- Get meter info by mAddress ---
function get_meter($db, $address) {
    $stmt = $db->prepare("SELECT id, mSerialNumber, mAddress, mType FROM Meters
                          WHERE mAddress = :addr AND (DELETED = 0 OR DELETED IS NULL)");
    $stmt->bindValue(':addr', intval($address), SQLITE3_INTEGER);
    $res = $stmt->execute();
    $row = $res->fetchArray(SQLITE3_ASSOC);
    $res->finalize();

    if (!$row) {
        err_exit("Meter with address $address not found");
    }

    $row['tipo'] = get_device_tipo($row['mSerialNumber']);
    $row['is_mtx3'] = is_mtx3($row['tipo']);
    return $row;
}

// --- List all meters ---
function cmd_list($db) {
    $res = $db->query("SELECT m.id, m.mSerialNumber, m.mAddress, m.mType,
                              cs.StartTime, cs.RMFlags, cs.LastRun, cs.DISABLED
                       FROM Meters m
                       LEFT JOIN COMSchedule cs ON cs.mid = m.id
                       WHERE (m.DELETED = 0 OR m.DELETED IS NULL)
                       ORDER BY m.mAddress");

    $meters = [];
    while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
        $tipo = get_device_tipo($row['mSerialNumber']);
        $flags = intval($row['RMFlags']);
        $meters[] = [
            'address'   => $row['mAddress'],
            'serial'    => $row['mSerialNumber'],
            'type'      => is_mtx3($tipo) ? 'MTX3' : (is_mtx1($tipo) ? 'MTX1' : 'unknown'),
            'schedule'  => $row['StartTime'],
            'lastRun'   => $row['LastRun'],
            'rmflags'   => $flags,
            'tasks'     => decode_rmflags($flags),
            'disabled'  => intval($row['DISABLED']),
        ];
    }
    $res->finalize();

    echo json_encode(['meters' => $meters], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// --- Current consumption (latest mediciones) ---
function cmd_current($db, $meter) {
    $mid = $meter['id'];
    $table = $meter['is_mtx3'] ? 'mediciones_mtx3' : 'mediciones_mtx1';

    $res = $db->query("SELECT * FROM $table WHERE mid = $mid ORDER BY fecha DESC LIMIT 1");
    $row = $res->fetchArray(SQLITE3_ASSOC);
    $res->finalize();

    if (!$row) {
        echo json_encode(['error' => 'No measurement data found for this meter']) . "\n";
        return;
    }

    $data = [
        'meter'     => $meter['mAddress'],
        'serial'    => strtoupper($meter['mSerialNumber']),
        'type'      => $meter['is_mtx3'] ? 'MTX3' : 'MTX1',
        'timestamp' => $row['fecha'],
    ];

    if ($meter['is_mtx3']) {
        // MTX3: 3-phase meter
        $data['voltage'] = [
            'A' => round($row['VA_rms'] * 0.001, 1),   // V
            'B' => round($row['VB_rms'] * 0.001, 1),
            'C' => round($row['VC_rms'] * 0.001, 1),
            'unit' => 'V',
        ];
        $data['current'] = [
            'A' => round($row['IA_rms'] / 1000, 3),    // mA -> A
            'B' => round($row['IB_rms'] / 1000, 3),
            'C' => round($row['IC_rms'] / 1000, 3),
            'unit' => 'A',
            'raw_mA' => [
                'A' => $row['IA_rms'],
                'B' => $row['IB_rms'],
                'C' => $row['IC_rms'],
            ],
        ];
        $data['power_active'] = [
            'A' => round($row['POWER_A'] * 0.00001 * 0.1, 3),  // kW
            'B' => round($row['POWER_B'] * 0.00001 * 0.1, 3),
            'C' => round($row['POWER_C'] * 0.00001 * 0.1, 3),
            'unit' => 'kW',
        ];
        $data['power_reactive'] = [
            'A' => round($row['VAR_A'] * 0.00001 * 0.1, 3),    // kVar
            'B' => round($row['VAR_B'] * 0.00001 * 0.1, 3),
            'C' => round($row['VAR_C'] * 0.00001 * 0.1, 3),
            'unit' => 'kVar',
        ];
        $data['frequency'] = round($row['FREQ_norm'] * 0.1, 1);    // Hz
        $data['temperature'] = round($row['Temperature'] * 0.1, 1); // °C
    } else {
        // MTX1: single-phase meter
        $data['voltage'] = [
            'AB' => round($row['vavb_rms'] * 0.001, 3),  // V
            'unit' => 'V',
        ];
        $data['current'] = [
            'A' => round($row['ia_rms'] / 1000, 3),    // mA -> A
            'B' => round($row['ib_rms'] / 1000, 3),
            'unit' => 'A',
            'raw_mA' => [
                'A' => $row['ia_rms'],
                'B' => $row['ib_rms'],
            ],
        ];
        $data['power_active'] = [
            'A' => round($row['power'] * 0.0001, 3),     // kW
            'B' => round($row['power_b'] * 0.0001, 3),
            'unit' => 'kW',
        ];
        $data['power_reactive'] = [
            'A' => round($row['var_a'] * 0.0001, 3),     // kVar
            'B' => round($row['var_b'] * 0.0001, 3),
            'unit' => 'kVar',
        ];
        $data['frequency'] = round($row['FREQ_norm'] * 0.1, 1);
        $data['temperature'] = round($row['Temperature'] * 0.1, 1);
    }

    echo json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// --- Monthly consumption ---
function cmd_monthly($db, $meter, $month = null) {
    $mid = $meter['id'];
    $table = $meter['is_mtx3'] ? 'indicaciones_mtx3' : 'indicaciones_mtx1';

    if (!$month) {
        $month = date('Y-m');
    }

    // Parse month range
    $start = $month . '-01';
    $end = date('Y-m-t', strtotime($start)); // last day of month

    // pType = 1 (OBIS_1_8_0 = Active energy A+)
    $query = "SELECT fecha, r_fecha, pType, Wh_t0, Wh_t1, Wh_t2, Wh_t3
              FROM $table
              WHERE mid = $mid
                AND pType = 1
                AND strftime('%Y-%m-%d', fecha) >= '$start'
                AND strftime('%Y-%m-%d', fecha) <= '$end'
              ORDER BY fecha";

    $res = $db->query($query);

    $days = [];
    $totals = ['t0' => 0, 't1' => 0, 't2' => 0, 't3' => 0];
    $first_reading = null;
    $last_reading = null;

    while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
        $wh_sum = $row['Wh_t0'] + $row['Wh_t1'] + $row['Wh_t2'] + $row['Wh_t3'];

        $day = [
            'date'    => $row['fecha'],
            'read_at' => $row['r_fecha'],
            'tariff'  => [
                'T1' => round($row['Wh_t0'] * 0.001, 3),   // Wh -> kWh
                'T2' => round($row['Wh_t1'] * 0.001, 3),
                'T3' => round($row['Wh_t2'] * 0.001, 3),
                'T4' => round($row['Wh_t3'] * 0.001, 3),
            ],
            'total_kWh' => round($wh_sum * 0.001, 3),
        ];
        $days[] = $day;

        if (!$first_reading) $first_reading = $row;
        $last_reading = $row;
    }
    $res->finalize();

    // indicaciones are cumulative meter readings, not daily deltas
    // Monthly consumption = last reading - first reading of prev month's last day
    // But we need to find the delta. Let's get the reading just before our range too
    $prev_query = "SELECT Wh_t0, Wh_t1, Wh_t2, Wh_t3
                   FROM $table
                   WHERE mid = $mid AND pType = 1
                     AND strftime('%Y-%m-%d', fecha) < '$start'
                   ORDER BY fecha DESC LIMIT 1";
    $prev_res = $db->query($prev_query);
    $prev = $prev_res->fetchArray(SQLITE3_ASSOC);
    $prev_res->finalize();

    $consumption = null;
    if ($prev && $last_reading) {
        $consumption = [
            'T1' => round(($last_reading['Wh_t0'] - $prev['Wh_t0']) * 0.001, 3),
            'T2' => round(($last_reading['Wh_t1'] - $prev['Wh_t1']) * 0.001, 3),
            'T3' => round(($last_reading['Wh_t2'] - $prev['Wh_t2']) * 0.001, 3),
            'T4' => round(($last_reading['Wh_t3'] - $prev['Wh_t3']) * 0.001, 3),
            'total_kWh' => round(
                (($last_reading['Wh_t0'] - $prev['Wh_t0']) +
                 ($last_reading['Wh_t1'] - $prev['Wh_t1']) +
                 ($last_reading['Wh_t2'] - $prev['Wh_t2']) +
                 ($last_reading['Wh_t3'] - $prev['Wh_t3'])) * 0.001, 3),
            'unit' => 'kWh',
        ];
    }

    $data = [
        'meter'       => $meter['mAddress'],
        'serial'      => strtoupper($meter['mSerialNumber']),
        'month'       => $month,
        'readings'    => $days,
        'consumption' => $consumption,
    ];

    echo json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// --- Halfhour data (30-min intervals for graph) ---
// Returns ALL available pType series so frontend can display/toggle them
function cmd_halfhour($db, $meter, $date, $days = 1) {
    $mid = $meter['id'];

    $start = $date;
    $end = date('Y-m-d', strtotime($date . " + " . ($days - 1) . " days"));

    // OBIS type mapping
    $obis_map = [
        21 => ['code' => 'A+',  'name' => 'Активна A+',        'unit' => 'kWh',  'color' => '#2980b9'],
        22 => ['code' => 'A-',  'name' => 'Зворотна A-',       'unit' => 'kWh',  'color' => '#e74c3c'],
        23 => ['code' => 'Q+',  'name' => 'Реактивна Q+',      'unit' => 'kvarh', 'color' => '#27ae60'],
        24 => ['code' => 'Q-',  'name' => 'Реактивна Q-',      'unit' => 'kvarh', 'color' => '#f39c12'],
        // VP/CP profiles use different pTypes — will appear automatically when collected
    ];

    // Fetch ALL pTypes for this meter in the date range
    $query = "SELECT fecha, pType, " .
             implode(', ', array_map(function($i) { return "Wh_t$i"; }, range(0, 47))) .
             " FROM archivo_mhora
               WHERE mid = $mid
                 AND strftime('%Y-%m-%d', fecha) >= '$start'
                 AND strftime('%Y-%m-%d', fecha) <= '$end'
               ORDER BY pType, fecha";

    $res = $db->query($query);

    // Group data by pType → series
    $series = [];  // pType => [points]
    $total_points = 0;

    while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
        $pt = intval($row['pType']);
        $fecha = $row['fecha'];

        if (!isset($series[$pt])) $series[$pt] = [];

        for ($t = 0; $t < 48; $t++) {
            $wh = $row["Wh_t$t"];
            if (is_null($wh)) continue;

            $hour = intdiv($t, 2);
            $min = ($t % 2) * 30;
            $time = sprintf("%s %02d:%02d:00", $fecha, $hour, $min);

            $series[$pt][] = [
                'time' => $time,
                'Wh'   => intval($wh),
                'kWh'  => round($wh * 0.001, 3),
            ];
            $total_points++;
        }
    }
    $res->finalize();

    // Build series metadata
    $series_info = [];
    foreach ($series as $pt => $points) {
        $info = isset($obis_map[$pt]) ? $obis_map[$pt] : [
            'code' => "pType$pt", 'name' => "pType=$pt", 'unit' => 'Wh', 'color' => '#999'
        ];
        $series_info[] = [
            'pType'  => $pt,
            'code'   => $info['code'],
            'name'   => $info['name'],
            'unit'   => $info['unit'],
            'color'  => $info['color'],
            'points' => count($points),
            'data'   => $points,
        ];
    }

    // Backward compatibility: also output flat 'data' for pType=21 (A+)
    $data_flat = isset($series[21]) ? $series[21] : (count($series) > 0 ? reset($series) : []);

    $data = [
        'meter'      => $meter['mAddress'],
        'serial'     => strtoupper($meter['mSerialNumber']),
        'date_from'  => $start,
        'date_to'    => $end,
        'interval'   => '30min',
        'points'     => $total_points,
        'series'     => $series_info,
        'data'       => $data_flat,  // backward compat: A+ or first available
    ];

    echo json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// --- View COMSchedule for a meter ---
function cmd_schedule($db, $meter) {
    $mid = $meter['id'];

    $res = $db->query("SELECT * FROM COMSchedule WHERE mid = $mid");
    $row = $res->fetchArray(SQLITE3_ASSOC);
    $res->finalize();

    if (!$row) {
        echo json_encode([
            'meter' => $meter['mAddress'],
            'serial' => strtoupper($meter['mSerialNumber']),
            'schedule' => null,
            'message' => 'No COMSchedule entry for this meter'
        ]) . "\n";
        return;
    }

    $flags = intval($row['RMFlags']);

    // Check data availability
    $table_med = $meter['is_mtx3'] ? 'mediciones_mtx3' : 'mediciones_mtx1';
    $table_ind = $meter['is_mtx3'] ? 'indicaciones_mtx3' : 'indicaciones_mtx1';

    $cnt_med = $db->querySingle("SELECT COUNT(*) FROM $table_med WHERE mid=$mid");
    $cnt_ind = $db->querySingle("SELECT COUNT(*) FROM $table_ind WHERE mid=$mid");
    $cnt_hh  = $db->querySingle("SELECT COUNT(*) FROM archivo_mhora WHERE mid=$mid");

    $last_med = $db->querySingle("SELECT fecha FROM $table_med WHERE mid=$mid ORDER BY fecha DESC LIMIT 1");
    $last_ind = $db->querySingle("SELECT fecha FROM $table_ind WHERE mid=$mid ORDER BY fecha DESC LIMIT 1");
    $last_hh  = $db->querySingle("SELECT fecha FROM archivo_mhora WHERE mid=$mid ORDER BY fecha DESC LIMIT 1");

    $first_ind = $db->querySingle("SELECT fecha FROM $table_ind WHERE mid=$mid ORDER BY fecha ASC LIMIT 1");
    $first_hh  = $db->querySingle("SELECT fecha FROM archivo_mhora WHERE mid=$mid ORDER BY fecha ASC LIMIT 1");

    $data = [
        'meter'    => $meter['mAddress'],
        'serial'   => strtoupper($meter['mSerialNumber']),
        'type'     => $meter['is_mtx3'] ? 'MTX3' : 'MTX1',
        'schedule' => [
            'id'        => $row['id'],
            'startTime' => $row['StartTime'],
            'lastRun'   => $row['LastRun'],
            'rmflags'   => $flags,
            'tasks'     => decode_rmflags($flags),
            'disabled'  => intval($row['DISABLED']),
        ],
        'data_available' => [
            'mediciones'   => ['count' => intval($cnt_med), 'last' => $last_med],
            'indicaciones' => ['count' => intval($cnt_ind), 'first' => $first_ind, 'last' => $last_ind],
            'archivo_mhora'=> ['count' => intval($cnt_hh),  'first' => $first_hh,  'last' => $last_hh],
        ],
    ];

    echo json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// --- Queue: show all COMSchedule entries with pending tasks ---
function cmd_queue($db) {
    // RMFlags > 0 means there are pending tasks for PollReader
    // Also show recently completed (LastRun within last hour) for context
    $res = $db->query("
        SELECT cs.id, cs.mid, cs.StartTime, cs.RMFlags, cs.DISABLED, cs.LastRun,
               m.mAddress, m.mSerialNumber, m.mType
        FROM COMSchedule cs
        JOIN Meters m ON m.id = cs.mid
        WHERE (m.DELETED = 0 OR m.DELETED IS NULL)
        ORDER BY cs.StartTime DESC
    ");

    $pending = [];
    $idle = [];
    $disabled = [];

    while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
        $flags = intval($row['RMFlags']);
        $tipo = get_device_tipo($row['mSerialNumber']);

        $entry = [
            'address'   => $row['mAddress'],
            'serial'    => $row['mSerialNumber'],
            'type'      => is_mtx3($tipo) ? 'MTX3' : (is_mtx1($tipo) ? 'MTX1' : 'unknown'),
            'startTime' => $row['StartTime'],
            'lastRun'   => $row['LastRun'],
            'rmflags'   => $flags,
            'tasks'     => decode_rmflags($flags),
            'disabled'  => intval($row['DISABLED']),
        ];

        if (intval($row['DISABLED'])) {
            $disabled[] = $entry;
        } elseif ($flags > 0) {
            $pending[] = $entry;
        } else {
            $idle[] = $entry;
        }
    }
    $res->finalize();

    echo json_encode([
        'pending'  => $pending,
        'idle'     => $idle,
        'disabled' => $disabled,
        'summary'  => [
            'pending'  => count($pending),
            'idle'     => count($idle),
            'disabled' => count($disabled),
            'total'    => count($pending) + count($idle) + count($disabled),
        ],
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// --- Request reading from meter (set RMFlags) ---
function cmd_request_read($address, $flags_to_add, $start_date = null) {
    $flags_to_add = intval($flags_to_add);
    if ($flags_to_add <= 0 || $flags_to_add > 0x07FF) {
        err_exit("Invalid flags value. Use bitmask: 0x0001=LP, 0x0002=MR, 0x0010=Ms, 0x0003=LP+MR");
    }

    // Validate and format start_date if provided
    $start_sql = "datetime('now','localtime')";
    if ($start_date) {
        // Accept YYYY-MM-DD or YYYY-MM-DD HH:MM:SS
        $ts = strtotime($start_date);
        if ($ts !== false) {
            $start_sql = "'" . date('Y-m-d H:i:s', $ts) . "'";
        }
    }

    // Open in read-write mode
    $db = new SQLite3(DB_PATH, SQLITE3_OPEN_READWRITE);
    $db->busyTimeout(5000);

    $meter = get_meter($db, $address);
    $mid = $meter['id'];

    // Check if COMSchedule entry exists
    $res = $db->query("SELECT id, RMFlags FROM COMSchedule WHERE mid = $mid");
    $row = $res->fetchArray(SQLITE3_ASSOC);
    $res->finalize();

    if ($row) {
        $old_flags = intval($row['RMFlags']);
        $new_flags = $old_flags | $flags_to_add;
        $db->exec("UPDATE COMSchedule SET RMFlags=$new_flags, StartTime=$start_sql WHERE id=" . $row['id']);
    } else {
        $db->exec("INSERT INTO COMSchedule (mid, StartTime, RMFlags) VALUES ($mid, $start_sql, $flags_to_add)");
    }

    $db->close();

    // Re-read to confirm
    $db2 = new SQLite3(DB_PATH, SQLITE3_OPEN_READONLY);
    $db2->busyTimeout(5000);
    $res2 = $db2->query("SELECT RMFlags, StartTime FROM COMSchedule WHERE mid = $mid");
    $row2 = $res2->fetchArray(SQLITE3_ASSOC);
    $res2->finalize();
    $db2->close();

    $new_flags = intval($row2['RMFlags']);
    echo json_encode([
        'meter'     => $address,
        'serial'    => strtoupper($meter['mSerialNumber']),
        'result'    => 'ok',
        'rmflags'   => $new_flags,
        'tasks'     => decode_rmflags($new_flags),
        'startTime' => $row2['StartTime'],
        'added'     => decode_rmflags($flags_to_add),
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// --- Bulk request reading for multiple meters ---
function cmd_bulk_request_read($addresses, $flags_to_add, $start_date = null) {
    $flags_to_add = intval($flags_to_add);
    if ($flags_to_add <= 0 || $flags_to_add > 0x07FF) {
        err_exit("Invalid flags value");
    }

    // Validate start_date if provided
    $start_sql = "datetime('now','localtime')";
    if ($start_date) {
        $ts = strtotime($start_date);
        if ($ts !== false) {
            $start_sql = "'" . date('Y-m-d H:i:s', $ts) . "'";
        }
    }

    $addr_list = array_filter(array_map('trim', explode(',', $addresses)));
    if (empty($addr_list)) {
        err_exit("No addresses provided");
    }

    $db = new SQLite3(DB_PATH, SQLITE3_OPEN_READWRITE);
    $db->busyTimeout(5000);

    $results = [];
    $ok = 0;
    $fail = 0;

    foreach ($addr_list as $address) {
        $address = intval($address);
        // Get meter id
        $stmt = $db->prepare("SELECT m.id, m.mSerialNumber, m.mAddress FROM Meters m WHERE m.mAddress = :addr AND (m.DELETED = 0 OR m.DELETED IS NULL)");
        $stmt->bindValue(':addr', $address, SQLITE3_INTEGER);
        $res = $stmt->execute();
        $meter = $res->fetchArray(SQLITE3_ASSOC);
        $res->finalize();

        if (!$meter) {
            $results[] = ['address' => $address, 'status' => 'not_found'];
            $fail++;
            continue;
        }

        $mid = $meter['id'];

        // Check existing COMSchedule entry
        $res2 = $db->query("SELECT id, RMFlags FROM COMSchedule WHERE mid = $mid");
        $row = $res2->fetchArray(SQLITE3_ASSOC);
        $res2->finalize();

        if ($row) {
            $old_flags = intval($row['RMFlags']);
            $new_flags = $old_flags | $flags_to_add;
            $db->exec("UPDATE COMSchedule SET RMFlags=$new_flags, StartTime=$start_sql WHERE id=" . $row['id']);
        } else {
            $db->exec("INSERT INTO COMSchedule (mid, StartTime, RMFlags) VALUES ($mid, $start_sql, $flags_to_add)");
        }

        $results[] = [
            'address' => $address,
            'serial'  => $meter['mSerialNumber'],
            'status'  => 'ok',
            'tasks'   => decode_rmflags($flags_to_add),
        ];
        $ok++;
    }

    $db->close();

    echo json_encode([
        'result'  => 'ok',
        'total'   => count($addr_list),
        'ok'      => $ok,
        'fail'    => $fail,
        'flags'   => $flags_to_add,
        'tasks'   => decode_rmflags($flags_to_add),
        'details' => $results,
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// --- Prioritize: clear queue for all except target meter ---
function cmd_prioritize($address, $flags_to_set) {
    $flags_to_set = intval($flags_to_set);
    if ($flags_to_set <= 0) $flags_to_set = RMF_LOAD_MEAS; // default: Ms only

    $db = new SQLite3(DB_PATH, SQLITE3_OPEN_READWRITE);
    $db->busyTimeout(5000);

    $meter = get_meter($db, $address);
    $mid = $meter['id'];

    // Count pending tasks for other meters
    $others = $db->querySingle("SELECT COUNT(*) FROM COMSchedule WHERE mid != $mid AND RMFlags > 0");

    // Clear all other meters' flags
    $db->exec("UPDATE COMSchedule SET RMFlags = 0 WHERE mid != $mid AND RMFlags > 0");

    // Set target meter flags + fresh StartTime
    $res = $db->query("SELECT id, RMFlags FROM COMSchedule WHERE mid = $mid");
    $row = $res->fetchArray(SQLITE3_ASSOC);
    $res->finalize();

    if ($row) {
        $new_flags = intval($row['RMFlags']) | $flags_to_set;
        $db->exec("UPDATE COMSchedule SET RMFlags=$new_flags, StartTime=datetime('now','localtime') WHERE id=" . $row['id']);
    } else {
        $db->exec("INSERT INTO COMSchedule (mid, StartTime, RMFlags) VALUES ($mid, datetime('now','localtime'), $flags_to_set)");
    }

    $db->close();

    // Re-read
    $db2 = new SQLite3(DB_PATH, SQLITE3_OPEN_READONLY);
    $db2->busyTimeout(5000);
    $res2 = $db2->query("SELECT RMFlags, StartTime FROM COMSchedule WHERE mid = $mid");
    $row2 = $res2->fetchArray(SQLITE3_ASSOC);
    $res2->finalize();
    $db2->close();

    $new_flags = intval($row2['RMFlags']);
    echo json_encode([
        'result'   => 'ok',
        'meter'    => $address,
        'serial'   => strtoupper($meter['mSerialNumber']),
        'rmflags'  => $new_flags,
        'tasks'    => decode_rmflags($new_flags),
        'startTime'=> $row2['StartTime'],
        'cleared'  => intval($others),
        'message'  => "Очищено чергу для $others лічильників. $address — пріоритет.",
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// --- Clear queue: reset RMFlags for all meters ---
function cmd_clear_queue() {
    $db = new SQLite3(DB_PATH, SQLITE3_OPEN_READWRITE);
    $db->busyTimeout(5000);

    $count = $db->querySingle("SELECT COUNT(*) FROM COMSchedule WHERE RMFlags > 0");
    $db->exec("UPDATE COMSchedule SET RMFlags = 0 WHERE RMFlags > 0");
    $db->close();

    echo json_encode([
        'result'  => 'ok',
        'cleared' => intval($count),
        'message' => "Очищено задачі для $count лічильників.",
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// --- Remove specific flags from selected meters (bulk) ---
function cmd_bulk_remove_flags($addresses, $flags_to_remove) {
    $flags_to_remove = intval($flags_to_remove);
    if ($flags_to_remove <= 0 || $flags_to_remove > 0x07FF) {
        err_exit("Invalid flags value");
    }

    $addr_list = array_filter(array_map('trim', explode(',', $addresses)));
    if (empty($addr_list)) {
        err_exit("No addresses provided");
    }

    $db = new SQLite3(DB_PATH, SQLITE3_OPEN_READWRITE);
    $db->busyTimeout(5000);

    $ok = 0;
    $mask = ~$flags_to_remove & 0x07FF;

    foreach ($addr_list as $address) {
        $address = intval($address);
        $stmt = $db->prepare("SELECT m.id FROM Meters m WHERE m.mAddress = :addr AND (m.DELETED = 0 OR m.DELETED IS NULL)");
        $stmt->bindValue(':addr', $address, SQLITE3_INTEGER);
        $res = $stmt->execute();
        $meter = $res->fetchArray(SQLITE3_ASSOC);
        $res->finalize();
        if (!$meter) continue;

        $mid = $meter['id'];
        $db->exec("UPDATE COMSchedule SET RMFlags = (RMFlags & $mask) WHERE mid = $mid AND (RMFlags & $flags_to_remove) > 0");
        $ok += $db->changes();
    }

    $db->close();

    echo json_encode([
        'result'   => 'ok',
        'removed'  => decode_rmflags($flags_to_remove),
        'affected' => $ok,
        'total'    => count($addr_list),
        'message'  => "Знято " . implode(', ', decode_rmflags($flags_to_remove)) . " для $ok лічильників з " . count($addr_list) . ".",
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// --- Bulk enable/disable meters ---
function cmd_bulk_set_disabled($addresses, $disabled_value) {
    $addr_list = array_filter(array_map('trim', explode(',', $addresses)));
    if (empty($addr_list)) {
        err_exit("No addresses provided");
    }

    $db = new SQLite3(DB_PATH, SQLITE3_OPEN_READWRITE);
    $db->busyTimeout(5000);

    $updated = 0;
    $disabled_value = intval($disabled_value);

    foreach ($addr_list as $address) {
        $address = intval($address);
        // Update Meters table: DELETED column (0 = active, 1 = disabled/deleted)
        $stmt = $db->prepare("UPDATE Meters SET DELETED = :del WHERE mAddress = :addr");
        $stmt->bindValue(':del', $disabled_value, SQLITE3_INTEGER);
        $stmt->bindValue(':addr', $address, SQLITE3_INTEGER);
        $stmt->execute();
        $updated += $db->changes();
    }

    $db->close();

    $action = $disabled_value ? 'disabled' : 'enabled';
    echo json_encode([
        'result'  => 'ok',
        'action'  => $action,
        'updated' => $updated,
        'total'   => count($addr_list),
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// --- Remove specific flags from all meters ---
function cmd_remove_flags($flags_to_remove) {
    $flags_to_remove = intval($flags_to_remove);
    if ($flags_to_remove <= 0 || $flags_to_remove > 0x07FF) {
        err_exit("Invalid flags value");
    }

    $db = new SQLite3(DB_PATH, SQLITE3_OPEN_READWRITE);
    $db->busyTimeout(5000);

    // Count affected
    $affected = $db->querySingle("SELECT COUNT(*) FROM COMSchedule WHERE (RMFlags & $flags_to_remove) > 0");

    // Bitwise AND with NOT of flags to remove
    $mask = ~$flags_to_remove & 0x07FF;
    $db->exec("UPDATE COMSchedule SET RMFlags = (RMFlags & $mask) WHERE (RMFlags & $flags_to_remove) > 0");

    $db->close();

    echo json_encode([
        'result'   => 'ok',
        'removed'  => decode_rmflags($flags_to_remove),
        'affected' => intval($affected),
        'message'  => "Знято " . implode(', ', decode_rmflags($flags_to_remove)) . " для $affected лічильників.",
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// --- Remove specific flag(s) from a single meter ---
function cmd_remove_single_flag($address, $flag_to_remove) {
    $db = new SQLite3(DB_PATH, SQLITE3_OPEN_READWRITE);
    $db->busyTimeout(5000);

    $meter = get_meter($db, $address);
    $mid = $meter['id'];

    $res = $db->query("SELECT id, RMFlags FROM COMSchedule WHERE mid = $mid");
    $row = $res->fetchArray(SQLITE3_ASSOC);
    $res->finalize();

    if (!$row) {
        $db->close();
        echo json_encode([
            'meter' => $address,
            'result' => 'ok',
            'rmflags' => 0,
            'tasks' => [],
            'message' => 'No schedule entry',
        ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
        return;
    }

    $old_flags = intval($row['RMFlags']);
    $new_flags = $old_flags & (~$flag_to_remove & 0x07FF);
    $db->exec("UPDATE COMSchedule SET RMFlags = $new_flags WHERE id = " . $row['id']);
    $db->close();

    echo json_encode([
        'meter'   => $address,
        'serial'  => strtoupper($meter['mSerialNumber']),
        'result'  => 'ok',
        'rmflags' => $new_flags,
        'tasks'   => decode_rmflags($new_flags),
        'removed' => decode_rmflags($flag_to_remove),
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// --- Daily report: latest readings per meter for a date ---
function cmd_daily_report($db, $date) {
    // Count total meters
    $total = $db->querySingle("SELECT COUNT(*) FROM Meters WHERE DELETED = 0 OR DELETED IS NULL");

    $meters = [];
    $tables = ['indicaciones_mtx1', 'indicaciones_mtx3'];

    foreach ($tables as $table) {
        $sql = "SELECT m.mAddress, m.mSerialNumber, m.mType,
                       i.Wh_t0, i.Wh_t1, i.Wh_t2, i.Wh_t3, i.fecha
                FROM $table i
                JOIN Meters m ON m.id = i.mid
                WHERE i.pType = 1
                  AND DATE(i.fecha) = :date
                  AND (m.DELETED = 0 OR m.DELETED IS NULL)
                GROUP BY i.mid
                HAVING i.fecha = MAX(i.fecha)";
        $stmt = $db->prepare($sql);
        $stmt->bindValue(':date', $date, SQLITE3_TEXT);
        $res = $stmt->execute();

        while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
            $tipo = get_device_tipo($row['mSerialNumber']);
            $type_str = is_mtx3($tipo) ? 'MTX3' : (is_mtx1($tipo) ? 'MTX1' : 'unknown');
            $t1 = round($row['Wh_t0'] * 0.001, 1);
            $t2 = round($row['Wh_t1'] * 0.001, 1);
            $t3 = round($row['Wh_t2'] * 0.001, 1);
            $t4 = round($row['Wh_t3'] * 0.001, 1);
            $meters[] = [
                'address' => $row['mAddress'],
                'serial'  => $row['mSerialNumber'],
                'type'    => $type_str,
                'a_plus'  => [
                    't1' => $t1, 't2' => $t2,
                    't3' => $t3 > 0 ? $t3 : null,
                    't4' => $t4 > 0 ? $t4 : null,
                    'total' => round($t1 + $t2 + $t3 + $t4, 1)
                ],
                'fecha' => $row['fecha']
            ];
        }
        $res->finalize();
    }

    // Sort by address
    usort($meters, function($a, $b) { return strcmp($a['address'], $b['address']); });

    echo json_encode([
        'date' => $date,
        'meters' => $meters,
        'summary' => [
            'total_meters' => intval($total),
            'with_data' => count($meters),
            'missing' => intval($total) - count($meters)
        ]
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// --- Monthly report: consumption (end - start) for all meters ---
function cmd_monthly_report($db, $month) {
    $start = $month . '-01';
    $end = date('Y-m-t', strtotime($start));
    $total = $db->querySingle("SELECT COUNT(*) FROM Meters WHERE DELETED = 0 OR DELETED IS NULL");

    $meters = [];
    $tables = ['indicaciones_mtx1', 'indicaciones_mtx3'];

    foreach ($tables as $table) {
        // Get first and last reading per meter for the month (pType=1, A+)
        $sql = "SELECT m.mAddress, m.mSerialNumber, m.mType, i.mid,
                       MIN(i.fecha) as first_fecha, MAX(i.fecha) as last_fecha
                FROM $table i
                JOIN Meters m ON m.id = i.mid
                WHERE i.pType = 1
                  AND DATE(i.fecha) >= :start AND DATE(i.fecha) <= :end
                  AND (m.DELETED = 0 OR m.DELETED IS NULL)
                GROUP BY i.mid";
        $stmt = $db->prepare($sql);
        $stmt->bindValue(':start', $start, SQLITE3_TEXT);
        $stmt->bindValue(':end', $end, SQLITE3_TEXT);
        $res = $stmt->execute();

        while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
            $mid = $row['mid'];
            $tipo = get_device_tipo($row['mSerialNumber']);
            $type_str = is_mtx3($tipo) ? 'MTX3' : (is_mtx1($tipo) ? 'MTX1' : 'unknown');

            // Get first reading values
            $q1 = $db->prepare("SELECT Wh_t0,Wh_t1,Wh_t2,Wh_t3 FROM $table WHERE mid=:mid AND pType=1 AND fecha=:f");
            $q1->bindValue(':mid', $mid, SQLITE3_INTEGER);
            $q1->bindValue(':f', $row['first_fecha'], SQLITE3_TEXT);
            $r1 = $q1->execute(); $first = $r1->fetchArray(SQLITE3_ASSOC); $r1->finalize();

            // Get last reading values
            $q2 = $db->prepare("SELECT Wh_t0,Wh_t1,Wh_t2,Wh_t3 FROM $table WHERE mid=:mid AND pType=1 AND fecha=:f");
            $q2->bindValue(':mid', $mid, SQLITE3_INTEGER);
            $q2->bindValue(':f', $row['last_fecha'], SQLITE3_TEXT);
            $r2 = $q2->execute(); $last = $r2->fetchArray(SQLITE3_ASSOC); $r2->finalize();

            if (!$first || !$last) continue;

            $st = ['t1'=>round($first['Wh_t0']*0.001,1), 't2'=>round($first['Wh_t1']*0.001,1),
                   't3'=>round($first['Wh_t2']*0.001,1), 't4'=>round($first['Wh_t3']*0.001,1)];
            $st['total'] = round($st['t1']+$st['t2']+$st['t3']+$st['t4'], 1);

            $en = ['t1'=>round($last['Wh_t0']*0.001,1), 't2'=>round($last['Wh_t1']*0.001,1),
                   't3'=>round($last['Wh_t2']*0.001,1), 't4'=>round($last['Wh_t3']*0.001,1)];
            $en['total'] = round($en['t1']+$en['t2']+$en['t3']+$en['t4'], 1);

            $con = ['t1'=>round($en['t1']-$st['t1'],1), 't2'=>round($en['t2']-$st['t2'],1),
                    't3'=>round($en['t3']-$st['t3'],1), 't4'=>round($en['t4']-$st['t4'],1)];
            $con['total'] = round($con['t1']+$con['t2']+$con['t3']+$con['t4'], 1);

            $meters[] = [
                'address' => $row['mAddress'],
                'serial'  => $row['mSerialNumber'],
                'type'    => $type_str,
                'start'   => array_merge($st, ['fecha' => $row['first_fecha']]),
                'end'     => array_merge($en, ['fecha' => $row['last_fecha']]),
                'consumed'=> $con
            ];
        }
        $res->finalize();
    }

    usort($meters, function($a, $b) { return strcmp($a['address'], $b['address']); });

    // Calculate totals
    $totals = ['t1'=>0, 't2'=>0, 't3'=>0, 't4'=>0, 'total'=>0];
    foreach ($meters as $m) {
        $totals['t1'] = round($totals['t1'] + $m['consumed']['t1'], 1);
        $totals['t2'] = round($totals['t2'] + $m['consumed']['t2'], 1);
        $totals['t3'] = round($totals['t3'] + $m['consumed']['t3'], 1);
        $totals['t4'] = round($totals['t4'] + $m['consumed']['t4'], 1);
        $totals['total'] = round($totals['total'] + $m['consumed']['total'], 1);
    }

    echo json_encode([
        'month' => $month,
        'meters' => $meters,
        'totals' => $totals,
        'summary' => [
            'total_meters' => intval($total),
            'with_data' => count($meters),
            'missing' => intval($total) - count($meters)
        ]
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// --- Settlement report: selected meters, full A+/A-/R+/R- for month end ---
function cmd_settlement_report($db, $month, $meter_addrs) {
    $month_end = date('Y-m-t', strtotime($month . '-01'));
    $addrs = array_map('trim', explode(',', $meter_addrs));
    $addrs = array_filter($addrs, function($a) { return preg_match('/^\d+$/', $a); });
    $addrs = array_unique($addrs);

    // Look up all requested meters
    $meter_map = []; // address => {id, serial, type, is_mtx3}
    $not_found = [];
    foreach ($addrs as $addr) {
        $stmt = $db->prepare("SELECT id, mSerialNumber, mAddress, mType FROM Meters
                              WHERE mAddress = :addr AND (DELETED = 0 OR DELETED IS NULL)");
        $stmt->bindValue(':addr', intval($addr), SQLITE3_INTEGER);
        $res = $stmt->execute();
        $row = $res->fetchArray(SQLITE3_ASSOC);
        $res->finalize();
        if ($row) {
            $tipo = get_device_tipo($row['mSerialNumber']);
            $meter_map[$addr] = [
                'id' => $row['id'],
                'serial' => $row['mSerialNumber'],
                'mType' => $row['mType'],
                'tipo' => $tipo,
                'is_mtx3' => is_mtx3($tipo),
                'type_str' => is_mtx3($tipo) ? 'MTX3' : (is_mtx1($tipo) ? 'MTX1' : 'unknown')
            ];
        } else {
            $not_found[] = $addr;
        }
    }

    $results = [];
    $totals = ['a_plus' => [], 'a_minus' => [], 'r_plus' => [], 'r_minus' => []];

    foreach ($meter_map as $addr => $info) {
        $table = $info['is_mtx3'] ? 'indicaciones_mtx3' : 'indicaciones_mtx1';
        $mid = $info['id'];

        $entry = [
            'address' => $addr,
            'serial' => $info['serial'],
            'type' => $info['type_str'],
            'fecha' => null,
            'a_plus' => null, 'a_minus' => null, 'r_plus' => null, 'r_minus' => null
        ];

        // Get pType=1 (A+) last reading <= month_end
        $sql = "SELECT Wh_t0, Wh_t1, Wh_t2, Wh_t3, fecha"
             . ($info['is_mtx3'] ? ", VARi_t0, VARi_t1, VARi_t2, VARi_t3, VARe_t0, VARe_t1, VARe_t2, VARe_t3" : "")
             . " FROM $table WHERE mid = :mid AND pType = 1 AND DATE(fecha) <= :end ORDER BY fecha DESC LIMIT 1";
        $stmt = $db->prepare($sql);
        $stmt->bindValue(':mid', $mid, SQLITE3_INTEGER);
        $stmt->bindValue(':end', $month_end, SQLITE3_TEXT);
        $res = $stmt->execute();
        $row = $res->fetchArray(SQLITE3_ASSOC);
        $res->finalize();

        if ($row) {
            $entry['fecha'] = $row['fecha'];
            $t1 = round($row['Wh_t0'] * 0.001, 1);
            $t2 = round($row['Wh_t1'] * 0.001, 1);
            $t3 = round($row['Wh_t2'] * 0.001, 1);
            $t4 = round($row['Wh_t3'] * 0.001, 1);
            $entry['a_plus'] = ['t1'=>$t1, 't2'=>$t2, 't3'=>$t3, 't4'=>$t4, 'total'=>round($t1+$t2+$t3+$t4, 1)];

            if ($info['is_mtx3']) {
                $r1 = round($row['VARi_t0'] * 0.001, 1);
                $r2 = round($row['VARi_t1'] * 0.001, 1);
                $r3 = round($row['VARi_t2'] * 0.001, 1);
                $r4 = round($row['VARi_t3'] * 0.001, 1);
                $entry['r_plus'] = ['t1'=>$r1, 't2'=>$r2, 't3'=>$r3, 't4'=>$r4, 'total'=>round($r1+$r2+$r3+$r4, 1)];

                $e1 = round($row['VARe_t0'] * 0.001, 1);
                $e2 = round($row['VARe_t1'] * 0.001, 1);
                $e3 = round($row['VARe_t2'] * 0.001, 1);
                $e4 = round($row['VARe_t3'] * 0.001, 1);
                $entry['r_minus'] = ['t1'=>$e1, 't2'=>$e2, 't3'=>$e3, 't4'=>$e4, 'total'=>round($e1+$e2+$e3+$e4, 1)];
            }
        }

        // Get pType=2 (A-) for MTX3
        if ($info['is_mtx3']) {
            $sql2 = "SELECT Wh_t0, Wh_t1, Wh_t2, Wh_t3, fecha
                     FROM $table WHERE mid = :mid AND pType = 2 AND DATE(fecha) <= :end ORDER BY fecha DESC LIMIT 1";
            $stmt2 = $db->prepare($sql2);
            $stmt2->bindValue(':mid', $mid, SQLITE3_INTEGER);
            $stmt2->bindValue(':end', $month_end, SQLITE3_TEXT);
            $res2 = $stmt2->execute();
            $row2 = $res2->fetchArray(SQLITE3_ASSOC);
            $res2->finalize();

            if ($row2) {
                $a1 = round($row2['Wh_t0'] * 0.001, 1);
                $a2 = round($row2['Wh_t1'] * 0.001, 1);
                $a3 = round($row2['Wh_t2'] * 0.001, 1);
                $a4 = round($row2['Wh_t3'] * 0.001, 1);
                $entry['a_minus'] = ['t1'=>$a1, 't2'=>$a2, 't3'=>$a3, 't4'=>$a4, 'total'=>round($a1+$a2+$a3+$a4, 1)];
            }
        }

        $results[] = $entry;

        // Accumulate totals
        foreach (['a_plus', 'a_minus', 'r_plus', 'r_minus'] as $key) {
            if ($entry[$key]) {
                foreach (['t1','t2','t3','t4','total'] as $f) {
                    if (!isset($totals[$key][$f])) $totals[$key][$f] = 0;
                    $totals[$key][$f] = round($totals[$key][$f] + $entry[$key][$f], 1);
                }
            }
        }
    }

    // Clean empty totals
    foreach ($totals as $k => $v) {
        if (empty($v)) $totals[$k] = null;
    }

    echo json_encode([
        'month' => $month,
        'month_end' => $month_end,
        'meters' => $results,
        'not_found' => $not_found,
        'totals' => $totals,
        'summary' => [
            'requested' => count($addrs),
            'found' => count($meter_map),
            'not_found' => count($not_found)
        ]
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// ============================
// MAIN
// ============================

$cmd     = isset($_GET['cmd'])   ? $_GET['cmd']   : '';
$address = isset($_GET['addr'])  ? $_GET['addr']  : '';

if (!$cmd) {
    err_exit("Missing 'cmd' parameter. Use: list, current, monthly, halfhour, schedule, request_read, bulk_request_read");
}

// --- Set StartTime for a meter (controls archive read depth) ---
if ($cmd === 'set_start_date') {
    if (!$address) err_exit("Missing 'addr' parameter");
    $start_date = isset($_GET['start_date']) ? $_GET['start_date'] : '';
    if (!$start_date) err_exit("Missing 'start_date' parameter (YYYY-MM-DD)");
    $ts = strtotime($start_date);
    if ($ts === false) err_exit("Invalid date format. Use YYYY-MM-DD");
    $start_sql = date('Y-m-d H:i:s', $ts);

    $db = new SQLite3(DB_PATH, SQLITE3_OPEN_READWRITE);
    $db->busyTimeout(5000);
    $meter = get_meter($db, $address);
    $mid = $meter['id'];

    $db->exec("UPDATE COMSchedule SET StartTime='$start_sql' WHERE mid=$mid");
    $new_st = $db->querySingle("SELECT StartTime FROM COMSchedule WHERE mid=$mid");
    $db->close();

    echo json_encode([
        'result'    => 'ok',
        'meter'     => $address,
        'startTime' => $new_st,
        'message'   => "StartTime set to $start_sql. PollReader will read archive from this date.",
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
    exit(0);
}

// Write commands handled separately (need write access)
if ($cmd === 'request_read') {
    if (!$address) err_exit("Missing 'addr' parameter");
    $flags = isset($_GET['flags']) ? $_GET['flags'] : '';
    if (!$flags) err_exit("Missing 'flags' parameter (bitmask: 1=LP, 2=MR, 3=LP+MR, 16=Ms)");
    $start_date = isset($_GET['start_date']) ? $_GET['start_date'] : null;
    cmd_request_read($address, $flags, $start_date);
    exit(0);
}

if ($cmd === 'bulk_remove_flags') {
    $addrs = isset($_GET['addrs']) ? $_GET['addrs'] : '';
    if (!$addrs) err_exit("Missing 'addrs' parameter (comma-separated addresses)");
    $flags = isset($_GET['flags']) ? $_GET['flags'] : '';
    if (!$flags) err_exit("Missing 'flags' parameter (bitmask of flags to remove)");
    cmd_bulk_remove_flags($addrs, $flags);
    exit(0);
}

if ($cmd === 'bulk_request_read') {
    $addrs = isset($_GET['addrs']) ? $_GET['addrs'] : '';
    if (!$addrs) err_exit("Missing 'addrs' parameter (comma-separated addresses)");
    $flags = isset($_GET['flags']) ? $_GET['flags'] : '';
    if (!$flags) err_exit("Missing 'flags' parameter (bitmask: 1=LP, 2=MR, 3=LP+MR, 16=Ms, 128=Sync, 243=all)");
    $start_date = isset($_GET['start_date']) ? $_GET['start_date'] : null;
    cmd_bulk_request_read($addrs, $flags, $start_date);
    exit(0);
}

if ($cmd === 'bulk_disable') {
    $addrs = isset($_GET['addrs']) ? $_GET['addrs'] : '';
    if (!$addrs) err_exit("Missing 'addrs' parameter (comma-separated addresses)");
    cmd_bulk_set_disabled($addrs, 1);
    exit(0);
}

if ($cmd === 'bulk_enable') {
    $addrs = isset($_GET['addrs']) ? $_GET['addrs'] : '';
    if (!$addrs) err_exit("Missing 'addrs' parameter (comma-separated addresses)");
    cmd_bulk_set_disabled($addrs, 0);
    exit(0);
}

if ($cmd === 'prioritize') {
    if (!$address) err_exit("Missing 'addr' parameter");
    $flags = isset($_GET['flags']) ? $_GET['flags'] : '16'; // default Ms
    cmd_prioritize($address, $flags);
    exit(0);
}

if ($cmd === 'clear_queue') {
    cmd_clear_queue();
    exit(0);
}

if ($cmd === 'remove_flags') {
    $flags = isset($_GET['flags']) ? $_GET['flags'] : '';
    if (!$flags) err_exit("Missing 'flags' parameter (bitmask of flags to remove)");
    cmd_remove_flags($flags);
    exit(0);
}

if ($cmd === 'remove_single_flag') {
    if (!$address) err_exit("Missing 'addr' parameter");
    $flag = isset($_GET['flag']) ? intval($_GET['flag']) : 0;
    if ($flag <= 0 || $flag > 0x07FF) err_exit("Invalid flag value");
    cmd_remove_single_flag($address, $flag);
    exit(0);
}

$db = open_db();

switch ($cmd) {
    case 'list':
        cmd_list($db);
        break;

    case 'current':
        if (!$address) err_exit("Missing 'addr' parameter");
        $meter = get_meter($db, $address);
        cmd_current($db, $meter);
        break;

    case 'monthly':
        if (!$address) err_exit("Missing 'addr' parameter");
        $meter = get_meter($db, $address);
        $month = isset($_GET['month']) ? $_GET['month'] : null;
        cmd_monthly($db, $meter, $month);
        break;

    case 'halfhour':
        if (!$address) err_exit("Missing 'addr' parameter");
        if (!isset($_GET['date'])) err_exit("Missing 'date' parameter (YYYY-MM-DD)");
        $meter = get_meter($db, $address);
        $date = $_GET['date'];
        $days = isset($_GET['days']) ? intval($_GET['days']) : 1;
        cmd_halfhour($db, $meter, $date, $days);
        break;

    case 'schedule':
        if (!$address) err_exit("Missing 'addr' parameter");
        $meter = get_meter($db, $address);
        cmd_schedule($db, $meter);
        break;

    case 'queue':
        cmd_queue($db);
        break;

    case 'daily_report':
        $date = isset($_GET['date']) ? $_GET['date'] : date('Y-m-d');
        cmd_daily_report($db, $date);
        break;

    case 'settlement_report':
        $month = isset($_GET['month']) ? $_GET['month'] : date('Y-m');
        $meter_list = isset($_GET['meters']) ? $_GET['meters'] : '';
        if (!$meter_list) err_exit("Missing 'meters' parameter (comma-separated addresses)");
        cmd_settlement_report($db, $month, $meter_list);
        break;

    case 'monthly_report':
        $month = isset($_GET['month']) ? $_GET['month'] : date('Y-m');
        cmd_monthly_report($db, $month);
        break;

    case 'instant_log':
        $logfile = '/tmp/meter_instant.log';
        if (file_exists($logfile)) {
            $lines = file($logfile);
            $n = isset($_GET['n']) ? intval($_GET['n']) : 80;
            echo json_encode(array('log' => array_slice($lines, -$n)), JSON_UNESCAPED_UNICODE);
        } else {
            echo json_encode(array('log' => '(no log file)', 'path' => $logfile));
        }
        break;

    case 'max_demand':
        $addr = isset($_GET['addr']) ? $_GET['addr'] : '';
        if (!$addr) err_exit("Missing 'addr' parameter");
        $meter = get_meter($db, $addr);
        $month = isset($_GET['month']) ? $_GET['month'] : date('Y-m');
        cmd_max_demand($db, $meter, $month);
        break;

    case 'events':
        $addr = isset($_GET['addr']) ? $_GET['addr'] : '';
        if (!$addr) err_exit("Missing 'addr' parameter");
        $meter = get_meter($db, $addr);
        $from = isset($_GET['from']) ? $_GET['from'] : date('Y-m-d', strtotime('-30 days'));
        $to = isset($_GET['to']) ? $_GET['to'] : date('Y-m-d');
        $type = isset($_GET['type']) ? $_GET['type'] : 'all'; // all, events, critical
        cmd_events($db, $meter, $from, $to, $type);
        break;

    default:
        err_exit("Unknown cmd: $cmd. Available: list, current, monthly, halfhour, schedule, queue, daily_report, settlement_report, max_demand, events, request_read, bulk_request_read");
}

$db->close();

// ============================================================
// Max Demand (MP)
// ============================================================
function cmd_max_demand($db, $meter, $month) {
    $mid = $meter['id'];
    $table = $meter['is_mtx3'] ? 'max_demand_dia_mtx3' : 'max_demand_dia_mtx1';

    $parts = explode('-', $month);
    $y = intval($parts[0]);
    $m = intval($parts[1]);
    $date_from = sprintf('%04d-%02d-01', $y, $m);
    $last_day = intval(date('t', mktime(0, 0, 0, $m, 1, $y)));
    $date_to = sprintf('%04d-%02d-%02d', $y, $m, $last_day);

    $sql = "SELECT * FROM $table WHERE mid = $mid
            AND fecha >= '$date_from' AND fecha <= '$date_to'
            ORDER BY fecha";
    $res = @$db->query($sql);
    if (!$res) {
        // Table may not exist
        echo json_encode([
            'meter' => $meter['mAddress'], 'serial' => $meter['mSerialNumber'],
            'type' => $meter['is_mtx3'] ? 'MTX3' : 'MTX1', 'month' => $month,
            'rows' => [], 'summary' => [], 'count' => 0,
            'warning' => "Table $table not found or query failed"
        ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
        return;
    }

    $rows = [];
    while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
        $entry = [
            'fecha'    => $row['fecha'],
            'r_fecha'  => isset($row['r_fecha']) ? $row['r_fecha'] : $row['fecha'],
        ];

        for ($t = 0; $t < 4; $t++) {
            if ($meter['is_mtx3']) {
                $hora_key = "hora_power_t$t";
            } else {
                $hora_key = "hora_t$t";
            }
            $entry["hora_t$t"]  = isset($row[$hora_key]) ? $row[$hora_key] : null;
            $entry["power_t$t"] = isset($row["max_power_t$t"]) ? floatval($row["max_power_t$t"]) : null;
        }

        if ($meter['is_mtx3']) {
            for ($t = 0; $t < 4; $t++) {
                $entry["hora_VARi_t$t"] = isset($row["hora_VARi_t$t"]) ? $row["hora_VARi_t$t"] : null;
                $entry["VARi_t$t"]      = isset($row["max_VARi_t$t"]) ? floatval($row["max_VARi_t$t"]) : null;
                $entry["hora_VARe_t$t"] = isset($row["hora_VARe_t$t"]) ? $row["hora_VARe_t$t"] : null;
                $entry["VARe_t$t"]      = isset($row["max_VARe_t$t"]) ? floatval($row["max_VARe_t$t"]) : null;
            }
        }

        $rows[] = $entry;
    }
    $res->finalize();

    $summary = [];
    for ($t = 0; $t < 4; $t++) {
        $max_val = null;
        $max_date = null;
        $max_hora = null;
        foreach ($rows as $r) {
            if ($r["power_t$t"] !== null && ($max_val === null || $r["power_t$t"] > $max_val)) {
                $max_val = $r["power_t$t"];
                $max_date = $r['fecha'];
                $max_hora = $r["hora_t$t"];
            }
        }
        $summary["t$t"] = ['max_power' => $max_val, 'date' => $max_date, 'time' => $max_hora];
    }

    echo json_encode([
        'meter'   => $meter['mAddress'],
        'serial'  => $meter['mSerialNumber'],
        'type'    => $meter['is_mtx3'] ? 'MTX3' : 'MTX1',
        'month'   => $month,
        'rows'    => $rows,
        'summary' => $summary,
        'count'   => count($rows),
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

// ============================================================
// Events (Log + Critical)
// ============================================================
function cmd_events($db, $meter, $from, $to, $type) {
    $mid = $meter['id'];
    $events = [];

    if ($type === 'all' || $type === 'events') {
        $sql = "SELECT EventTime, EVENT, ExtDataLength, ExtDATA FROM MeterEventLog
                WHERE mid = $mid
                AND EventTime >= '$from 00:00:00' AND EventTime <= '$to 23:59:59'
                GROUP BY EventTime, EVENT
                ORDER BY EventTime DESC";
        $res = @$db->query($sql);
        if ($res) {
            while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
                $events[] = [
                    'time'       => $row['EventTime'],
                    'event'      => intval($row['EVENT']),
                    'event_name' => decode_event($row['EVENT']),
                    'type'       => 'event',
                    'ext_data'   => $row['ExtDataLength'] ? bin2hex($row['ExtDATA']) : null,
                ];
            }
            $res->finalize();
        }
    }

    if ($type === 'all' || $type === 'critical') {
        $sql = "SELECT EventTime, EventType, EventCount, r_fecha FROM MeterCriticalEventLog
                WHERE mid = $mid
                AND EventTime >= '$from 00:00:00' AND EventTime <= '$to 23:59:59'
                ORDER BY EventTime DESC";
        $res = @$db->query($sql);
        if ($res) {
            while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
                $events[] = [
                    'time'       => $row['EventTime'],
                    'event'      => intval($row['EventType']),
                    'event_name' => decode_critical_event($row['EventType']),
                    'type'       => 'critical',
                    'count'      => intval($row['EventCount']),
                    'r_fecha'    => $row['r_fecha'],
                ];
            }
            $res->finalize();
        }
    }

    usort($events, function($a, $b) {
        return strcmp($b['time'], $a['time']);
    });

    echo json_encode([
        'meter'  => $meter['mAddress'],
        'serial' => $meter['mSerialNumber'],
        'type'   => $meter['is_mtx3'] ? 'MTX3' : 'MTX1',
        'from'   => $from,
        'to'     => $to,
        'events' => $events,
        'count'  => count($events),
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
}

function decode_event($code) {
    $code = intval($code);
    $map = [
        1 => 'Вкл. живлення',
        2 => 'Викл. живлення',
        3 => 'Вкл. фази A',
        4 => 'Викл. фази A',
        5 => 'Вкл. фази B',
        6 => 'Викл. фази B',
        7 => 'Вкл. фази C',
        8 => 'Викл. фази C',
        9 => 'Відкриття кришки',
       10 => 'Закриття кришки',
       11 => 'Магнітне поле',
       12 => 'Зникнення магнітного поля',
       13 => 'Перевищення потужності',
       14 => 'Зміна параметрів',
       15 => 'Корекція часу',
       16 => 'Перепрограмування',
       17 => 'Помилка EEPROM',
       18 => 'Скидання показників',
       19 => 'Зміна тарифного розкладу',
       20 => 'Відкриття кл. кришки',
       21 => 'Закриття кл. кришки',
       22 => 'Реле вкл.',
       23 => 'Реле викл.',
    ];
    return isset($map[$code]) ? $map[$code] : "Подія #$code";
}

function decode_critical_event($code) {
    $code = intval($code);
    $map = [
        1 => 'KO: пошкодження',
        2 => 'KO: маніпуляція',
        3 => 'KO: магнітне поле',
        4 => 'KO: відкриття кришки',
        5 => 'KO: втручання',
        6 => 'KO: перенапруга',
        7 => 'KO: перевищення струму',
    ];
    return isset($map[$code]) ? $map[$code] : "Критична #$code";
}
