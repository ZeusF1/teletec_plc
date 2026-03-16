<?php
/*
 * auth.inc — Session authentication middleware
 *
 * Compatible with existing HTTP Basic auth from index.php.
 * When a user logs in via index.php, $_SESSION['login'] = 1 is set.
 * This middleware checks that session variable for API endpoints.
 *
 * Usage:
 *   require_once 'auth.inc';
 *   check_auth();                        // 401 if not logged in
 *   check_permission('can_write');        // 403 if no permission
 */

/**
 * Verify that the user is authenticated via session.
 *
 * In non-strict mode (default): if no session cookie exists at all,
 * access is allowed — this supports meter_ui.html / status.html which
 * are accessed directly without going through index.php's Basic Auth.
 * If a session cookie IS present but login is invalid, returns 401.
 *
 * In strict mode: always requires a valid session (use for write operations).
 *
 * @param bool $strict  If true, always require valid session.
 */
function check_auth($strict = false) {
    $sid_name = session_name() ?: 'PHPSESSID';

    // No session cookie at all — user hasn't been through index.php
    if (!$strict && empty($_COOKIE[$sid_name])) {
        return; // Allow access without auth
    }

    if (session_status() === PHP_SESSION_NONE) {
        // read_and_close: read session data but release lock immediately
        // This prevents session file locking from blocking concurrent API requests
        @session_start(['read_and_close' => true]);
    }

    if (empty($_SESSION['login']) || $_SESSION['login'] != 1) {
        if ($strict) {
            http_response_code(401);
            header('Content-Type: application/json; charset=utf-8');
            echo json_encode(['error' => 'Unauthorized', 'code' => 401]) . "\n";
            exit;
        }
        // Non-strict: allow through
    }
}

/**
 * Check a specific permission from the session.
 * Permission names correspond to gr_users table columns (e.g., 'aparatos_attr').
 *
 * @param string $perm  The permission attribute name
 * @param int    $mask  Required bitmask (default 0x08 = View)
 */
function check_permission($perm, $mask = 0x08) {
    check_auth();

    if (empty($_SESSION[$perm]) || !(intval($_SESSION[$perm]) & $mask)) {
        http_response_code(403);
        header('Content-Type: application/json; charset=utf-8');
        echo json_encode(['error' => 'Forbidden', 'code' => 403]) . "\n";
        exit;
    }
}

/**
 * Attempt login with username and password.
 * Checks against users table. Supports MD5 (legacy) and bcrypt (new).
 * On successful MD5 login, migrates password to bcrypt.
 *
 * @param SQLite3 $db
 * @param string  $login
 * @param string  $password
 * @return array  ['success' => bool, 'error' => string|null, 'user' => array|null]
 */
function attempt_login($db, $login, $password) {
    if (session_status() === PHP_SESSION_NONE) {
        session_start();
    }

    // Brute force protection
    if (!empty($_SESSION['login_attempts']) && $_SESSION['login_attempts'] >= 5) {
        $lockout_until = $_SESSION['lockout_until'] ?? 0;
        if (time() < $lockout_until) {
            $remaining = $lockout_until - time();
            return ['success' => false, 'error' => "Account locked. Try again in {$remaining}s."];
        }
        // Lockout expired, reset
        $_SESSION['login_attempts'] = 0;
        unset($_SESSION['lockout_until']);
    }

    // Find user
    $stmt = $db->prepare("SELECT u.*, g.* FROM users u LEFT JOIN gr_users g ON g.id = u.id_grupo WHERE u.name = :name");
    $stmt->bindValue(':name', $login, SQLITE3_TEXT);
    $result = $stmt->execute();
    $user = $result->fetchArray(SQLITE3_ASSOC);
    $result->finalize();

    if (!$user) {
        increment_failed_attempts();
        return ['success' => false, 'error' => 'Invalid credentials'];
    }

    // Check if user is blocked
    if (isset($user['estado']) && $user['estado'] == 1) {
        return ['success' => false, 'error' => 'User is blocked'];
    }

    // Password verification
    $authenticated = false;

    // Try bcrypt first (if column exists and has value)
    if (!empty($user['password_bcrypt'])) {
        if (password_verify($password, $user['password_bcrypt'])) {
            $authenticated = true;
        }
    }

    // Fall back to MD5 (legacy)
    if (!$authenticated) {
        $expected_md5 = $user['password'] ?: md5('');
        if ($expected_md5 === md5($password)) {
            $authenticated = true;

            // Migrate to bcrypt
            $bcrypt_hash = password_hash($password, PASSWORD_BCRYPT);
            $upd = $db->prepare("UPDATE users SET password_bcrypt = :hash WHERE id = :id");
            if ($upd) {
                $upd->bindValue(':hash', $bcrypt_hash, SQLITE3_TEXT);
                $upd->bindValue(':id', $user['id'], SQLITE3_INTEGER);
                @$upd->execute();
            }
        }
    }

    if (!$authenticated) {
        increment_failed_attempts();
        return ['success' => false, 'error' => 'Invalid credentials'];
    }

    // Success — set session
    session_regenerate_id(true);
    $_SESSION['login'] = 1;
    $_SESSION['id_user'] = $user['id'];
    $_SESSION['id_gr_users'] = $user['id_grupo'];
    $_SESSION['login_attempts'] = 0;

    // Copy group permissions to session (same as index.php)
    foreach ($user as $f_name => $value) {
        if (strpos($f_name, '_attr') !== false) {
            $_SESSION[$f_name] = $value;
        }
    }

    return ['success' => true, 'error' => null, 'user' => ['id' => $user['id'], 'name' => $user['name']]];
}

/**
 * Increment failed login attempts and set lockout if threshold reached.
 */
function increment_failed_attempts() {
    if (!isset($_SESSION['login_attempts'])) {
        $_SESSION['login_attempts'] = 0;
    }
    $_SESSION['login_attempts']++;

    if ($_SESSION['login_attempts'] >= 5) {
        $_SESSION['lockout_until'] = time() + 300; // 5 minute lockout
    }
}

/**
 * Ensure password_bcrypt column exists in users table.
 * Safe to call multiple times.
 */
function ensure_bcrypt_column($db) {
    // Check if column exists
    $res = $db->query("PRAGMA table_info(users)");
    $has_bcrypt = false;
    while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
        if ($row['name'] === 'password_bcrypt') {
            $has_bcrypt = true;
            break;
        }
    }
    if (!$has_bcrypt) {
        $db->exec("ALTER TABLE users ADD COLUMN password_bcrypt TEXT DEFAULT NULL");
    }
}
