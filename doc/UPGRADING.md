# Upgrading to Horde_Memcache 3.0

**Date:** 2026-04-01
**Breaking Changes:** YES - Major version
**Migration Required:** YES

---

## Overview

Horde_Memcache 3.0 introduces **PSR-16 Simple Cache interface** compliance while preserving Horde's advanced features through a separate `HordeMemcacheInterface`.

---

## Breaking Changes

### Method Renaming

| Old Method (2.x) | New PSR-16 (3.0) | New Horde Extended (3.0) |
|------------------|------------------|--------------------------|
| `get($keys)` | `get(string): mixed` | `getLarge(string): mixed` + `getItems(array): array` |
| `set($key, $val, $expire)` | `set(string, mixed, ttl): bool` | `setLarge(string, mixed, int): bool` |
| `delete($key, $timeout)` | `delete(string): bool` | `deleteDelayed(string, int): bool` |

### Behavioral Changes

**1. PSR-16 `get()` vs Old `get()`:**
```php
// OLD (2.x)
$value = $cache->get('key');  // Returns false on miss
$values = $cache->get(['k1', 'k2']);  // Multi-key supported

// NEW PSR-16 (3.0)
$value = $cache->get('key', $default);  // Returns $default on miss
// Multi-key NOT supported - use getMultiple() or getItems()

// NEW Horde Extended (3.0)
$value = $cache->getLarge('key');  // Single key with oversized support
$values = $cache->getItems(['k1', 'k2']);  // Multi-key with oversized support
```

**2. PSR-16 `set()` vs Old `set()`:**
```php
// OLD (2.x)
$cache->set('key', $largeValue, 3600);  // Auto-splits >1MB items

// NEW PSR-16 (3.0)
$cache->set('key', $largeValue, 3600);  // FAILS on >1MB items

// NEW Horde Extended (3.0)
$cache->setLarge('key', $largeValue, 3600);  // Auto-splits >1MB items
```

**3. PSR-16 `delete()` vs Old `delete()`:**
```php
// OLD (2.x)
$cache->delete('key', 10);  // Delete with 10s timeout

// NEW PSR-16 (3.0)
$cache->delete('key');  // No timeout parameter

// NEW Horde Extended (3.0)
$cache->deleteDelayed('key', 10);  // Delete with 10s timeout
```

---

## Migration Guide

### Step 1: Identify Usage Pattern

**For standard caching (< 1MB items):**
```php
// OLD (2.x)
$cache->set('user:123', $userData, 3600);
$data = $cache->get('user:123');
$cache->delete('user:123');

// NEW (3.0) - Use PSR-16 interface (no changes needed!)
$cache->set('user:123', $userData, 3600);
$data = $cache->get('user:123');
$cache->delete('user:123');
```

**For large items (> 1MB):**
```php
// OLD (2.x)
$cache->set('report:monthly', $hugeReport, 86400);
$report = $cache->get('report:monthly');

// NEW (3.0) - Use Horde Extended interface
$cache->setLarge('report:monthly', $hugeReport, 86400);
$report = $cache->getLarge('report:monthly');
```

**For multi-key retrieval:**
```php
// OLD (2.x)
$values = $cache->get(['user:1', 'user:2', 'user:3']);

// NEW (3.0) - Use Horde Extended interface
$values = $cache->getItems(['user:1', 'user:2', 'user:3']);
```

**For delayed deletion:**
```php
// OLD (2.x)
$cache->delete('lock:import', 30);

// NEW (3.0) - Use Horde Extended interface
$cache->deleteDelayed('lock:import', 30);
```

### Step 2: Search and Replace Patterns

**Pattern 1: Single-key get (check if large items needed)**
```php
// Search for:
$cache->get('literal-key')
$cache->get($variable)

// Replace with:
$cache->getLarge('literal-key')  // If need oversized support
$cache->get('literal-key')       // If PSR-16 is fine (< 1MB)
```

**Pattern 2: Array get (multi-key)**
```php
// Search for:
$cache->get($arrayVar)
$cache->get(['k1', 'k2'])

// Replace with:
$cache->getItems($arrayVar)
$cache->getItems(['k1', 'k2'])
```

**Pattern 3: Set (check if large items needed)**
```php
// Search for:
$cache->set(

// Replace with:
$cache->setLarge(  // If need oversized support
$cache->set(       // If PSR-16 is fine (< 1MB)
```

**Pattern 4: Delete with timeout**
```php
// Search for:
$cache->delete($key, $timeout)

// Replace with:
$cache->deleteDelayed($key, $timeout)
```

**Pattern 5: Delete without timeout**
```php
// Search for:
$cache->delete($key)

// Replace with:
$cache->delete($key)  // PSR-16, no change needed
```

---

## Two Interfaces: When to Use Which?

### Use PSR-16 Interface (Standard)

**Methods:** `get`, `set`, `delete`, `clear`, `has`, `getMultiple`, `setMultiple`, `deleteMultiple`

**When:**
- Standard cache operations (most use cases)
- Items are < 1MB
- Single-key operations
- Need framework portability
- Working with PSR-16-aware libraries

**Example:**
```php
use Psr\SimpleCache\CacheInterface;

function cacheData(CacheInterface $cache, string $key, mixed $data): void
{
    $cache->set($key, $data, 3600);
}
```

### Use HordeMemcacheInterface (Extended)

**Methods:** `getLarge`, `setLarge`, `getItems`, `deleteDelayed`

**When:**
- Need large item support (> 1MB auto-chunking)
- Need multi-key batch retrieval
- Need delete with timeout (blocks add() for N seconds)
- Horde-specific features required

**Example:**
```php
use Horde\Memcache\HordeMemcacheInterface;

function cacheLargeReport(HordeMemcacheInterface $cache, array $report): void
{
    $cache->setLarge('report:monthly', $report, 86400);
}
```

### Using Both Interfaces

```php
use Horde\Memcache\MemcacheApi;
use Horde\Memcache\Config;

$config = new Config(
    hostspec: ['127.0.0.1'],
    port: [11211],
    prefix: 'myapp_'
);

$cache = new MemcacheApi($config);

// Use PSR-16 for standard operations
$cache->set('user:123', $userData, 3600);

// Use Horde Extended for large items
$cache->setLarge('report:monthly', $hugeReport, 86400);

// Use Horde Extended for multi-key
$users = $cache->getItems(['user:1', 'user:2', 'user:3']);
```

---

## New Features in 3.0

### PSR-16 Compliance

```php
use Psr\SimpleCache\CacheInterface;

$cache = new \Horde\Memcache\MemcacheApi($config);
assert($cache instanceof CacheInterface);  // ✓ true

// Standard PSR-16 methods
$cache->set('key', 'value', 3600);
$value = $cache->get('key', 'default');
$cache->delete('key');
$cache->clear();
$exists = $cache->has('key');

// Batch operations
$cache->setMultiple(['k1' => 'v1', 'k2' => 'v2'], 3600);
$values = $cache->getMultiple(['k1', 'k2'], 'default');
$cache->deleteMultiple(['k1', 'k2']);

// DateInterval TTL support
$cache->set('key', 'value', new \DateInterval('PT1H'));  // 1 hour
```

### Split `get()` Feature

Old `get()` combined two features - now cleanly separated:

**Single-key with oversized:** `getLarge()`
```php
$cache->setLarge('key', $largeObject, 3600);
$object = $cache->getLarge('key');  // Automatic reassembly
```

**Multi-key retrieval:** `getItems()`
```php
$values = $cache->getItems(['k1', 'k2', 'k3']);
// Returns: ['k1' => value1, 'k2' => value2, 'k3' => false]
```

### Full PHP Extension Support

Both Memcache and Memcached extensions now have complete feature parity:

| Feature | Memcache Extension | Memcached Extension | Implementation |
|---------|-------------------|---------------------|----------------|
| PSR-16 operations | Supported | Supported | Both extensions |
| Large items (>1MB) | Supported | Supported | Memcache: flags; Memcached: metadata+chunks |
| Delete with timeout | Supported | Supported | Memcache: native; Memcached: set(null, ttl) |
| Multi-key operations | Supported | Supported | Both extensions |

**Implementation Details:**
- **Memcache extension**: Uses flags parameter for large item detection and native delete timeout
- **Memcached extension**: Uses metadata keys (`:meta`) + chunk keys (`:chunk:N`) for large items; uses `set(key, null, ttl)` for delete with timeout

---

## Testing Strategy

### Running Tests

```bash
# Unit tests only (default) - no memcached server required
phpunit
# or
phpunit --testsuite=unit

# Integration tests - requires memcached server
phpunit --testsuite=integration
```

### Test Your Migration

```php
// Test PSR-16 interface
assert($cache instanceof \Psr\SimpleCache\CacheInterface);
$cache->set('test', 'value', 60);
assert($cache->get('test') === 'value');

// Test Horde Extended interface
assert($cache instanceof \Horde\Memcache\HordeMemcacheInterface);
$largeData = str_repeat('x', 1024 * 1024 + 1000);
$cache->setLarge('large', $largeData, 60);
assert($cache->getLarge('large') === $largeData);

// Test multi-key
$cache->setLarge('k1', 'v1', 60);
$cache->setLarge('k2', 'v2', 60);
$values = $cache->getItems(['k1', 'k2']);
assert($values === ['k1' => 'v1', 'k2' => 'v2']);
```

---

## Common Migration Scenarios

### Scenario 1: Simple Key-Value Cache

**Before (2.x):**
```php
$cache->set('user:' . $userId, $userData, 3600);
$userData = $cache->get('user:' . $userId);
```

**After (3.0):**
```php
// No changes needed! PSR-16 interface works the same
$cache->set('user:' . $userId, $userData, 3600);
$userData = $cache->get('user:' . $userId);
```

### Scenario 2: Large Object Caching

**Before (2.x):**
```php
$cache->set('report:monthly', $hugeReport, 86400);
$report = $cache->get('report:monthly');
```

**After (3.0):**
```php
$cache->setLarge('report:monthly', $hugeReport, 86400);
$report = $cache->getLarge('report:monthly');
```

### Scenario 3: Batch Retrieval

**Before (2.x):**
```php
$keys = ['user:1', 'user:2', 'user:3'];
$users = $cache->get($keys);
```

**After (3.0):**
```php
$keys = ['user:1', 'user:2', 'user:3'];
$users = $cache->getItems($keys);
```

### Scenario 4: Lock Management

**Before (2.x):**
```php
$cache->set('lock:import', true, 300);
// ... do work ...
$cache->delete('lock:import', 30);  // Block for 30s
```

**After (3.0):**
```php
$cache->set('lock:import', true, 300);
// ... do work ...
$cache->deleteDelayed('lock:import', 30);  // Block for 30s
```

---

## Configuration Changes

### Config Object (Recommended)

```php
use Horde\Memcache\Config;
use Horde\Memcache\MemcacheApi;
use Psr\Log\NullLogger;

// New Config object approach (recommended)
$config = new Config(
    hostspec: ['127.0.0.1', '127.0.0.2'],
    port: [11211, 11211],
    prefix: 'myapp_',
    largeItems: true,  // Enable large item support
    compression: true,
    persistent: false
);

$cache = new MemcacheApi($config, new NullLogger());
```

### Array Config (Still Supported)

```php
// Legacy array config still works
$config = [
    'hostspec' => ['127.0.0.1'],
    'port' => [11211],
    'prefix' => 'myapp_',
];

$cache = new MemcacheApi($config);
```

---

## Troubleshooting

### Issue: "Call to undefined method get()"

**Cause:** Trying to use old multi-key `get(['k1', 'k2'])` syntax

**Solution:**
```php
// Change from:
$values = $cache->get(['k1', 'k2']);

// To:
$values = $cache->getItems(['k1', 'k2']);
```

### Issue: "Item too large" or silent failures

**Cause:** Using PSR-16 `set()` for items > 1MB

**Solution:**
```php
// Change from:
$cache->set('key', $largeData, 3600);

// To:
$cache->setLarge('key', $largeData, 3600);
```

### Issue: "Too few arguments to function delete()"

**Cause:** Passing timeout parameter to PSR-16 `delete()`

**Solution:**
```php
// Change from:
$cache->delete('key', 30);

// To:
$cache->deleteDelayed('key', 30);
```

---

## Performance Considerations

### PSR-16 vs Horde Extended

- **PSR-16 methods**: Lightweight, no overhead, best for < 1MB items
- **Horde Extended methods**: Minimal overhead for < 1MB, automatic chunking for > 1MB

### Memcache vs Memcached Extension

Both extensions have equal performance in 3.0:

- **Large items**: Memcached uses extra metadata key (+1 round trip on write)
- **Delete timeout**: Memcached uses set-null workaround (same performance)
- **Normal operations**: Identical performance

---

## Support and Resources

### Documentation

- **README.md** - Quick start guide
- **UPGRADING.md** - This file
- **src/HordeMemcacheInterface.php** - Horde Extended API contract
- **Psr\SimpleCache\CacheInterface** - PSR-16 specification

### Getting Help

- **GitHub Issues**: https://github.com/horde/Memcache/issues
- **Horde Mailing List**: dev@lists.horde.org

### Related Standards

- **PSR-16**: https://www.php-fig.org/psr/psr-16/
- **PSR-6**: https://www.php-fig.org/psr/psr-6/ (future compatibility via `getItems()`)

---

**Migration effort:** Low for standard usage, medium for advanced features.

