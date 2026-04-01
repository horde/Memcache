# Horde_Memcache

PSR-16 compliant memcache wrapper with Horde-specific extensions.

## Installation

```bash
composer require horde/memcache
```

## Quick Start

```php
use Horde\Memcache\{MemcacheApi, Config};

$config = new Config(
    hostspec: ['127.0.0.1'],
    port: [11211],
    prefix: 'myapp_'
);

$cache = new MemcacheApi($config);

// PSR-16 interface (standard)
$cache->set('key', 'value', 3600);
$value = $cache->get('key', 'default');
$cache->delete('key');

// Horde Extended interface (large items, multi-key)
$cache->setLarge('report', $hugeData, 3600);  // >1MB OK
$values = $cache->getItems(['k1', 'k2', 'k3']);
$cache->deleteDelayed('lock', 30);
```

## Two Interfaces

### PSR-16 (Standard)
`get` `set` `delete` `clear` `has` `getMultiple` `setMultiple` `deleteMultiple`

- Standard cache operations
- Items < 1MB
- Framework-portable

### HordeMemcacheInterface (Extended)
`getLarge` `setLarge` `getItems` `deleteDelayed`

- Large items (>1MB auto-chunking)
- Multi-key batch retrieval
- Delete with timeout

## Extensions Supported

Both **Memcache** and **Memcached** PHP extensions fully supported with equal features.

## Requirements

- PHP 8.1+
- memcache or memcached PHP extension
- memcached server

## Documentation

- **doc/UPGRADING.md** - Migration guide from 2.x
- **src/HordeMemcacheInterface.php** - Extended API contract
- **PSR-16**: https://www.php-fig.org/psr/psr-16/

## License

LGPL 2.1 - See LICENSE file
