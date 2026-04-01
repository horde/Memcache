<?php

/**
 * Copyright 2007-2026 Horde LLC (http://www.horde.org/)
 *
 * See the enclosed file LICENSE for license information (LGPL). If you
 * did not receive this file, see http://www.horde.org/licenses/lgpl21.
 *
 * @author   Jan Schneider <jan@horde.org>
 * @author   Michael Slusarz <slusarz@horde.org>
 * @author   Didi Rieder <adrieder@sbox.tugraz.at>
 * @category Horde
 * @license  http://www.horde.org/licenses/lgpl21 LGPL 2.1
 * @package  Memcache
 */
declare(strict_types=1);

namespace Horde\Memcache;

use Horde\Memcache\Exception\ConnectionException;
use Horde\Memcache\Exception\DeserializationException;
use Horde\Memcache\Exception\SerializationException;
use Horde\Memcache\HordeMemcacheInterface;
use Memcache;
use Memcached;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Psr\SimpleCache\CacheInterface;
use Serializable;
use DateInterval;
use DateTimeImmutable;
use Throwable;

/**
 * This class provides an API or Horde code to interact with a centrally
 * configured memcache installation.
 *
 * Implements two disjunct interfaces:
 * - PSR-16 SimpleCache\CacheInterface (standard cache operations)
 * - HordeMemcacheInterface (Horde-specific extensions)
 *
 * memcached website: http://www.danga.com/memcached/
 *
 * @author   Jan Schneider <jan@horde.org>
 * @author   Michael Slusarz <slusarz@horde.org>
 * @author   Didi Rieder <adrieder@sbox.tugraz.at>
 * @category Horde
 * @license  http://www.horde.org/licenses/lgpl21 LGPL 2.1
 * @package  Memcache
 */
class MemcacheApi implements Serializable, CacheInterface, HordeMemcacheInterface
{
    /**
     * The number of bits reserved by PHP's memcache layer for internal flag
     * use.
     */
    public const FLAGS_RESERVED = 16;

    /**
     * Locking timeout.
     */
    public const LOCK_TIMEOUT = 30;

    /**
     * Suffix added to key to create the lock entry.
     */
    public const LOCK_SUFFIX = '_l';

    /**
     * The max storage size of the memcache server.  This should be slightly
     * smaller than the actual value due to overhead.  By default, the max
     * slab size of memcached (as of 1.1.2) is 1 MB.
     */
    public const MAX_SIZE = 1000000;

    /**
     * Serializable version.
     */
    public const VERSION = 1;

    /**
     * Locked keys.
     */
    protected array $locks = [];

    /**
     * Logger instance.
     */
    protected LoggerInterface $logger;

    /**
     * Memcache object.
     */
    protected Memcache|Memcached $memcache;

    /**
     * A list of items known not to exist.
     */
    protected array $noexist = [];

    /**
     * Memcache defaults.
     */
    protected array $params = [
        'compression' => false,
        'hostspec' => ['localhost'],
        'large_items' => true,
        'persistent' => false,
        'port' => [11211],
        'prefix' => 'horde',
    ];

    /**
     * The list of active servers.
     */
    protected array $servers = [];

    /**
     * Constructor.
     *
     * @param Config|array $config  Configuration object or legacy array.
     * @param LoggerInterface $logger  PSR-3 logger instance.
     *
     * @throws ConnectionException
     */
    public function __construct(Config|array $config = new Config(), LoggerInterface $logger = new NullLogger())
    {
        $this->logger = $logger;

        // Convert array to Config if needed (legacy compatibility)
        if (is_array($config)) {
            $config = Config::fromArray($config);
        }

        $this->params = $config->toArray();
        $this->init();
    }

    /**
     * Do initialization.
     *
     * @throws MemcacheException
     */
    public function init(): void
    {
        if (class_exists('Memcached')) {
            if (empty($this->params['persistent'])) {
                $this->memcache = new Memcached();
            } else {
                $this->memcache = new Memcached('hordememcache');
            }
            $this->memcache->setOptions([
                Memcached::OPT_COMPRESSION => $this->params['compression'],
                Memcached::OPT_DISTRIBUTION => Memcached::DISTRIBUTION_CONSISTENT,
                Memcached::OPT_HASH => Memcached::HASH_MD5,
                Memcached::OPT_LIBKETAMA_COMPATIBLE => true,
                Memcached::OPT_PREFIX_KEY => $this->params['prefix'],
            ]);
        } else {
            // Force consistent hashing
            ini_set('memcache.hash_strategy', 'consistent');
            $this->memcache = new Memcache();
        }

        for ($i = 0, $n = count($this->params['hostspec']); $i < $n; ++$i) {
            if ($this->memcache instanceof Memcached) {
                $res = $this->memcache->addServer(
                    $this->params['hostspec'][$i],
                    empty($this->params['port'][$i]) ? 0 : $this->params['port'][$i],
                    !empty($this->params['weight'][$i]) ? $this->params['weight'][$i] : 0
                );
            } else {
                $res = $this->memcache->addServer(
                    $this->params['hostspec'][$i],
                    empty($this->params['port'][$i]) ? 0 : $this->params['port'][$i],
                    !empty($this->params['persistent']),
                    !empty($this->params['weight'][$i]) ? $this->params['weight'][$i] : 1,
                    1,
                    15,
                    true,
                    [$this, 'failover']
                );
            }

            if ($res) {
                $this->servers[] = $this->params['hostspec'][$i] . (!empty($this->params['port'][$i]) ? ':' . $this->params['port'][$i] : '');
            }
        }

        /* Check if any of the connections worked. */
        if (empty($this->servers)) {
            $this->logger->critical('Could not connect to any memcache servers', [
                'hostspec' => $this->params['hostspec'],
                'port' => $this->params['port'] ?? [],
            ]);
            throw new ConnectionException(
                'Could not connect to any defined memcache servers.',
                $this->params['hostspec']
            );
        }

        if ($this->memcache instanceof Memcache
            && !empty($this->params['c_threshold'])) {
            $this->memcache->setCompressThreshold($this->params['c_threshold']);
        }

        $this->logger->info('Connected to memcache servers', [
            'servers' => $this->servers,
            'backend' => get_class($this->memcache),
            'persistent' => $this->params['persistent'],
        ]);
    }

    /**
     * Shutdown function.
     */
    public function shutdown(): void
    {
        foreach (array_keys($this->locks) as $key) {
            $this->unlock($key);
        }
    }

    /**
     * Delete with timeout (delayed deletion).
     *
     * @implements HordeMemcacheInterface
     *
     * Horde Extended API: Blocks add() operations on this key for the
     * specified timeout period after deletion.
     *
     * Implementation:
     * - Memcache extension: Uses native delete($key, $timeout)
     * - Memcached extension: Sets key to null with TTL (workaround)
     *
     * @param string $key       The key.
     * @param int $timeout      Timeout in seconds (blocks add() for this duration).
     *
     * @return bool  True on success.
     */
    public function deleteDelayed(string $key, int $timeout = 0): bool
    {
        if (isset($this->noexist[$key])) {
            return false;
        }

        if ($this->memcache instanceof Memcached) {
            // Memcached doesn't support delete timeout, so we set the key
            // to null with a TTL as a workaround to block the key
            if ($timeout > 0) {
                return $this->memcache->set($this->_key($key), null, $timeout);
            }
            return $this->memcache->delete($this->_key($key));
        }

        // Memcache extension supports native timeout
        return $this->memcache->delete($this->_key($key), $timeout);
    }

    // ============================================================
    // PSR-16 Simple Cache Interface
    // ============================================================

    /**
     * Fetches a value from the cache.
     *
     * @implements CacheInterface (PSR-16)
     *
     * PSR-16: Returns default value on miss. Does NOT support oversized items.
     * For large item support, use getLarge() instead.
     *
     * @param string $key      The unique key of this item in the cache.
     * @param mixed  $default  Default value to return if the key does not exist.
     *
     * @return mixed  The value of the item from the cache, or $default in case of cache miss.
     */
    public function get(string $key, mixed $default = null): mixed
    {
        $result = $this->fetchSingleStandard($key);
        return $result === false ? $default : $result;
    }

    /**
     * Persists data in the cache, uniquely referenced by a key with an optional expiration TTL time.
     *
     * @implements CacheInterface (PSR-16)
     *
     * PSR-16: Fails on items >1MB (no automatic splitting).
     * For large item support, use setLarge() instead.
     *
     * @param string                $key    The key of the item to store.
     * @param mixed                 $value  The value of the item to store.
     * @param null|int|DateInterval $ttl   Optional. The TTL value of this item.
     *
     * @return bool  True on success and false on failure.
     */
    public function set(string $key, mixed $value, int|DateInterval|null $ttl = null): bool
    {
        $expire = $this->convertTtl($ttl);
        $serialized = $this->serializeValue($value, $key);

        // PSR-16 version does NOT handle oversized items
        if (strlen($serialized) > self::MAX_SIZE) {
            return false;
        }

        $mc_key = $this->_key($key);
        $result = $this->memcache->set($mc_key, $serialized, $expire);

        if ($result !== false) {
            unset($this->noexist[$key]);
        }

        return $result !== false;
    }

    /**
     * Delete an item from the cache by its unique key.
     *
     * @implements CacheInterface (PSR-16)
     *
     * PSR-16: No timeout parameter. For delayed deletion, use deleteDelayed() instead.
     *
     * @param string $key  The unique cache key of the item to delete.
     *
     * @return bool  True if the item was successfully removed. False if there was an error.
     */
    public function delete(string $key): bool
    {
        return $this->deleteDelayed($key, 0);
    }

    /**
     * Wipes clean the entire cache's keys.
     *
     * @implements CacheInterface (PSR-16)
     *
     * @return bool  True on success and false on failure.
     */
    public function clear(): bool
    {
        $this->flush();
        return true;
    }

    /**
     * Obtains multiple cache items by their unique keys.
     *
     * @implements CacheInterface (PSR-16)
     *
     * PSR-16: Returns iterable with default values for missing keys.
     * Does NOT support oversized items. For large item support, use getItems() instead.
     *
     * @param iterable $keys     A list of keys that can obtained in a single operation.
     * @param mixed    $default  Default value to return for keys that do not exist.
     *
     * @return iterable  A list of key => value pairs.
     */
    public function getMultiple(iterable $keys, mixed $default = null): iterable
    {
        $results = [];
        foreach ($keys as $key) {
            $results[$key] = $this->get($key, $default);
        }
        return $results;
    }

    /**
     * Persists a set of key => value pairs in the cache, with an optional TTL.
     *
     * @implements CacheInterface (PSR-16)
     *
     * PSR-16: Fails on items >1MB (no automatic splitting).
     *
     * @param iterable              $values  A list of key => value pairs for a multiple-set operation.
     * @param null|int|DateInterval $ttl    Optional. The TTL value of this item.
     *
     * @return bool  True on success and false on failure.
     */
    public function setMultiple(iterable $values, int|DateInterval|null $ttl = null): bool
    {
        $success = true;
        foreach ($values as $key => $value) {
            $success = $this->set($key, $value, $ttl) && $success;
        }
        return $success;
    }

    /**
     * Deletes multiple cache items in a single operation.
     *
     * @implements CacheInterface (PSR-16)
     *
     * @param iterable $keys  A list of string-based keys to be deleted.
     *
     * @return bool  True if the items were successfully removed. False if there was an error.
     */
    public function deleteMultiple(iterable $keys): bool
    {
        $success = true;
        foreach ($keys as $key) {
            $success = $this->delete($key) && $success;
        }
        return $success;
    }

    /**
     * Determines whether an item is present in the cache.
     *
     * @implements CacheInterface (PSR-16)
     *
     * @param string $key  The cache item key.
     *
     * @return bool
     */
    public function has(string $key): bool
    {
        return $this->fetchSingleStandard($key) !== false;
    }

    /**
     * Get single item with large item support.
     *
     * @implements HordeMemcacheInterface
     *
     * Horde Extended API: Mirrors setLarge() - automatically reassembles
     * items >1MB that were split across multiple keys (key, key_s1, key_s2, etc.).
     *
     * @param string $key  Cache key.
     *
     * @return mixed  Value or false on miss.
     */
    public function getLarge(string $key): mixed
    {
        $result = $this->getItems([$key]);
        return $result[$key];
    }

    /**
     * Get multiple items (PSR-6 compatible signature).
     *
     * @implements HordeMemcacheInterface
     *
     * Horde Extended API: Returns array of raw values for given keys.
     * Each value supports large item reassembly.
     *
     * Signature compatible with PSR-6 getItems() but returns raw values
     * instead of CacheItemInterface objects. Can be used as building block
     * for PSR-6 adapter.
     *
     * @param array $keys  Array of cache keys.
     *
     * @return array  ['key1' => value1, 'key2' => value2, 'key3' => false]
     */
    public function getItems(array $keys): array
    {
        $flags = null;
        $key_map = $missing_parts = $os = $out_array = [];

        foreach ($keys as $v) {
            $key_map[$v] = (string) $this->_key($v);
        }

        if ($this->memcache instanceof Memcached) {
            $res = $this->memcache->getMulti(array_values($key_map));
        } else {
            $res = $this->memcache->get(array_values($key_map), $flags);
        }
        if ($res === false) {
            // Return array with all keys as false
            return array_fill_keys($keys, false);
        }

        /* Check to see if we have any oversize items we need to get. */
        if (!empty($this->params['large_items'])) {
            if ($this->memcache instanceof Memcached) {
                // Memcached: Check for metadata keys to detect chunked items
                $meta_keys = [];
                foreach ($keys as $key) {
                    $meta_keys[$key] = $this->_key($key . ':meta');
                }
                $meta_res = $this->memcache->getMulti(array_values($meta_keys));

                // Process chunked items
                if ($meta_res !== false) {
                    foreach ($keys as $key) {
                        $meta_key = $meta_keys[$key];
                        if (isset($meta_res[$meta_key]) && is_int($meta_res[$meta_key])) {
                            // This is a chunked item
                            $chunk_count = $meta_res[$meta_key];
                            $data = '';

                            // Fetch all chunks
                            $chunk_keys = [];
                            for ($i = 0; $i < $chunk_count; ++$i) {
                                $chunk_keys[] = $this->_key($key . ':chunk:' . $i);
                            }
                            $chunks = $this->memcache->getMulti($chunk_keys);

                            if ($chunks === false) {
                                // Chunk retrieval failed - delete corrupted item
                                $this->deleteDelayed($key, 0);
                                $this->noexist[$key] = true;
                                continue;
                            }

                            // Reassemble chunks
                            for ($i = 0; $i < $chunk_count; ++$i) {
                                $chunk_key = $this->_key($key . ':chunk:' . $i);
                                if (!isset($chunks[$chunk_key])) {
                                    // Missing chunk - delete corrupted item
                                    $this->deleteDelayed($key, 0);
                                    $this->noexist[$key] = true;
                                    continue 2;
                                }
                                $data .= $chunks[$chunk_key];
                            }

                            // Store reassembled data
                            $res[$key_map[$key]] = $data;
                        }
                    }
                }
            } else {
                // Memcache: Use flags-based approach (original implementation)
                foreach ($key_map as $key => $val) {
                    $part_count = isset($flags[$val])
                        ? ($flags[$val] >> self::FLAGS_RESERVED) - 1
                        : -1;

                    switch ($part_count) {
                        case -1:
                            /* Ignore. */
                            unset($res[$val]);
                            break;

                        case 0:
                            /* Not an oversize part. */
                            break;

                        default:
                            $os[$key] = $this->_getOSKeyArray($key, $part_count);
                            foreach ($os[$key] as $val2) {
                                $missing_parts[] = $key_map[$val2] = $this->_key($val2);
                            }
                            break;
                    }
                }

                if (!empty($missing_parts)) {
                    if (($res2 = $this->memcache->get($missing_parts)) === false) {
                        // Return array with all keys as false
                        return array_fill_keys($keys, false);
                    }

                    /* $res should now contain the same results as if we had
                     * run a single get request with all keys above. */
                    $res = array_merge($res, $res2);
                }
            }
        }

        foreach ($key_map as $k => $v) {
            if (!isset($res[$v])) {
                $this->noexist[$k] = true;
            }
        }

        foreach ($keys as $k) {
            $out_array[$k] = false;
            if (isset($res[$key_map[$k]])) {
                $data = $res[$key_map[$k]];
                if (isset($os[$k])) {
                    foreach ($os[$k] as $v) {
                        if (isset($res[$key_map[$v]])) {
                            $data .= $res[$key_map[$v]];
                        } else {
                            $this->deleteDelayed($k, 0);
                            continue 2;
                        }
                    }
                }
                $out_array[$k] = $this->unserializeValue($data, $k);
            } elseif (isset($os[$k]) && !isset($res[$key_map[$k]])) {
                $this->deleteDelayed($k, 0);
            }
        }

        return $out_array;
    }

    /**
     * Set the value of a key.
     *
     * @see Memcache::set()
     *
     * @param string $key       The key.
     * @param mixed $var       The data to store.
     * @param int $expire  Expiration time in seconds.
     *
     * @return bool  True on success.
     */
    /**
     * Set item with large item support.
     *
     * @implements HordeMemcacheInterface
     *
     * Horde Extended API: Automatically splits items >1MB into multiple keys
     * using flag bits to track part count for reassembly by getLarge().
     *
     * @param string $key     The cache key.
     * @param mixed $var      The data to store.
     * @param int $expire     Expiration time in seconds (0 = no expiration).
     *
     * @return bool  True on success.
     */
    public function setLarge(string $key, $var, int $expire = 0): bool
    {
        return $this->_set($key, $this->serializeValue($var, $key), $expire);
    }

    /**
     * Set the value of a key.
     *
     * @param string $key       The key.
     * @param string $var       The data to store (serialized).
     * @param int $expire  Expiration time in seconds.
     * @param ?int $len     String length of $len.
     *
     * @return bool  True on success.
     */
    protected function _set(string $key, $var, int $expire = 0, ?int $len = null): bool
    {
        $res = false;
        if (is_null($len)) {
            $len = strlen($var);
        }

        if (empty($this->params['large_items']) && ($len > self::MAX_SIZE)) {
            return false;
        }

        if ($this->memcache instanceof Memcached) {
            // Memcached: Use metadata + chunks approach
            if ($len <= self::MAX_SIZE) {
                // Small item - single set
                $res = $this->memcache->set($this->_key($key), $var, $expire);
                if ($res !== false) {
                    unset($this->noexist[$key]);
                }
                return $res;
            }

            // Large item - split into chunks
            $chunk_count = (int) ceil($len / self::MAX_SIZE);

            // Store metadata with chunk count
            $res = $this->memcache->set(
                $this->_key($key . ':meta'),
                $chunk_count,
                $expire
            );
            if ($res === false) {
                return false;
            }

            // Store each chunk
            for ($i = 0; $i < $chunk_count; ++$i) {
                $chunk = substr($var, $i * self::MAX_SIZE, self::MAX_SIZE);
                $res = $this->memcache->set(
                    $this->_key($key . ':chunk:' . $i),
                    $chunk,
                    $expire
                );
                if ($res === false) {
                    // Cleanup on failure
                    $this->memcache->delete($this->_key($key . ':meta'));
                    for ($j = 0; $j < $i; ++$j) {
                        $this->memcache->delete($this->_key($key . ':chunk:' . $j));
                    }
                    return false;
                }
            }
            unset($this->noexist[$key]);
            return true;
        }

        // Memcache: Use flags-based approach (original implementation)
        for ($i = 0; ($i * self::MAX_SIZE) < $len; ++$i) {
            $curr_key = $i ? ($key . '_s' . $i) : $key;
            $res = $this->memcache->set(
                $this->_key($curr_key),
                substr($var, $i * self::MAX_SIZE, self::MAX_SIZE),
                $this->_getFlags($i ? 0 : ceil($len / self::MAX_SIZE)),
                $expire
            );
            if ($res === false) {
                $this->deleteDelayed($key, 0);
                break;
            }
            unset($this->noexist[$curr_key]);
        }

        return $res;
    }

    /**
     * Replace the value of a key.
     *
     * @see Memcache::replace()
     *
     * @param string $key       The key.
     * @param mixed $var       The data to store.
     * @param int $expire  Expiration time in seconds.
     *
     * @return bool  True on success, false if key doesn't exist.
     */
    public function replace(string $key, $var, int $expire = 0): bool
    {
        $var = $this->serializeValue($var, $key);
        $len = strlen($var);

        if ($len > self::MAX_SIZE) {
            if (!empty($this->params['large_items'])
                && $this->memcache->get($this->_key($key))) {
                return $this->_set($key, $var, $expire, $len);
            }
            return false;
        }

        return $this->memcache instanceof Memcached
            ? $this->memcache->replace($key, $var, $expire)
            : $this->memcache->replace(
                $this->_key($key),
                $var,
                $this->_getFlags(1),
                $expire
            );
    }

    /**
     * Obtain lock on a key.
     *
     * @param string $key  The key to lock.
     */
    public function lock(string $key): void
    {
        $i = 0;

        while ($this->_lockAdd($key) === false) {
            usleep(min(pow(2, $i++) * 10000, 100000));
        }

        /* Register a shutdown handler function here to catch cases where PHP
         * suffers a fatal error. Must be done via shutdown function, since
         * a destructor will not be called in this case.
         * Only trigger on error, since we must assume that the code that
         * locked will also handle unlocks (which may occur in the destruct
         * phase, e.g. session handling).
         * @todo: $this is not usable in closures until PHP 5.4+ */
        if (empty($this->locks)) {
            $self = $this;
            register_shutdown_function(function () use ($self) {
                $e = error_get_last();
                if ($e['type'] & E_ERROR) {
                    /* Try to do cleanup at very end of shutdown methods. */
                    register_shutdown_function([$self, 'shutdown']);
                }
            });
        }

        $this->locks[$key] = true;
    }

    /**
     * Small wrapper around Memcache[d]#add().
     *
     * @param string $key  The key to lock.
     *
     * @return bool  True if lock acquired, false otherwise.
     */
    protected function _lockAdd(string $key): bool
    {
        if ($this->memcache instanceof Memcached) {
            return $this->memcache->add(
                $this->_key($key . self::LOCK_SUFFIX),
                1,
                self::LOCK_TIMEOUT
            );
        } else {
            return $this->memcache->add(
                $this->_key($key . self::LOCK_SUFFIX),
                1,
                0,
                self::LOCK_TIMEOUT
            );
        }
    }

    /**
     * Release lock on a key.
     *
     * @param string $key  The key to lock.
     */
    public function unlock(string $key): void
    {
        $this->memcache->delete($this->_key($key . self::LOCK_SUFFIX), 0);
        unset($this->locks[$key]);
    }

    /**
     * Mark all entries on a memcache installation as expired.
     */
    public function flush(): void
    {
        $this->memcache->flush();
    }

    /**
     * Get the statistics output from the current memcache pool.
     *
     * @return array  The output from Memcache::getExtendedStats() using the
     *                current configuration values.
     */
    public function stats(): array
    {
        return $this->memcache instanceof Memcached
            ? $this->memcache->getStats()
            : $this->memcache->getExtendedStats();
    }

    /**
     * Failover method.
     *
     * @see Memcache::addServer()
     *
     * @param string $host   Hostname.
     * @param integer $port  Port.
     *
     * @throws ConnectionException
     */
    public function failover(string $host, int $port): void
    {
        $pos = array_search($host . ':' . $port, $this->servers);
        if ($pos !== false) {
            unset($this->servers[$pos]);
            if (!count($this->servers)) {
                $this->logger->critical('All memcache servers failed', [
                    'last_server' => $host . ':' . $port,
                ]);
                throw new ConnectionException(
                    'Could not connect to any defined memcache servers.',
                    $this->params['hostspec']
                );
            }
        }
    }

    /**
     * Obtains the md5 sum for a key.
     *
     * @param string $key  The key.
     *
     * @return string  The corresponding memcache key.
     */
    protected function _key(string $key): string
    {
        return $this->memcache instanceof Memcached
            ? $key
            : hash('md5', $this->params['prefix'] . $key);
    }

    /**
     * Returns the key listing of all key IDs for an oversized item.
     *
     * @param string $key The cache item key
     * @param int $length
     * @return array  The array of key IDs.
     */
    protected function _getOSKeyArray(string $key, int $length): array
    {
        $ret = [];
        for ($i = 0; $i < $length; ++$i) {
            $ret[] = $key . '_s' . ($i + 1);
        }
        return $ret;
    }

    /**
     * Get flags for memcache call.
     *
     * @param int $count
     *
     * @return int
     */
    protected function _getFlags(int $count): int
    {
        $flags = empty($this->params['compression'])
            ? 0
            : MEMCACHE_COMPRESSED;
        return ($flags | $count << self::FLAGS_RESERVED);
    }

    /**
     * Serialize a value for storage.
     *
     * Handles errors properly and logs failures.
     *
     * @param mixed $value  Value to serialize.
     * @param string $key  Cache key (for error reporting).
     *
     * @return string  Serialized value.
     *
     * @throws SerializationException
     */
    protected function serializeValue(mixed $value, string $key): string
    {
        try {
            return serialize($value);
        } catch (Throwable $e) {
            $this->logger->error('Serialization failed', [
                'key' => $key,
                'type' => get_debug_type($value),
                'exception' => $e->getMessage(),
            ]);
            throw new SerializationException(
                "Failed to serialize value for key '{$key}'",
                $key,
                $value,
                $e
            );
        }
    }

    /**
     * Unserialize a value from storage.
     *
     * Handles errors properly and logs failures.
     *
     * @param string $data  Serialized data.
     * @param string $key  Cache key (for error reporting).
     *
     * @return mixed  Unserialized value.
     *
     * @throws DeserializationException
     */
    protected function unserializeValue(string $data, string $key): mixed
    {
        try {
            // Security: Allow all classes for backward compatibility
            // Consider restricting with allowed_classes in future
            return unserialize($data, ['allowed_classes' => true]);
        } catch (Throwable $e) {
            $this->logger->warning('Deserialization failed', [
                'key' => $key,
                'dataLength' => strlen($data),
                'exception' => $e->getMessage(),
            ]);
            throw new DeserializationException(
                "Failed to unserialize value for key '{$key}'",
                $key,
                $data,
                $e
            );
        }
    }

    /* Serializable methods. */

    /**
     * Serialize (magic method for PHP 7.4+).
     *
     * @return array  Data to serialize.
     */
    public function __serialize(): array
    {
        return [
            self::VERSION,
            $this->params,
        ];
    }

    /**
     * Unserialize (magic method for PHP 7.4+).
     *
     * @param array $data  Serialized data.
     *
     * @throws MemcacheException
     */
    public function __unserialize(array $data): void
    {
        if (!is_array($data)
            || !isset($data[0])
            || ($data[0] != self::VERSION)) {
            throw new MemcacheException('Cache version change');
        }

        $this->params = $data[1];

        $this->init();
    }

    /**
     * Serialize (Serializable interface - PHP 7 compatibility).
     *
     * @return string  Serialized representation of this object.
     */
    public function serialize(): string
    {
        return serialize($this->__serialize());
    }

    /**
     * Unserialize (Serializable interface - PHP 7 compatibility).
     *
     * @param string $data  Serialized data.
     *
     * @throws MemcacheException
     */
    public function unserialize($data): void
    {
        try {
            $data = unserialize($data, ['allowed_classes' => true]);
        } catch (Throwable $e) {
            throw new MemcacheException('Failed to unserialize MemcacheApi', 0, $e);
        }
        $this->__unserialize($data);
    }

    /**
     * Convert PSR-16 TTL to seconds.
     *
     * @param null|int|DateInterval $ttl  The TTL value.
     *
     * @return int  Expiration time in seconds (0 = no expiration).
     */
    protected function convertTtl(int|DateInterval|null $ttl): int
    {
        if ($ttl === null) {
            return 0;  // No expiration
        }

        if ($ttl instanceof DateInterval) {
            $now = new DateTimeImmutable();
            $then = $now->add($ttl);
            return $then->getTimestamp() - $now->getTimestamp();
        }

        return $ttl;
    }

    /**
     * Fetch single key without oversized support (for PSR-16).
     *
     * Used by PSR-16 get() and has() methods. Does NOT handle oversized items.
     *
     * @param string $key  The cache key.
     *
     * @return mixed  The cached value or false on miss.
     */
    protected function fetchSingleStandard(string $key): mixed
    {
        $mc_key = $this->_key($key);
        $result = $this->memcache->get($mc_key);

        if ($result === false) {
            $this->noexist[$key] = true;
            return false;
        }

        return $this->unserializeValue($result, $key);
    }
}
