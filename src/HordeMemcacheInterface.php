<?php

/**
 * Copyright 2020-2026 Horde LLC (http://www.horde.org/)
 *
 * See the enclosed file LICENSE for license information (LGPL). If you
 * did not receive this file, see http://www.horde.org/licenses/lgpl21.
 *
 * @author   Ralf Lang <ralf.lang@ralf-lang.de>
 * @category Horde
 * @package  Memcache
 */
declare(strict_types=1);

namespace Horde\Memcache;

/**
 * Horde Extended Memcache Interface.
 *
 * Defines Horde-specific caching features beyond PSR-16:
 * - Large item support (>1MB automatic chunking)
 * - Multi-key batch retrieval
 * - Delayed deletion (blocks key for timeout period)
 *
 * This interface is disjunct from PSR-16 SimpleCache\CacheInterface.
 * Implementations may choose to implement both interfaces.
 */
interface HordeMemcacheInterface
{
    /**
     * Get single item with large item support.
     *
     * Automatically reassembles items >1MB that were split across
     * multiple keys by setLarge().
     *
     * @param string $key  Cache key.
     *
     * @return mixed  Value or false on miss.
     */
    public function getLarge(string $key): mixed;

    /**
     * Set item with large item support.
     *
     * Automatically splits items >1MB into multiple keys using
     * chunking for reassembly by getLarge().
     *
     * @param string $key     Cache key.
     * @param mixed $value    Value to store.
     * @param int $expire     Expiration in seconds (0 = no expiration).
     *
     * @return bool  True on success.
     */
    public function setLarge(string $key, mixed $value, int $expire = 0): bool;

    /**
     * Get multiple items (batch retrieval).
     *
     * Returns array of raw values for given keys. Each value
     * supports large item reassembly.
     *
     * @param array $keys  Array of cache keys.
     *
     * @return array  ['key1' => value1, 'key2' => value2, 'key3' => false]
     */
    public function getItems(array $keys): array;

    /**
     * Delete with timeout (delayed deletion).
     *
     * Blocks add() operations on this key for the specified
     * timeout period after deletion.
     *
     * @param string $key      Cache key.
     * @param int $timeout     Timeout in seconds (blocks add() for this duration).
     *
     * @return bool  True on success.
     */
    public function deleteDelayed(string $key, int $timeout = 0): bool;
}
