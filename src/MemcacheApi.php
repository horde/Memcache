<?php
/**
 * Copyright 2007-2021 Horde LLC (http://www.horde.org/)
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

use Serializable;
use Horde_Log_Logger;
use Memcache;
use Memcached;

/**
 * This class provides an API or Horde code to interact with a centrally
 * configured memcache installation.
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
class MemcacheApi implements Serializable
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
     *
     * @var array
     */
    protected $locks = [];

    /**
     * Logger instance.
     *
     * @var Horde_Log_Logger
     */
    protected Horde_Log_Logger $logger;

    /**
     * Memcache object.
     *
     * @var Memcache|Memcached
     */
    protected $memcache;

    /**
     * A list of items known not to exist.
     *
     * @var array
     */
    protected $noexist = [];

    /**
     * Memcache defaults.
     *
     * @var array
     */
    protected $params = [
        'compression' => false,
        'hostspec' => ['localhost'],
        'large_items' => true,
        'persistent' => false,
        'port' => [11211],
        'prefix' => 'horde',
    ];

    /**
     * The list of active servers.
     *
     * @var array
     */
    protected $servers = [];

    /**
     * Constructor.
     *
     * @param array $params  Configuration parameters:
     *   - compression: (boolean) Compress data inside memcache?
     *                  DEFAULT: false
     *   - c_threshold: (integer) The minimum value length before attempting
     *                  to compress.
     *                  DEFAULT: none
     *   - hostspec: (array) The memcached host(s) to connect to.
     *                  DEFAULT: 'localhost'
     *   - large_items: (boolean) Allow storing large data items (larger than
     *                  Horde_Memcache::MAX_SIZE)? Currently not supported with
     *                  memcached extension.
     *                  DEFAULT: true
     *   - persistent: (boolean) Use persistent DB connections?
     *                 DEFAULT: false
     *   - prefix: (string) The prefix to use for the memcache keys.
     *             DEFAULT: 'horde'
     *   - port: (array) The port(s) memcache is listening on. Leave empty
     *           if using UNIX sockets.
     *           DEFAULT: 11211
     *   - weight: (array) The weight(s) to use for each memcached host.
     *             DEFAULT: none (equal weight to all servers)
     *
     * @throws MemcacheException
     */
    public function __construct(array $params = [])
    {
        $this->params = array_merge($this->params, $params);
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
            $this->params['large_items'] = false;
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
            throw new MemcacheException('Could not connect to any defined memcache servers.');
        }

        if ($this->memcache instanceof Memcache &&
            !empty($this->params['c_threshold'])) {
            $this->memcache->setCompressThreshold($this->params['c_threshold']);
        }

        if (isset($this->params['logger'])) {
            $this->logger = $this->params['logger'];
            $this->logger->log('Connected to the following memcache servers:' . implode(', ', $this->servers), 'DEBUG');
        }
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
     * Delete a key.
     *
     * @see Memcache::delete()
     *
     * @param string $key       The key.
     * @param integer $timeout  Expiration time in seconds.
     *
     * @return boolean  True on success.
     */
    public function delete(string $key, int $timeout = 0): bool
    {
        return isset($this->noexist[$key])
            ? false
            : $this->memcache->delete($this->_key($key), $timeout);
    }

    /**
     * Get data associated with a key.
     *
     * @see Memcache::get()
     *
     * @param string|string[] $keys  The key or an array of keys.
     *
     * @return mixed  The string/array on success (return type is the type of
     *                $keys), false on failure.
     */
    public function get($keys)
    {
        $flags = null;
        $key_map = $missing_parts = $os = $out_array = [];
        $ret_array = true;

        if (!is_array($keys)) {
            $keys = [$keys];
            $ret_array = false;
        }
        $search_keys = $keys;

        foreach ($search_keys as $v) {
            $key_map[$v] = (string)$this->_key($v);
        }

        if ($this->memcache instanceof Memcached) {
            $res = $this->memcache->getMulti(array_values($key_map));
        } else {
            $res = $this->memcache->get(array_values($key_map), $flags);
        }
        if ($res === false) {
            return false;
        }

        /* Check to see if we have any oversize items we need to get. */
        if (!empty($this->params['large_items'])) {
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
                    return false;
                }

                /* $res should now contain the same results as if we had
                 * run a single get request with all keys above. */
                $res = array_merge($res, $res2);
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
                            $this->delete($k);
                            continue 2;
                        }
                    }
                }
                $out_array[$k] = @unserialize($data);
            } elseif (isset($os[$k]) && !isset($res[$key_map[$k]])) {
                $this->delete($k);
            }
        }

        return $ret_array
            ? $out_array
            : reset($out_array);
    }

    /**
     * Set the value of a key.
     *
     * @see Memcache::set()
     *
     * @param string $key       The key.
     * @param string|Serializable $var       The data to store.
     * @param int $expire  Expiration time in seconds.
     *
     * @return bool  True on success.
     */
    public function set(string $key, $var, int $expire = 0): bool
    {
        return $this->_set($key, @serialize($var), $expire);
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

        for ($i = 0; ($i * self::MAX_SIZE) < $len; ++$i) {
            $curr_key = $i ? ($key . '_s' . $i) : $key;
            $res = $this->memcache instanceof Memcached
                ? $this->memcache->set($curr_key, $var, $expire)
                : $this->memcache->set(
                    $this->_key($curr_key),
                    substr($var, $i * self::MAX_SIZE, self::MAX_SIZE),
                    $this->_getFlags($i ? 0 : ceil($len / self::MAX_SIZE)),
                    $expire
                );
            if ($res === false) {
                $this->delete($key);
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
     * @param string|Serializable $var       The data to store.
     * @param int $expire  Expiration time in seconds.
     *
     * @return bool  True on success, false if key doesn't exist.
     */
    public function replace(string $key, $var, int $expire = 0): bool
    {
        $var = @serialize($var);
        $len = strlen($var);

        if ($len > self::MAX_SIZE) {
            if (!empty($this->params['large_items']) &&
                $this->memcache->get($this->_key($key))) {
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
     */
    protected function _lockAdd(string $key)
    {
        if ($this->memcache instanceof Memcached) {
            $this->memcache->add(
                $this->_key($key . self::LOCK_SUFFIX),
                1,
                self::LOCK_TIMEOUT
            );
        } else {
            $this->memcache->add(
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
     * @throws MemcacheException
     */
    public function failover(string $host, int $port)
    {
        $pos = array_search($host . ':' . $port, $this->servers);
        if ($pos !== false) {
            unset($this->servers[$pos]);
            if (!count($this->servers)) {
                throw new MemcacheException('Could not connect to any defined memcache servers.');
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

    /* Serializable methods. */

    /**
     * Serialize.
     *
     * @return string  Serialized representation of this object.
     */
    public function serialize(): string
    {
        return serialize([
            self::VERSION,
            $this->params,
        ]);
    }

    /**
     * Unserialize.
     *
     * @param string $data  Serialized data.
     *
     * @throws MemcacheException
     */
    public function unserialize($data)
    {
        $data = @unserialize($data);
        if (!is_array($data) ||
            !isset($data[0]) ||
            ($data[0] != self::VERSION)) {
            throw new MemcacheException('Cache version change');
        }

        $this->params = $data[1];

        $this->init();
    }
}
