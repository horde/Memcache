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

namespace Horde\Memcache\Test\Unit;

use Horde\Memcache\MemcacheApi;
use Horde\Memcache\Config;
use PHPUnit\Framework\TestCase;
use PHPUnit\Framework\Attributes\CoversClass;
use Psr\Log\NullLogger;
use Memcache;
use Memcached;
use DateInterval;
use ReflectionClass;

/**
 * Unit tests for MemcacheApi with mocked drivers.
 *
 * These tests use mocked Memcache/Memcached objects to test logic
 * without requiring actual memcached server or PHP extensions.
 */
#[CoversClass(MemcacheApi::class)]
#[CoversClass(Config::class)]
class MemcacheApiTest extends TestCase
{
    private MemcacheApi $cache;
    private Memcache|Memcached $mockDriver;

    protected function setUp(): void
    {
        // Create mock of Memcached (newer extension)
        $this->mockDriver = $this->createMock(Memcached::class);

        // Create Config
        $config = new Config(
            hostspec: ['127.0.0.1'],
            port: [11211],
            prefix: 'test_'
        );

        // Create MemcacheApi with mocked driver injected via reflection
        $this->cache = new MemcacheApi($config, new NullLogger());

        // Inject mock driver via reflection
        $reflection = new ReflectionClass($this->cache);
        $property = $reflection->getProperty('memcache');
        $property->setAccessible(true);
        $property->setValue($this->cache, $this->mockDriver);
    }

    // =================================================================
    // PSR-16 Interface Tests
    // =================================================================

    public function testGetReturnsDefaultOnMiss(): void
    {
        $this->mockDriver
            ->expects($this->once())
            ->method('get')
            ->with('mykey')  // Memcached uses OPT_PREFIX_KEY, no manual prefixing
            ->willReturn(false);

        $result = $this->cache->get('mykey', 'default_value');
        $this->assertEquals('default_value', $result);
    }

    public function testGetReturnsValueOnHit(): void
    {
        $serialized = serialize('cached_value');

        $this->mockDriver
            ->expects($this->once())
            ->method('get')
            ->with('mykey')  // Memcached uses OPT_PREFIX_KEY, no manual prefixing
            ->willReturn($serialized);

        $result = $this->cache->get('mykey');
        $this->assertEquals('cached_value', $result);
    }

    public function testSetStoresValue(): void
    {
        $this->mockDriver
            ->expects($this->once())
            ->method('set')
            ->with(
                'mykey',  // Memcached uses OPT_PREFIX_KEY, no manual prefixing
                $this->callback(function ($value) {
                    return unserialize($value) === 'test_value';
                }),
                3600
            )
            ->willReturn(true);

        $result = $this->cache->set('mykey', 'test_value', 3600);
        $this->assertTrue($result);
    }

    public function testSetFailsOnOversizedItem(): void
    {
        // Create item > 1MB
        $largeValue = str_repeat('x', 1024 * 1024 + 1000);

        // Should not call set() on driver because it's too large
        $this->mockDriver
            ->expects($this->never())
            ->method('set');

        $result = $this->cache->set('mykey', $largeValue, 3600);
        $this->assertFalse($result);
    }

    public function testDeleteRemovesKey(): void
    {
        $this->mockDriver
            ->expects($this->once())
            ->method('delete')
            ->with('mykey')  // Memcached uses OPT_PREFIX_KEY
            ->willReturn(true);

        $result = $this->cache->delete('mykey');
        $this->assertTrue($result);
    }

    public function testClearFlushesCache(): void
    {
        $this->mockDriver
            ->expects($this->once())
            ->method('flush')
            ->willReturn(true);

        $result = $this->cache->clear();
        $this->assertTrue($result);
    }

    public function testHasReturnsTrueWhenKeyExists(): void
    {
        $this->mockDriver
            ->expects($this->once())
            ->method('get')
            ->with('mykey')  // Memcached uses OPT_PREFIX_KEY
            ->willReturn(serialize('value'));

        $result = $this->cache->has('mykey');
        $this->assertTrue($result);
    }

    public function testHasReturnsFalseWhenKeyMissing(): void
    {
        $this->mockDriver
            ->expects($this->once())
            ->method('get')
            ->with('mykey')  // Memcached uses OPT_PREFIX_KEY
            ->willReturn(false);

        $result = $this->cache->has('mykey');
        $this->assertFalse($result);
    }

    public function testGetMultipleReturnsArray(): void
    {
        // getMultiple() calls get() for each key individually
        $this->mockDriver
            ->expects($this->exactly(2))
            ->method('get')
            ->willReturnCallback(function ($key) {
                if ($key === 'key1') {
                    return serialize('value1');
                }
                return false;  // key2 missing
            });

        $result = $this->cache->getMultiple(['key1', 'key2'], 'default');

        $expected = [
            'key1' => 'value1',
            'key2' => 'default',
        ];

        $this->assertEquals($expected, $result);
    }

    public function testSetMultipleStoresValues(): void
    {
        $this->mockDriver
            ->expects($this->exactly(2))
            ->method('set')
            ->willReturn(true);

        $result = $this->cache->setMultiple(['key1' => 'val1', 'key2' => 'val2'], 3600);
        $this->assertTrue($result);
    }

    public function testDeleteMultipleRemovesKeys(): void
    {
        $this->mockDriver
            ->expects($this->exactly(2))
            ->method('delete')
            ->willReturn(true);

        $result = $this->cache->deleteMultiple(['key1', 'key2']);
        $this->assertTrue($result);
    }

    // =================================================================
    // TTL Conversion Tests
    // =================================================================

    public function testConvertTtlWithNull(): void
    {
        $reflection = new ReflectionClass($this->cache);
        $method = $reflection->getMethod('convertTtl');
        $method->setAccessible(true);

        $result = $method->invoke($this->cache, null);
        $this->assertEquals(0, $result);
    }

    public function testConvertTtlWithInteger(): void
    {
        $reflection = new ReflectionClass($this->cache);
        $method = $reflection->getMethod('convertTtl');
        $method->setAccessible(true);

        $result = $method->invoke($this->cache, 3600);
        $this->assertEquals(3600, $result);
    }

    public function testConvertTtlWithDateInterval(): void
    {
        $reflection = new ReflectionClass($this->cache);
        $method = $reflection->getMethod('convertTtl');
        $method->setAccessible(true);

        $interval = new DateInterval('PT1H'); // 1 hour
        $result = $method->invoke($this->cache, $interval);

        // Should be approximately 3600 seconds (allow for minor timing variations)
        $this->assertGreaterThanOrEqual(3599, $result);
        $this->assertLessThanOrEqual(3601, $result);
    }

    // =================================================================
    // Horde Extended API Tests
    // =================================================================

    public function testDeleteDelayedWithTimeout(): void
    {
        // Memcached doesn't support native timeout, should use set(null, ttl)
        $this->mockDriver
            ->expects($this->once())
            ->method('set')
            ->with('mykey', null, 5)  // Memcached uses OPT_PREFIX_KEY
            ->willReturn(true);

        $result = $this->cache->deleteDelayed('mykey', 5);
        $this->assertTrue($result);
    }

    public function testDeleteDelayedWithoutTimeout(): void
    {
        // Without timeout, should use normal delete
        $this->mockDriver
            ->expects($this->once())
            ->method('delete')
            ->with('mykey')  // Memcached uses OPT_PREFIX_KEY
            ->willReturn(true);

        $result = $this->cache->deleteDelayed('mykey', 0);
        $this->assertTrue($result);
    }

    // =================================================================
    // Key Prefixing Tests
    // =================================================================

    public function testKeyPrefixingNotManuallyAppliedForMemcached(): void
    {
        // Verify that for Memcached, keys are NOT manually prefixed
        // (OPT_PREFIX_KEY handles it automatically)
        $this->mockDriver
            ->expects($this->once())
            ->method('set')
            ->with(
                'mykey',  // No manual prefix!
                $this->anything(),
                $this->anything()
            )
            ->willReturn(true);

        $this->cache->set('mykey', 'value', 0);
    }
}
