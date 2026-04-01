<?php

/**
 * Copyright 2026 Horde LLC (http://www.horde.org/)
 *
 * See the enclosed file LICENSE for license information (LGPL). If you
 * did not receive this file, see http://www.horde.org/licenses/lgpl21.
 *
 * @author   Ralf Lang <ralf.lang@ralf-lang.de>
 * @category Horde
 * @package  Memcache
 */
declare(strict_types=1);

namespace Horde\Memcache\Test;

use Horde\Memcache\MemcacheApi;
use Horde\Memcache\Config;
use Horde\Test\TestCase;
use Psr\SimpleCache\CacheInterface;
use DateInterval;

/**
 * PSR-16 Simple Cache interface compliance tests.
 *
 * @coversNothing
 */
class Psr16ComplianceTest extends TestCase
{
    private CacheInterface $cache;

    public function setUp(): void
    {
        if (!(extension_loaded('memcache') || extension_loaded('memcached'))) {
            $this->markTestSkipped('Neither Memcache nor Memcached extension loaded');
            return;
        }
        if (!($config = self::getConfig('TEST_CONFIG'))
            || !isset($config['memcache'])) {
            $this->markTestSkipped('No configuration available, set TEST_CONFIG env var');
            return;
        }

        $configObj = Config::fromArray($config['memcache']);
        $this->cache = new MemcacheApi($configObj);
    }

    public function testImplementsPsr16Interface()
    {
        $this->assertInstanceOf(CacheInterface::class, $this->cache);
    }

    // =================================================================
    // PSR-16 Basic Operations
    // =================================================================

    public function testGetReturnsDefaultOnMiss()
    {
        $result = $this->cache->get('nonexistent-key', 'my-default');
        $this->assertEquals('my-default', $result);
    }

    public function testGetReturnsNullDefaultOnMiss()
    {
        $result = $this->cache->get('nonexistent-key-2');
        $this->assertNull($result);
    }

    public function testSetAndGetSimpleValue()
    {
        $this->assertTrue($this->cache->set('test-key', 'test-value', 60));
        $this->assertEquals('test-value', $this->cache->get('test-key'));
    }

    public function testSetAndGetIntValue()
    {
        $this->assertTrue($this->cache->set('test-int', 42, 60));
        $this->assertEquals(42, $this->cache->get('test-int'));
    }

    public function testSetAndGetArrayValue()
    {
        $data = ['a' => 1, 'b' => 2];
        $this->assertTrue($this->cache->set('test-array', $data, 60));
        $this->assertEquals($data, $this->cache->get('test-array'));
    }

    public function testSetWithNullTtl()
    {
        $this->assertTrue($this->cache->set('test-null-ttl', 'value'));
        $this->assertEquals('value', $this->cache->get('test-null-ttl'));
    }

    public function testSetWithIntTtl()
    {
        $this->assertTrue($this->cache->set('test-int-ttl', 'value', 3600));
        $this->assertEquals('value', $this->cache->get('test-int-ttl'));
    }

    public function testSetWithDateIntervalTtl()
    {
        $interval = new DateInterval('PT1H'); // 1 hour
        $this->assertTrue($this->cache->set('test-interval-ttl', 'value', $interval));
        $this->assertEquals('value', $this->cache->get('test-interval-ttl'));
    }

    public function testDelete()
    {
        $this->cache->set('test-delete', 'value', 60);
        $this->assertTrue($this->cache->delete('test-delete'));
        $this->assertNull($this->cache->get('test-delete'));
    }

    public function testHas()
    {
        $this->cache->set('test-has', 'value', 60);
        $this->assertTrue($this->cache->has('test-has'));
        $this->cache->delete('test-has');
        $this->assertFalse($this->cache->has('test-has'));
    }

    // =================================================================
    // PSR-16 Multiple Operations
    // =================================================================

    public function testGetMultiple()
    {
        $this->cache->set('multi1', 'value1', 60);
        $this->cache->set('multi2', 'value2', 60);

        $result = $this->cache->getMultiple(['multi1', 'multi2', 'nonexistent'], 'default');

        $this->assertEquals('value1', $result['multi1']);
        $this->assertEquals('value2', $result['multi2']);
        $this->assertEquals('default', $result['nonexistent']);
    }

    public function testSetMultiple()
    {
        $values = [
            'setmulti1' => 'value1',
            'setmulti2' => 'value2',
            'setmulti3' => 'value3',
        ];

        $this->assertTrue($this->cache->setMultiple($values, 60));

        $this->assertEquals('value1', $this->cache->get('setmulti1'));
        $this->assertEquals('value2', $this->cache->get('setmulti2'));
        $this->assertEquals('value3', $this->cache->get('setmulti3'));
    }

    public function testDeleteMultiple()
    {
        $this->cache->set('delmulti1', 'value1', 60);
        $this->cache->set('delmulti2', 'value2', 60);

        $this->assertTrue($this->cache->deleteMultiple(['delmulti1', 'delmulti2']));

        $this->assertNull($this->cache->get('delmulti1'));
        $this->assertNull($this->cache->get('delmulti2'));
    }

    public function testClear()
    {
        $this->cache->set('clear1', 'value1', 60);
        $this->cache->set('clear2', 'value2', 60);

        $this->assertTrue($this->cache->clear());

        // Note: clear() calls flush() which may not be scoped to prefix
        // So we just verify it succeeds without error
    }

    // =================================================================
    // PSR-16 Edge Cases
    // =================================================================

    public function testSetLargeValueFailsInPsr16()
    {
        // PSR-16 set() should fail on items >1MB
        $largeValue = str_repeat('x', 1024 * 1024 + 1); // >1MB
        $result = $this->cache->set('large-item', $largeValue, 60);
        $this->assertFalse($result);
    }

    public function testStoreFalseValue()
    {
        // Ensure false can be stored and retrieved
        $this->cache->set('false-value', false, 60);
        $result = $this->cache->get('false-value', 'default');
        $this->assertFalse($result);
        $this->assertNotEquals('default', $result);
    }

    public function testStoreZeroValue()
    {
        $this->cache->set('zero-value', 0, 60);
        $result = $this->cache->get('zero-value', 'default');
        $this->assertSame(0, $result);
    }

    public function testStoreEmptyString()
    {
        $this->cache->set('empty-string', '', 60);
        $result = $this->cache->get('empty-string', 'default');
        $this->assertSame('', $result);
    }
}
