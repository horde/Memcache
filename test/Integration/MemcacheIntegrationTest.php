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

namespace Horde\Memcache\Test\Integration;

use Horde\Memcache\MemcacheApi;
use Horde\Memcache\Config;
use Horde\Test\TestCase;
use Throwable;

/**
 * Integration tests for Horde Extended API (large items, multi-key operations).
 *
 * Requires memcached running on localhost:11211.
 * Run with: phpunit -c phpunit.integration.xml
 *
 * @coversNothing
 */
class MemcacheIntegrationTest extends TestCase
{
    private MemcacheApi $cache;

    public function setUp(): void
    {
        if (!(extension_loaded('memcache') || extension_loaded('memcached'))) {
            $this->markTestSkipped('Neither Memcache nor Memcached extension loaded');
            return;
        }

        // Use local memcached for integration tests
        $config = new Config(
            hostspec: ['127.0.0.1'],
            port: [11211],
            prefix: 'horde_integration_test_'
        );

        try {
            $this->cache = new MemcacheApi($config);
        } catch (Throwable $e) {
            $this->markTestSkipped('Could not connect to memcached: ' . $e->getMessage());
        }
    }

    public function testMemcacheSetup()
    {
        $this->assertInstanceOf(MemcacheApi::class, $this->cache);
    }

    // =================================================================
    // Horde Extended API Tests (setLarge/getLarge/getItems)
    // =================================================================

    public function testGetLargeSetLargeStringValue()
    {
        $this->assertTrue($this->cache->setLarge('TESTKEY', 'A'));
        $this->assertEquals('A', $this->cache->getLarge('TESTKEY'));
        $this->cache->deleteDelayed('TESTKEY', 0);
    }

    public function testGetLargeSetLargeIntValue()
    {
        $this->assertTrue($this->cache->setLarge('TESTKEY', 100));
        $this->assertEquals(100, $this->cache->getLarge('TESTKEY'));
        $this->cache->deleteDelayed('TESTKEY', 0);
    }

    public function testGetLargeSetLargeArrayValue()
    {
        $data = [100, 200, 'A'];
        $this->assertTrue($this->cache->setLarge('TESTKEY', $data));
        $this->assertEquals($data, $this->cache->getLarge('TESTKEY'));
        $this->cache->deleteDelayed('TESTKEY', 0);
    }

    public function testGetItemsMultipleKeys()
    {
        $this->cache->setLarge('TESTKEY1', 100);
        $this->cache->setLarge('TESTKEY2', 200);
        $result = $this->cache->getItems(['TESTKEY1', 'TESTKEY2']);

        $this->assertEquals(
            [
                'TESTKEY1' => 100,
                'TESTKEY2' => 200,
            ],
            $result
        );

        $this->cache->deleteDelayed('TESTKEY1', 0);
        $this->cache->deleteDelayed('TESTKEY2', 0);
    }

    public function testSetLargeLargeItem()
    {
        // Test that setLarge handles items >1MB via auto-splitting
        $largeValue = str_repeat('x', 1024 * 1024 + 1000); // ~1MB + 1KB

        $this->assertTrue($this->cache->setLarge('LARGE_ITEM', $largeValue, 60));
        $retrieved = $this->cache->getLarge('LARGE_ITEM');

        $this->assertEquals($largeValue, $retrieved);
        $this->cache->deleteDelayed('LARGE_ITEM', 0);
    }

    public function testDeleteDelayedWithTimeout()
    {
        $this->cache->setLarge('DEL_TEST', 'value', 60);
        $this->assertTrue($this->cache->deleteDelayed('DEL_TEST', 5));
        $this->assertFalse($this->cache->getLarge('DEL_TEST'));
    }

    public function testGetItemsWithMissingKeys()
    {
        $this->cache->setLarge('EXISTS', 'value', 60);
        $result = $this->cache->getItems(['EXISTS', 'MISSING']);

        $this->assertEquals('value', $result['EXISTS']);
        $this->assertFalse($result['MISSING']);

        $this->cache->deleteDelayed('EXISTS', 0);
    }
}
