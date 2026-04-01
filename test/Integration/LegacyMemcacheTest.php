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

/**
 * Tests for Horde Extended API (large items, multi-key operations).
 *
 * @coversNothing
 */
class LegacyMemcacheTest extends TestCase
{
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

        // Convert array config to Config object
        $configObj = Config::fromArray($config['memcache']);
        $this->cache = new MemcacheApi($configObj);
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
    }

    public function testGetLargeSetLargeIntValue()
    {
        $this->assertTrue($this->cache->setLarge('TESTKEY', 100));
        $this->assertEquals(100, $this->cache->getLarge('TESTKEY'));
    }

    public function testGetLargeSetLargeArrayValue()
    {
        $data = [100, 200, 'A'];
        $this->assertTrue($this->cache->setLarge('TESTKEY', $data));
        $this->assertEquals($data, $this->cache->getLarge('TESTKEY'));
    }

    public function testGetItemsMultipleKeys()
    {
        $this->cache->setLarge('TESTKEY1', 100);
        $this->cache->setLarge('TESTKEY2', 200);
        $this->assertEquals(
            [
                'TESTKEY1' => 100,
                'TESTKEY2' => 200,
            ],
            $this->cache->getItems(['TESTKEY1', 'TESTKEY2'])
        );
    }
}
