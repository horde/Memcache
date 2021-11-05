<?php
/**
 * Copyright 2020-2021 Horde LLC (http://www.horde.org/)
 *
 * See the enclosed file LICENSE for license information (LGPL). If you
 * did not receive this file, see http://www.horde.org/licenses/lgpl21.
 *
 * @author   Ralf Lang <lang@b1-systems.de>
 * @category Horde
 * @package  Memcache
 */
declare(strict_types=1);
 namespace Horde\Memcache\Test;
use Horde\Memcache\MemcacheApi;
use Horde\Test\TestCase;
class MemcacheTest extends TestCase
{
    function setUp(): void
    {
        if (!(extension_loaded('memcache') || extension_loaded('memcached'))) {
            $this->markTestSkipped('Neither Memcache nor Memcached extension loaded'); 
            return;
        }
        if (!($config = self::getConfig('TEST_CONFIG')) ||
            !isset($config['memcache'])) {
                $this->markTestSkipped('No configuration available, set TEST_CONFIG env var'); 
            return;
        }
/*        $config['memcache']
                ['prefix' => 'horde_cache_test']*/
        $this->cache = new MemcacheApi($config);
    }

    public function testMemcacheSetup()
    {
        $this->assertInstanceOf(MemcacheApi::class, $this->cache);
    }

    public function testGetSetStringValue()
    {
        $this->assertTrue($this->cache->set('TESTKEY', 'A'));
        $this->assertEquals('A', $this->cache->get('TESTKEY'));
    }
    public function testGetSetIntValue()
    {
        $this->assertTrue($this->cache->set('TESTKEY', 100));
        $this->assertEquals(100, $this->cache->get('TESTKEY'));
    }
    public function testGetSetArrayValue()
    {
        $data = [100, 200, 'A'];
        $this->assertTrue($this->cache->set('TESTKEY', $data));
        $this->assertEquals($data, $this->cache->get('TESTKEY'));
    }
    public function testGetSetMultipleKeys()
    {
        $this->cache->set('TESTKEY1', 100);
        $this->cache->set('TESTKEY2', 200);
        $this->assertEquals(
            [
                'TESTKEY1' => 100,
                'TESTKEY2' => 200
            ],
            $this->cache->get(['TESTKEY1', 'TESTKEY2'])
        );
    }
}
