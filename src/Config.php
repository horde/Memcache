<?php

declare(strict_types=1);

namespace Horde\Memcache;

use InvalidArgumentException;

/**
 * Configuration for Memcache connection.
 *
 * Copyright 2026 Horde LLC (http://www.horde.org/)
 *
 * See the enclosed file LICENSE for license information (LGPL). If you
 * did not receive this file, see http://www.horde.org/licenses/lgpl21.
 *
 * @author   Ralf Lang <ralf.lang@ralf-lang.de>
 * @category Horde
 * @license  http://www.horde.org/licenses/lgpl21 LGPL 2.1
 * @package  Memcache
 */
class Config
{
    /**
     * Constructor.
     *
     * @param array $hostspec  Memcache server hostnames or IPs.
     * @param array $port  Port numbers for each host (0 for UNIX socket).
     * @param string $prefix  Key prefix for namespacing.
     * @param bool $compression  Enable compression.
     * @param bool $persistent  Use persistent connections.
     * @param bool $largeItems  Allow items larger than 1MB (chunked storage).
     * @param array $weight  Connection weights for each server.
     * @param ?int $compressionThreshold  Minimum bytes before compressing.
     */
    public function __construct(
        public readonly array $hostspec = ['localhost'],
        public readonly array $port = [11211],
        public readonly string $prefix = 'horde',
        public readonly bool $compression = false,
        public readonly bool $persistent = false,
        public readonly bool $largeItems = true,
        public readonly array $weight = [],
        public readonly ?int $compressionThreshold = null,
    ) {
        // Validate
        if (empty($this->hostspec)) {
            throw new InvalidArgumentException('At least one host must be specified');
        }

        if (count($this->port) > 0 && count($this->port) !== count($this->hostspec)) {
            throw new InvalidArgumentException('Port count must match host count or be empty');
        }

        if (count($this->weight) > 0 && count($this->weight) !== count($this->hostspec)) {
            throw new InvalidArgumentException('Weight count must match host count or be empty');
        }
    }

    /**
     * Create from legacy array format.
     *
     * @param array $params  Legacy params array.
     *
     * @return self
     */
    public static function fromArray(array $params): self
    {
        return new self(
            hostspec: $params['hostspec'] ?? ['localhost'],
            port: $params['port'] ?? [11211],
            prefix: $params['prefix'] ?? 'horde',
            compression: $params['compression'] ?? false,
            persistent: $params['persistent'] ?? false,
            largeItems: $params['large_items'] ?? true,
            weight: $params['weight'] ?? [],
            compressionThreshold: $params['c_threshold'] ?? null,
        );
    }

    /**
     * Convert to legacy array format.
     *
     * @return array
     */
    public function toArray(): array
    {
        return [
            'hostspec' => $this->hostspec,
            'port' => $this->port,
            'prefix' => $this->prefix,
            'compression' => $this->compression,
            'persistent' => $this->persistent,
            'large_items' => $this->largeItems,
            'weight' => $this->weight,
            'c_threshold' => $this->compressionThreshold,
        ];
    }
}
