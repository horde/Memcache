<?php

declare(strict_types=1);

namespace Horde\Memcache\Exception;

use Horde\Exception\Wrapped;
use Throwable;

/**
 * Exception for connection failures.
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
class ConnectionException extends Wrapped
{
    /**
     * Constructor.
     *
     * @param string $message  Exception message.
     * @param array $servers  Server configuration that failed.
     * @param ?Throwable $previous  Previous exception.
     */
    public function __construct(
        string $message,
        public readonly array $servers,
        ?Throwable $previous = null
    ) {
        parent::__construct($message, 0, $previous);
    }
}
