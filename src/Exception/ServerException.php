<?php

declare(strict_types=1);

namespace Horde\Memcache\Exception;

use Horde\Exception\Wrapped;
use Throwable;

/**
 * Exception for server-specific failures.
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
class ServerException extends Wrapped
{
    /**
     * Constructor.
     *
     * @param string $message  Exception message.
     * @param string $server  Server hostname or IP.
     * @param int $port  Server port.
     * @param ?Throwable $previous  Previous exception.
     */
    public function __construct(
        string $message,
        public readonly string $server,
        public readonly int $port,
        ?Throwable $previous = null
    ) {
        parent::__construct($message, 0, $previous);
    }
}
