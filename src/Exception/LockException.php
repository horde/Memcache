<?php

declare(strict_types=1);

namespace Horde\Memcache\Exception;

use Horde\Exception\Wrapped;
use Throwable;

/**
 * Exception for locking failures.
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
class LockException extends Wrapped
{
    /**
     * Constructor.
     *
     * @param string $message  Exception message.
     * @param string $key  Cache key that failed to lock.
     * @param int $timeout  Lock timeout in seconds.
     * @param ?Throwable $previous  Previous exception.
     */
    public function __construct(
        string $message,
        public readonly string $key,
        public readonly int $timeout,
        ?Throwable $previous = null
    ) {
        parent::__construct($message, 0, $previous);
    }
}
