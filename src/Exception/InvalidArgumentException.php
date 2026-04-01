<?php

declare(strict_types=1);

namespace Horde\Memcache\Exception;

use Horde\Exception\Wrapped;

/**
 * Exception for invalid arguments (PSR-16 compatibility).
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
class InvalidArgumentException extends Wrapped implements \Psr\SimpleCache\InvalidArgumentException {}
