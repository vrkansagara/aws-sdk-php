<?php
namespace Aws\S3;

use Aws\CommandInterface;
use Psr\Http\Message\RequestInterface;

/**
 * Used to change the style in which buckets are inserted in to the URL
 * (path or virtual style) based on the context.
 *
 * IMPORTANT: this middleware must be added after the "build" step.
 *
 * @internal
 */
class BucketStyleMiddleware
{
    private static $exclusions = ['GetBucketLocation' => true];
    private $nextHandler;

    /**
     * Create a middleware wrapper function.
     *
     * @return callable
     */
    public static function wrap()
    {
        return function (callable $handler) {
            return new self($handler);
        };
    }

    public function __construct(callable $nextHandler)
    {
        $this->nextHandler = $nextHandler;
    }

    public function __invoke(CommandInterface $command, RequestInterface $request)
    {
        $nextHandler = $this->nextHandler;
        $bucket = $command['Bucket'];

        if ($bucket && !isset(self::$exclusions[$command->getName()])) {
            $request = $this->modifyRequest($request, $command);
        }

        return $nextHandler($command, $request);
    }

    private function removeBucketFromPath($path, $bucket)
    {
        $len = strlen($bucket) + 1;
        if (substr($path, 0, $len) === "/{$bucket}") {
            $path = substr($path, $len);
        }

        return $path ?: '/';
    }

    private function modifyRequest(
        RequestInterface $request,
        CommandInterface $command
    ) {
        $uri = $request->getUri();
        $path = $uri->getPath();
        $bucket = $command['Bucket'];
        $path = $this->removeBucketFromPath($path, $bucket);

        // Modify the Key to make sure the key is encoded, but slashes are not.
        if ($command['Key']) {
            $path = S3Client::encodeKey(rawurldecode($path));
        }

        return $request->withUri($uri->withPath($path));
    }
}
