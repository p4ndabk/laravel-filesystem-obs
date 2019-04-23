<?php

namespace Obs;

use League\Flysystem\Adapter\AbstractAdapter;
use League\Flysystem\Adapter\Polyfill\NotSupportingVisibilityTrait;
use League\Flysystem\Config;

/**
 * Class ObsAdapter
 * @package Obs
 */
class ObsAdapter extends AbstractAdapter
{
    use NotSupportingVisibilityTrait;

    /**
     * @var ObsClient
     */
    protected $client;

    /**
     * @var
     */
    protected $bucket;

    /**
     * ObsAdapter constructor.
     * @param ObsClient $client
     * @param string $bucket
     * @param string $prefix
     */
    public function __construct(ObsClient $client, string $bucket, string $prefix = '')
    {
        $this->client = $client;

        $this->bucket = $bucket;

        $this->setPathPrefix($prefix);
    }

    /**
     * @return ObsClient
     */
    public function getClient(): ObsClient
    {
        return $this->client;
    }

    /**
     * @return string
     */
    public function getBucket(): string
    {
        return $this->bucket;
    }

    public function write($path, $contents, Config $config)
    {
        $path = $this->applyPathPrefix($path);

        try {
            $object = $this->client->putObject([
                'Bucket' => $this->getBucket(),
                'Key' => $path,
                'SourceFile' => $contents
            ]);
        } catch (ObsException $e) {
            return false;
        }

        return $this->normalizeResponse($object);
    }

    public function writeStream($path, $resource, Config $config)
    {
        $path = $this->applyPathPrefix($path);

        try {
            $object = $this->client->putObject([
                'Bucket' => $this->getBucket(),
                'Key' => $path,
                'Body' => $contents
            ]);
        } catch (ObsException $e) {
            return false;
        }

        return $this->normalizeResponse($object);
    }

    public function update($path, $contents, Config $config)
    {
        return $this->write($path, $contents, $config);
    }

    public function updateStream($path, $resource, Config $config)
    {
        return $this->writeStream($path, $contents, $config);
    }

    public function rename($path, $newpath)
    {
        $this->copy($path, $newpath);

        $this->delete($path);
    }

    public function copy($path, $newpath)
    {
        $path = $this->applyPathPrefix($path);
        $newpath = $this->applyPathPrefix($newpath);

        try {
            $object = $this->client->deleteObject([
                'Bucket' => $this->getBucket(),
                'Key' => $newpath,
                'CopySource' => $this->getBucket() . '/' . $path
            ]);
        } catch (ObsException $e) {
            return false;
        }

        return $this->normalizeResponse($object);
    }

    public function delete($path)
    {
        $path = $this->applyPathPrefix($path);

        try {
            $object = $this->client->deleteObject([
                'Bucket' => $this->getBucket(),
                'Key' => $path
            ]);
        } catch (ObsException $e) {
            return false;
        }

        return $this->normalizeResponse($object);
    }

    public function deleteDir($dirname)
    {
        return $this->delete($dirname);
    }

    public function createDir($dirname, Config $config)
    {
        $path = $this->applyPathPrefix($dirname);

        try {
            $object = $this->client->putObject([
                'Bucket' => $this->getBucket(),
                'Key' => $path
            ]);
        } catch (ObsException $e) {
            return false;
        }

        return $this->normalizeResponse($object);
    }

    public function has($path)
    {
        return $this->getMetadata($path);
    }

    public function read($path)
    {
        $path = $this->applyPathPrefix($path);

        try {
            $object = $this->client->getObject([
                'Bucket' => $this->getBucket(),
                'Key' => $path,
                'SaveAsFile' => $path
            ]);
        } catch (ObsException $e) {
            return false;
        }

        return $this->normalizeResponse($object);
    }

    public function readStream($path)
    {
        $path = $this->applyPathPrefix($path);

        try {
            $object = $this->client->getObject([
                'Bucket' => $this->getBucket(),
                'Key' => $path,
                'SaveAsStream' => true
            ]);
        } catch (ObsException $e) {
            return false;
        }

        return $this->normalizeResponse($object);
    }

    public function listContents($directory = '', $recursive = false)
    {
        $path = $this->applyPathPrefix($directory);

        try {
            $object = $this->client->listObjects([
                'Bucket' => $this->getBucket(),
                'MaxKeys' => 1000,
                'Prefix' => $directory,
                'Marker' => null
            ]);
        } catch (ObsException $e) {
            return false;
        }

        return $this->normalizeResponse($object);
    }

    public function getMetadata($path)
    {
        $path = $this->applyPathPrefix($path);

        try {
            $object = $this->client->getObjectMetadata([
                'Bucket' => $this->getBucket(),
                'Key' => $path
            ]);
        } catch (ObsException $e) {
            return false;
        }

        return $this->normalizeResponse($object);
    }

    public function getSize($path)
    {
        $object = $this->getMetadata($path);
        $object['size'] = $object['ContentLength'];

        return $object;
    }

    public function getMimetype($path)
    {
        $object = $this->getMetadata($path);
        $object['mimetype'] = $object['ContentType'];

        return $object;
    }

    public function getTimestamp($path)
    {
        $object = $this->getMetadata($path);

        return $object;
    }

    public function normalizeResponse($response): array
    {
        $path = ltrim($this->removePathPrefix($response['path_display']), '/');

        $result = ['path' => $path];

        if (isset($response['LastModified'])) {
            $result['timestamp'] = strtotime($response['LastModified']);
        }

        if (isset($response['Size'])) {
            $result['size'] = $response['Size'];
            $result['bytes'] = $response['Size'];
        }

        $type = (substr($result['path'], -1) === '/' ? 'dir' : 'file');

        $result['type'] = $type;

        return $result;
    }
}
