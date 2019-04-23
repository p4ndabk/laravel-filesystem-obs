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

    /**
     * Write a new file.
     *
     * @param string $path
     * @param string $contents
     * @param Config $config Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function write($path, $contents, Config $config)
    {
        return $this->upload($path, $contents);
    }

    /**
     * Write a new file using a stream.
     *
     * @param string $path
     * @param resource $resource
     * @param Config $config Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function writeStream($path, $resource, Config $config)
    {

    }

    /**
     * Update a file.
     *
     * @param string $path
     * @param string $contents
     * @param Config $config Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function update($path, $contents, Config $config)
    {

    }

    /**
     * Update a file using a stream.
     *
     * @param string $path
     * @param resource $resource
     * @param Config $config Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function updateStream($path, $resource, Config $config)
    {

    }

    /**
     * Rename a file.
     *
     * @param string $path
     * @param string $newpath
     *
     * @return bool
     */
    public function rename($path, $newpath)
    {

    }

    /**
     * Copy a file.
     *
     * @param string $path
     * @param string $newpath
     *
     * @return bool
     */
    public function copy($path, $newpath)
    {

    }

    /**
     * Delete a file.
     *
     * @param string $path
     *
     * @return bool
     */
    public function delete($path)
    {

    }

    /**
     * Delete a directory.
     *
     * @param string $dirname
     *
     * @return bool
     */
    public function deleteDir($dirname)
    {

    }

    /**
     * Create a directory.
     *
     * @param string $dirname directory name
     * @param Config $config
     *
     * @return array|false
     */
    public function createDir($dirname, Config $config)
    {

    }

    /**
     * Set the visibility for a file.
     *
     * @param string $path
     * @param string $visibility
     *
     * @return array|false file meta data
     */
    public function setVisibility($path, $visibility)
    {

    }



    /**
     * @param string $path
     * @return string
     */
    public function applyPathPrefix($path): string
    {
        $path = parent::applyPathPrefix($path);

        return '/' . trim($path, '/');
    }

    /**
     * @param string $path
     * @param resource|string $contents
     * @return array|false file metadata
     */
    protected function upload(string $path, $contents)
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

    protected function normalizeResponse(array $response): array
    {
        $normalizedPath = ltrim($this->removePathPrefix($response['path_display']), '/');

        $normalizedResponse = ['path' => $normalizedPath];

        if (isset($response['server_modified'])) {
            $normalizedResponse['timestamp'] = strtotime($response['server_modified']);
        }

        if (isset($response['size'])) {
            $normalizedResponse['size'] = $response['size'];
            $normalizedResponse['bytes'] = $response['size'];
        }

        $type = ($response['.tag'] === 'folder' ? 'dir' : 'file');

        $normalizedResponse['type'] = $type;

        return $normalizedResponse;
    }
}
