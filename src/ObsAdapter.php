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
    public function writeStream($path, $resource, Config $config);

    /**
     * Update a file.
     *
     * @param string $path
     * @param string $contents
     * @param Config $config Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function update($path, $contents, Config $config);

    /**
     * Update a file using a stream.
     *
     * @param string $path
     * @param resource $resource
     * @param Config $config Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function updateStream($path, $resource, Config $config);

    /**
     * Rename a file.
     *
     * @param string $path
     * @param string $newpath
     *
     * @return bool
     */
    public function rename($path, $newpath);

    /**
     * Copy a file.
     *
     * @param string $path
     * @param string $newpath
     *
     * @return bool
     */
    public function copy($path, $newpath);

    /**
     * Delete a file.
     *
     * @param string $path
     *
     * @return bool
     */
    public function delete($path);

    /**
     * Delete a directory.
     *
     * @param string $dirname
     *
     * @return bool
     */
    public function deleteDir($dirname);

    /**
     * Create a directory.
     *
     * @param string $dirname directory name
     * @param Config $config
     *
     * @return array|false
     */
    public function createDir($dirname, Config $config);

    /**
     * Set the visibility for a file.
     *
     * @param string $path
     * @param string $visibility
     *
     * @return array|false file meta data
     */
    public function setVisibility($path, $visibility);








    //========================

    /**
     * @param string $path
     * @param string $contents
     * @param Config $config
     * @return array|false
     */
    public function write($path, $contents, Config $config)
    {
        return $this->upload($path, $contents);
    }

    /**
     * @param string $path
     * @param resource $resource
     * @param Config $config
     * @return array|false
     */
    public function writeStream($path, $resource, Config $config)
    {
        return $this->upload($path, $resource);
    }

    /**
     * @param string $path
     * @param string $contents
     * @param Config $config
     * @return array|false
     */
    public function update($path, $contents, Config $config)
    {
        return $this->upload($path, $contents);
    }

    /**
     * @param string $path
     * @param resource $resource
     * @param Config $config
     * @return array|false
     */
    public function updateStream($path, $resource, Config $config)
    {
        return $this->upload($path, $resource);
    }

    /**
     * @param string $path
     * @param string $newPath
     * @return bool
     */
    public function rename($path, $newPath): bool
    {
        $path = $this->applyPathPrefix($path);

        $newPath = $this->applyPathPrefix($newPath);

        try {
            $this->client->move($path, $newPath);
        } catch (ObsException $e) {
            return false;
        }

        return true;
    }

    /**
     * @param string $path
     * @param string $newpath
     * @return bool
     */
    public function copy($path, $newpath): bool
    {
        $path = $this->applyPathPrefix($path);

        $newpath = $this->applyPathPrefix($newpath);

        try {
            $this->client->copy($path, $newpath);
        } catch (BadRequest $e) {
            return false;
        }

        return true;
    }

    /**
     * @param string $path
     * @return bool
     */
    public function delete($path): bool
    {
        $location = $this->applyPathPrefix($path);

        try {
            $this->client->delete($location);
        } catch (ObsException $e) {
            return false;
        }

        return true;
    }

    /**
     * @param string $dirname
     * @return bool
     */
    public function deleteDir($dirname): bool
    {
        return $this->delete($dirname);
    }

    /**
     * @param string $dirname
     * @param Config $config
     * @return array|bool|false
     */
    public function createDir($dirname, Config $config)
    {
        $path = $this->applyPathPrefix($dirname);

        try {
            $object = $this->client->createFolder($path);
        } catch (BadRequest $e) {
            return false;
        }

        return $this->normalizeResponse($object);
    }

    /**
     * @param string $path
     * @return array|bool|false|null
     */
    public function has($path)
    {
        return $this->getMetadata($path);
    }

    /**
     * @param string $path
     * @return array|bool|false
     */
    public function read($path)
    {
        if (!$object = $this->readStream($path)) {
            return false;
        }

        $object['contents'] = stream_get_contents($object['stream']);

        fclose($object['stream']);

        unset($object['stream']);

        return $object;
    }

    /**
     * @param string $path
     * @return array|bool|false
     */
    public function readStream($path)
    {
        $path = $this->applyPathPrefix($path);

        try {
            $stream = $this->client->download($path);
        } catch (BadRequest $e) {
            return false;
        }

        return compact('stream');
    }

    /**
     * @param string $directory
     * @param bool $recursive
     * @return array
     */
    public function listContents($directory = '', $recursive = false): array
    {
        $location = $this->applyPathPrefix($directory);

        try {
            $result = $this->client->listFolder($location, $recursive);
        } catch (BadRequest $e) {
            return [];
        }

        $entries = $result['entries'];
        while ($result['has_more']) {
            $result = $this->client->listFolderContinue($result['cursor']);
            $entries = array_merge($entries, $result['entries']);
        }

        if (!count($entries)) {
            return [];
        }

        return array_map(function ($entry) {
            $path = $this->removePathPrefix($entry['path_display']);
            return $this->normalizeResponse($entry, $path);
        }, $entries);
    }

    /**
     * @param string $path
     * @return array|bool|false
     */
    public function getMetadata($path)
    {
        $path = $this->applyPathPrefix($path);

        try {
            $object = $this->client->getMetadata($path);
        } catch (BadRequest $e) {
            return false;
        }

        return $this->normalizeResponse($object);
    }

    /**
     * @param string $path
     * @return array|bool|false
     */
    public function getSize($path)
    {
        return $this->getMetadata($path);
    }

    /**
     * @param string $path
     * @return array|false
     */
    public function getMimetype($path)
    {
        return ['mimetype' => MimeType::detectByFilename($path)];
    }

    /**
     * @param string $path
     * @return array|bool|false
     */
    public function getTimestamp($path)
    {
        return $this->getMetadata($path);
    }

    public function getTemporaryLink(string $path): string
    {
        return $this->client->getTemporaryLink($path);
    }

    public function getTemporaryUrl(string $path): string
    {
        return $this->getTemporaryLink($path);
    }

    public function getThumbnail(string $path, string $format = 'jpeg', string $size = 'w64h64')
    {
        return $this->client->getThumbnail($path, $format, $size);
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
