<?php
namespace Aws\Test\S3;

use Aws\S3\MultipartUploader;
use Aws\Result;
use Aws\Test\UsesServiceTrait;
use GuzzleHttp\Psr7;
use Psr\Http\Message\StreamInterface;

/**
 * @covers Aws\S3\MultipartUploader
 */
class MultipartUploaderTest extends \PHPUnit_Framework_TestCase
{
    use UsesServiceTrait;

    const MB = 1048576;
    const FILENAME = '_aws-sdk-php-s3-mup-test-dots.txt';

    public static function tearDownAfterClass()
    {
        @unlink(sys_get_temp_dir() . '/' . self::FILENAME);
    }

    /**
     * @dataProvider getTestCases
     */
    public function testS3MultipartUploadWorkflow(
        array $clientOptions = [],
        array $uploadOptions = [],
        StreamInterface $source,
        $error = false
    ) {
        $client = $this->getTestClient('s3', $clientOptions);
        $url = 'http://foo.s3.amazonaws.com/bar';
        $this->addMockResults($client, [
            new Result(['UploadId' => 'baz']),
            new Result(['ETag' => 'A']),
            new Result(['ETag' => 'B']),
            new Result(['ETag' => 'C']),
            new Result(['Location' => $url])
        ]);

        if ($error) {
            $this->setExpectedException($error);
        }

        $uploader = new MultipartUploader($client, $source, $uploadOptions);
        $result = $uploader->upload();

        $this->assertTrue($uploader->getState()->isCompleted());
        $this->assertEquals($url, $result['ObjectURL']);
    }

    public function getTestCases()
    {
        $defaults = [
            'bucket' => 'foo',
            'key'    => 'bar',
        ];

        $data = str_repeat('.', 12 * self::MB);
        $filename = sys_get_temp_dir() . '/' . self::FILENAME;
        file_put_contents($filename, $data);

        return [
            [ // Seekable stream, regular config
                [],
                ['acl' => 'private'] + $defaults,
                Psr7\stream_for(fopen($filename, 'r'))
            ],
            [ // Non-seekable stream
                [],
                $defaults,
                Psr7\stream_for($data)
            ],
            [ // Error: bad part_size
                [],
                ['part_size' => 1] + $defaults,
                Psr7\FnStream::decorate(
                    Psr7\stream_for($data), [
                        'getSize' => function () {return null;}
                    ]
                ),
                'InvalidArgumentException'
            ],
        ];
    }

    public function testCanLoadStateFromService()
    {
        $client = $this->getTestClient('s3');
        $url = 'http://foo.s3.amazonaws.com/bar';
        $this->addMockResults($client, [
            new Result(['Parts' => [
                ['PartNumber' => 1, 'ETag' => 'A', 'Size' => 4 * self::MB],
            ]]),
            new Result(['ETag' => 'B']),
            new Result(['ETag' => 'C']),
            new Result(['Location' => $url])
        ]);

        $state = MultipartUploader::getStateFromService($client, 'foo', 'bar', 'baz');
        $source = Psr7\stream_for(str_repeat('.', 9 * self::MB));
        $uploader = new MultipartUploader($client, $source, ['state' => $state]);
        $result = $uploader->upload();

        $this->assertTrue($uploader->getState()->isCompleted());
        $this->assertEquals(4 * self::MB, $uploader->getState()->getPartSize());
        $this->assertEquals($url, $result['ObjectURL']);
    }
}
