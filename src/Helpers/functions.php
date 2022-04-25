<?php

use Spatie\Backup\Helpers\ConsoleOutput;
use Illuminate\Support\Facades\Storage;

function consoleOutput(): ConsoleOutput
{
    return app(ConsoleOutput::class);
}

if (! function_exists('lb_s3_file_path')) {
    /**
     * @param $filepath
     *
     * @return string
     */
    function lb_s3_file_path($filepath)
    {
        $bucket = config('filesystems.disks.s3.bucket');

        if (blank($bucket)) {
            return '';
        }

        /**
         * Get SignerUrl.
         */
        $cmd = Storage::disk('s3')->getClient()->getCommand('GetObject', [
            'Bucket' => $bucket,
            'Key'    => $filepath,
        ]);

        $request = Storage::disk('s3')->getClient()->createPresignedRequest($cmd, '+20 minutes');
        $presignedUrl = (string) $request->getUri();

        return $presignedUrl;
    }
}
