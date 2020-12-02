<?php

namespace Spatie\Backup\Commands;

use Exception;
use Khill\Duration\Duration;
use Spatie\Backup\Events\BackupHasFailed;
use Spatie\Backup\Exceptions\InvalidCommand;
use Spatie\Backup\Tasks\Backup\BackupJobFactory;

class BackupCommand extends BaseCommand
{
    /** @var string */
    protected $signature = 'backup:run
        {--filename=}
        {--only-db}
        {--db-name=*}
        {--only-files}
        {--only-to-disk=}
        {--disable-notifications}
        {--timeout=}
        {--sanitized}
    ';

    /** @var string */
    protected $description = 'Run the backup.';

    public function handle()
    {
        consoleOutput()->comment('Starting backup...');
        $start_time = microtime(true);

        $disableNotifications = $this->option('disable-notifications');

        if ($this->option('timeout') && is_numeric($this->option('timeout'))) {
            set_time_limit((int) $this->option('timeout'));
        }

        try {
            $this->guardAgainstInvalidOptions();

            $backupJob = BackupJobFactory::createFromArray(config('backup'));

            if ($this->option('only-db')) {
                $backupJob->dontBackupFilesystem();
            }

            if ($this->option('db-name')) {
                $backupJob->onlyDbName($this->option('db-name'));
            }

            if ($this->option('only-files')) {
                $backupJob->dontBackupDatabases();
            }

            if ($this->option('only-to-disk')) {
                $backupJob->onlyBackupTo($this->option('only-to-disk'));
            }

            if ($this->option('filename')) {
                $backupJob->setFilename($this->option('filename'));
            }

            if ($this->option('sanitized')) {
                $backupJob->setSanitized();
            }

            if ($disableNotifications) {
                $backupJob->disableNotifications();
            }

            $backupJob->run();

            $time_duration = microtime(true) - $start_time;

            $duration = new Duration((int) $time_duration);

            consoleOutput()->comment('Backup completed!');
            consoleOutput()->comment('Time: ' . $duration->humanize());

        } catch (Exception $exception) {
            consoleOutput()->error("Backup failed because: {$exception->getMessage()}.");

            if (! $disableNotifications) {
                event(new BackupHasFailed($exception));
            }

            return 1;
        }
    }

    protected function guardAgainstInvalidOptions()
    {
        if ($this->option('only-db') && $this->option('only-files')) {
            throw InvalidCommand::create('Cannot use `only-db` and `only-files` together');
        }
    }
}
