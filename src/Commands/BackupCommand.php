<?php

namespace Spatie\Backup\Commands;

use Carbon\Carbon;
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
        {--filter-week=}
        {--filter-month=}
        {--number-months-saving=}
    ';

    /** @var string */
    protected $description = 'Run the backup.';

    public function handle()
    {
        consoleOutput()->comment('Starting backup...');
        $start_time = microtime(true);

        $disableNotifications = $this->option('disable-notifications');
        $number_of_months_saving = (int) ($this->option('number-months-saving') ?? config__uib('backup.logs.number_of_months_saving'));

        if ($number_of_months_saving < 0) {
            $this->error('Number of months saving is < 0.');
            return;
        }

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

            if ($this->option('filter-week')) {
                $backupJob->setFilterWeek($this->option('filter-week'));
            }

            if ($filter_month = $this->option('filter-month')) {
                $filter_month_carbon = Carbon::parse($filter_month);

                if (!$filter_month_carbon->eq(Carbon::parse($filter_month)->firstOfMonth())) {
                    $this->error('Filter month option is not first day of month.');
                    return;
                }

                $backupJob->setNumberOfMonthsSaving($number_of_months_saving);
                $backupJob->setFilterMonth($filter_month);
            }

            if ($disableNotifications) {
                $backupJob->disableNotifications();
            }

            $backupJob->run();

            $time_duration = microtime(true) - $start_time;

            $duration = new Duration((int) $time_duration);

            consoleOutput()->info('Filename: ' . $backupJob->getFileName());

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
