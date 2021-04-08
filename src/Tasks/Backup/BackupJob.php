<?php

namespace Spatie\Backup\Tasks\Backup;

use Carbon\Carbon;
use Exception;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Spatie\Backup\BackupDestination\BackupDestination;
use Spatie\Backup\Events\BackupHasFailed;
use Spatie\Backup\Events\BackupManifestWasCreated;
use Spatie\Backup\Events\BackupWasSuccessful;
use Spatie\Backup\Events\BackupZipWasCreated;
use Spatie\Backup\Exceptions\InvalidBackupJob;
use Spatie\DbDumper\Compressors\GzipCompressor;
use Spatie\DbDumper\Databases\MongoDb;
use Spatie\DbDumper\Databases\Sqlite;
use Spatie\DbDumper\DbDumper;
use Spatie\TemporaryDirectory\TemporaryDirectory;

class BackupJob
{
    public const FILENAME_FORMAT = 'Y-m-d_h-iA.\z\i\p';

    /** @var \Spatie\Backup\Tasks\Backup\FileSelection */
    protected $fileSelection;

    /** @var \Illuminate\Support\Collection */
    protected $dbDumpers;

    /** @var \Illuminate\Support\Collection */
    protected $backupDestinations;

    /** @var string */
    protected $filename;

    /** @var \Spatie\TemporaryDirectory\TemporaryDirectory */
    protected $temporaryDirectory;

    /** @var bool */
    protected $sendNotifications = true;

    /** @var bool */
    protected $sanitized = false;

    /** @var bool */
    protected $filter_week = false;

    /** @var bool */
    protected $filter_month = false;

    /** @var null  */
    protected $filter_start_date = null;

    /** @var null  */
    protected $filter_end_date = null;

    public function __construct()
    {
        $this->dontBackupFilesystem();
        $this->dontBackupDatabases();
        $this->setDefaultFilename();

        $this->backupDestinations = new Collection();
    }

    public function dontBackupFilesystem(): self
    {
        $this->fileSelection = FileSelection::create();

        return $this;
    }

    public function setSanitized(): self
    {
        $this->sanitized = true;

        //$this->changeFilenameWithSanitized();

        return $this;
    }

    /**
     * @param $filter_week
     *
     * @return $this
     */
    public function setFilterWeek($filter_week)
    {
        $this->filter_week = true;

        $day = Carbon::parse($filter_week);

        $this->filter_start_date = (new Carbon($day))->subWeek()->startOfWeek()->toDateString();
        $this->filter_end_date = (new Carbon($day))->subWeek()->endOfWeek()->toDateString();

        return $this;
    }

    public function getFilterWeek(): bool
    {
        return $this->filter_week;
    }

    /**
     * @param $filter_month
     *
     * @return $this
     */
    public function setFilterMonth($filter_month)
    {
        $this->filter_month = true;

        $day = Carbon::parse($filter_month);


        $this->filter_start_date = (new Carbon($day))->subMonthNoOverflow()->startOfMonth()->toDateString();
        $this->filter_end_date = (new Carbon($day))->subMonthNoOverflow()->endOfMonth()->toDateString();

        return $this;
    }


    /**
     * @return string
     */
    public function getFilterMonth(): bool
    {
        return $this->filter_month;
    }


    /**
     * @return string
     */
    public function getFilterStartDate(): string
    {
        return $this->filter_start_date;
    }


    /**
     * @return string
     */
    public function getFilterEndDate(): string
    {
        return $this->filter_end_date;
    }

    /*protected function changeFilenameWithSanitized()
    {
        $filename = $this->filename;
        $file_data = explode('.', $filename);
        $inserted = ['sanitized'];
        array_splice( $file_data, count($file_data) - 1, 0, $inserted );

        $this->filename = implode('.', $file_data);
    }*/

    public function onlyDbName(array $allowedDbNames): self
    {
        $this->dbDumpers = $this->dbDumpers->filter(
            function (DbDumper $dbDumper, string $connectionName) use ($allowedDbNames) {
                return in_array($connectionName, $allowedDbNames);
            }
        );

        return $this;
    }

    public function dontBackupDatabases(): self
    {
        $this->dbDumpers = new Collection();

        return $this;
    }

    public function disableNotifications(): self
    {
        $this->sendNotifications = false;

        return $this;
    }

    public function setDefaultFilename(): self
    {
        $this->filename = Carbon::now()->format(static::FILENAME_FORMAT);

        return $this;
    }

    public function setFileSelection(FileSelection $fileSelection): self
    {
        $this->fileSelection = $fileSelection;

        return $this;
    }

    public function setDbDumpers(Collection $dbDumpers): self
    {
        $this->dbDumpers = $dbDumpers;

        return $this;
    }

    public function setFilename(string $filename): self
    {
        $this->filename = $filename;

        return $this;
    }

    public function onlyBackupTo(string $diskName): self
    {
        $this->backupDestinations = $this->backupDestinations->filter(function (BackupDestination $backupDestination) use ($diskName) {
            return $backupDestination->diskName() === $diskName;
        });

        if (! count($this->backupDestinations)) {
            throw InvalidBackupJob::destinationDoesNotExist($diskName);
        }

        return $this;
    }

    public function setBackupDestinations(Collection $backupDestinations): self
    {
        $this->backupDestinations = $backupDestinations;

        return $this;
    }

    public function run()
    {
        $temporaryDirectoryPath = config('backup.backup.temporary_directory') ?? storage_path('app/backup-temp');

        $this->temporaryDirectory = (new TemporaryDirectory($temporaryDirectoryPath))
            ->name('temp')
            ->force()
            ->create()
            ->empty();

        try {
            if (! count($this->backupDestinations)) {
                throw InvalidBackupJob::noDestinationsSpecified();
            }

            $manifest = $this->createBackupManifest();

            if (! $manifest->count()) {
                throw InvalidBackupJob::noFilesToBeBackedUp();
            }

            $zipFile = $this->createZipContainingEveryFileInManifest($manifest);

            $this->copyToBackupDestinations($zipFile);

        } catch (Exception $exception) {
            consoleOutput()->error("Backup failed because {$exception->getMessage()}.".PHP_EOL.$exception->getTraceAsString());

            $this->sendNotification(new BackupHasFailed($exception));

            $this->temporaryDirectory->delete();

            throw $exception;
        }

        $this->temporaryDirectory->delete();
    }

    protected function createBackupManifest(): Manifest
    {
        $databaseDumps = $this->dumpDatabases();

        consoleOutput()->info('Determining files to backup...');

        $manifest = Manifest::create($this->temporaryDirectory->path('manifest.txt'))
            ->addFiles($databaseDumps)
            ->addFiles($this->filesToBeBackedUp());

        $this->sendNotification(new BackupManifestWasCreated($manifest));

        return $manifest;
    }

    public function filesToBeBackedUp()
    {
        $this->fileSelection->excludeFilesFrom($this->directoriesUsedByBackupJob());

        return $this->fileSelection->selectedFiles();
    }

    protected function directoriesUsedByBackupJob(): array
    {
        return $this->backupDestinations
            ->filter(function (BackupDestination $backupDestination) {
                return $backupDestination->filesystemType() === 'local';
            })
            ->map(function (BackupDestination $backupDestination) {
                return $backupDestination->disk()->getDriver()->getAdapter()->applyPathPrefix('').$backupDestination->backupName();
            })
            ->each(function (string $backupDestinationDirectory) {
                $this->fileSelection->excludeFilesFrom($backupDestinationDirectory);
            })
            ->push($this->temporaryDirectory->path())
            ->toArray();
    }

    protected function createZipContainingEveryFileInManifest(Manifest $manifest)
    {
        consoleOutput()->info("Zipping {$manifest->count()} files and directories...");

        $pathToZip = $this->temporaryDirectory->path(config('backup.backup.destination.filename_prefix').$this->filename);

        $zip = Zip::createForManifest($manifest, $pathToZip);

        consoleOutput()->info("Created zip containing {$zip->count()} files and directories. Size is {$zip->humanReadableSize()}");

        $this->sendNotification(new BackupZipWasCreated($pathToZip));

        return $pathToZip;
    }

    /**
     * Dumps the databases to the given directory.
     * Returns an array with paths to the dump files.
     *
     * @return array
     */
    protected function dumpDatabases(): array
    {
        $logs_tables = collect(config_ext__logs('logs_tables'))->diff([
            'telescope_monitoring',
            'telescope_entries_tags',
        ])->toArray();

        foreach ($logs_tables as $key => $table) {
            if (!Schema::connection('mysql_logs')->hasTable($table)) {
                unset($logs_tables[$key]);
            }
        }


        return $this->dbDumpers->map(function (DbDumper $dbDumper, $key) use ($logs_tables) {
            $db_name = $dbDumper->getDbName();
            $mysql_view_tables = collect(
                DB::select("
                    SHOW FULL TABLES IN {$db_name}
                        WHERE
                            TABLE_TYPE LIKE 'VIEW' OR
                            TABLES_IN_{$db_name} IN ('contact_agreement_view', 'follow_up_view', 'z_payments_recurring_items') OR
                            TABLES_IN_{$db_name} LIKE 'pt_%' OR
                            TABLES_IN_{$db_name} LIKE 'dpth_%' OR
                            TABLES_IN_{$db_name} LIKE 'dpj_%' OR
                            TABLES_IN_{$db_name} LIKE 'jatc_%' OR
                            TABLES_IN_{$db_name} LIKE 'jatce_%' OR
                            TABLES_IN_{$db_name} LIKE 'mp_%' OR
                            TABLES_IN_{$db_name} LIKE 'message_%' OR
                            TABLES_IN_{$db_name} LIKE 'mrp_%' OR
                            TABLES_IN_{$db_name} LIKE 'org_%' OR
                            TABLES_IN_{$db_name} LIKE 'py_%' OR
                            TABLES_IN_{$db_name} LIKE 'pp_%' OR
                            TABLES_IN_{$db_name} LIKE 'vm_%' OR
                            TABLES_IN_{$db_name} LIKE 'uib_%'")
            )
                ->pluck("Tables_in_{$db_name}")
                ->toArray();

            if ($key === 'mysql') {
                $dbDumper->excludeTables($mysql_view_tables);
            }

            if ($this->getFilterWeek()) {
                $dbDumper->setFilterWeek();
                $dbDumper->setFilterStartDate($this->getFilterStartDate());
                $dbDumper->setFilterEndDate($this->getFilterEndDate());
            }

            if ($this->getFilterMonth()) {
                $dbDumper->setFilterMonth();
                $dbDumper->setFilterStartDate($this->getFilterStartDate());
                $dbDumper->setFilterEndDate($this->getFilterEndDate());
            }

            consoleOutput()->info("Dumping database {$dbDumper->getDbName()} with connection ({$key})...");

            $dbType = mb_strtolower(basename(str_replace('\\', '/', get_class($dbDumper))));

            $dbName = $dbDumper->getDbName();
            if ($dbDumper instanceof Sqlite) {
                $dbName = $key.'-database';
            }

            if (
                $key === 'mysql_logs' && $this->getFilterMonth()
            ) {
                $fileName = (new Carbon($this->getFilterStartDate()))->format('Y-m');
            } elseif (
                $key === 'mysql_logs' && $this->getFilterWeek()
            ) {
                $fileName = $this->getFilterStartDate();
            } else {
                $fileName = "{$dbName}";
            }

            if ($key === 'mysql' && !$this->sanitized) {
                $this->onlyBackupTo('s3_backups');
            } elseif ($key === 'mysql' && $this->sanitized) {
                $this->onlyBackupTo('s3_backups_sanitized');
            } elseif ($key === 'mysql_logs') {
                $this->onlyBackupTo('s3_backups_logs');
            }

            $fileName .= ".{$this->getExtension($dbDumper)}";

            if (config('backup.backup.gzip_database_dump')) {
                $dbDumper->useCompressor(new GzipCompressor());
                $fileName .= '.'.$dbDumper->getCompressorExtension();
            }

            if ($compressor = config('backup.backup.database_dump_compressor')) {
                $dbDumper->useCompressor(new $compressor());
                $fileName .= '.'.$dbDumper->getCompressorExtension();
            }


            $temporaryFilePath = $this->temporaryDirectory->path('db-dumps'.DIRECTORY_SEPARATOR.$fileName);

            if ($this->sanitized) {
                $dbDumper->setDumpBinaryPath('vendor/bin');
                $dbDumper->addExtraOption('--gdpr-replacements-file="dump_sanitized.json"');

                $dbDumper->setSanitized();
            }

            $dbDumper->dumpToFile($temporaryFilePath);

            if (
                $key === 'mysql_logs'
                &&
                (
                    $this->getFilterWeek()
                    ||
                    $this->getFilterMonth()
                )
            ) {
                $dbDumper->includeTables($logs_tables);
                $dbDumper->doNotCreateTables();

                $this->clearLogTables($logs_tables);
            }

            return $temporaryFilePath;
        })->toArray();
    }

    protected function copyToBackupDestinations(string $path)
    {
        $this->backupDestinations->each(function (BackupDestination $backupDestination) use ($path) {
            try {

                if ($backupDestination->diskName() === 's3_backups_logs' && $this->getFilterMonth()) {
                    $backupDestination->appendBackupName('monthly');
                }

                if ($backupDestination->diskName() === 's3_backups_logs' && $this->getFilterWeek()) {
                    $backupDestination->appendBackupName('weekly');
                }

                if ($backupDestination->diskName() === 's3_backups_logs' && !$this->getFilterWeek() && !$this->getFilterMonth()) {
                    $backupDestination->appendBackupName('full');
                }

                consoleOutput()->info("Copying zip to disk named {$backupDestination->diskName()}...");

                $backupDestination->write($path);

                consoleOutput()->info("Successfully copied zip to disk named {$backupDestination->diskName()}.");

                $this->sendNotification(new BackupWasSuccessful($backupDestination));
            } catch (Exception $exception) {
                consoleOutput()->error("Copying zip failed because: {$exception->getMessage()}.");

                $this->sendNotification(new BackupHasFailed($exception, $backupDestination ?? null));
            }
        });
    }

    protected function sendNotification($notification)
    {
        if ($this->sendNotifications) {
            rescue(function () use ($notification) {
                event($notification);
            }, function () {
                consoleOutput()->error('Sending notification failed');
            });
        }
    }

    protected function getExtension(DbDumper $dbDumper): string
    {
        return $dbDumper instanceof MongoDb
            ? 'archive'
            : 'sql';
    }

    protected function clearLogTables(array $tables): void
    {
        $start_date = $this->getFilterStartDate();
        $end_date   = $this->getFilterEndDate();

        foreach ($tables as $table) {
            DB::connection('mysql_logs')
                ->table($table)
                ->where(function ($query) use ($start_date, $end_date) {
                    $query
                        ->whereRaw('DATE(created_at) >= ?', [$start_date])
                        ->whereRaw('DATE(created_at) <= ?', [$end_date])
                    ;
                })
                ->delete();
        }
    }
}
