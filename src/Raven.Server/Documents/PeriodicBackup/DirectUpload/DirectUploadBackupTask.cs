using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands;
using Sparrow.Logging;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public class DirectUploadBackupTask : BackupTask
{
    private readonly BackupConfiguration.BackupDestination _destination;

    internal DirectUploadBackupTask(DocumentDatabase database, BackupParameters backupParameters,
        BackupConfiguration configuration, Logger logger, PeriodicBackupRunner.TestingStuff forTestingPurposes = null) : base(database, backupParameters, configuration, logger, forTestingPurposes)
    {
        _destination = BackupConfigurationHelper.GetBackupDestinationForDirectUpload(backupParameters.BackupToLocalFolder, configuration);
    }

    protected override Stream GetStreamForBackupDestination(string filePath, string folderName, string fileName)
    {
        switch (_destination)
        {
            case BackupConfiguration.BackupDestination.AmazonS3:
                var s3Settings = GetBackupConfigurationFromScript(Configuration.S3Settings, x => JsonDeserializationServer.S3Settings(x),
                    settings => PutServerWideBackupConfigurationCommand.UpdateSettingsForS3(settings, Database.Name));

                return new AwsS3DirectUploadStream(new DirectUploadStream<RavenAwsS3Client>.Parameters
                {
                    ClientFactory = progress => new RavenAwsS3Client(s3Settings, Database.Configuration.Backup, progress, TaskCancelToken.Token),
                    Key = BackupUploader.CombinePathAndKey(s3Settings.RemoteFolderName, folderName, fileName),
                    Metadata = new Dictionary<string, string>
                    {
                        { "Description", BackupUploader.GetBackupDescription(Configuration.BackupType, _isFullBackup) }
                    },
                    IsFullBackup = _isFullBackup,
                    RetentionPolicyParameters = RetentionPolicyParameters,
                    CloudUploadStatus = BackupResult.S3Backup,
                    OnBackupException = OnBackupException,
                    OnProgress = AddInfo,
                });

            default:
                throw new ArgumentOutOfRangeException($"Missing implementation for direct upload destination '{_destination}'");
        }
    }

    protected override void UploadToServer(string backupFilePath, string folderName, string fileName)
    {
        // uploading was already done while generating the backup file.
    }

    protected override void ValidateFreeSpaceForSnapshot(string filePath)
    {
        // we're uploading directly without using a local file.
    }

    protected override void RenameFile(string backupFilePath, string tempBackupFilePath)
    {
        // we're uploading directly without using a local file.
    }

    protected override void DeleteFile(string path)
    {
        // we're uploading directly without using a local file.
    }
}