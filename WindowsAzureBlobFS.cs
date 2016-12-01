// DESCRIPTION
// -----------
// **WindowsAzureBlobFS** is a [Dokan](http://dokan-dev.net/en) file system provider for Windows Azure blob storage accounts.
// Full source code is at [http://github.com/smarx/BlobMount](http://github.com/smarx/BlobMount).

// USAGE
// -----
// To use it, install [Dokan](http://dokan-dev.net/en/download/#dokan) and run
// `BlobMount <mount-point> <account> <key>`. E.g. `BlobMount w:\ myaccount EbhC5+NTN...==`

// Top-level directories under the mount point are containers in blob storage. Lower-level directories are blob prefixes ('/'-delimited).
// Files are blobs.

// **NOTE:** Because Windows Azure blob storage doesn't have a notion of explicit "directories," empty directories are currently
// ephemeral. They don't exist in blob storage, but only in memory for the duration of the mounted drive. That means any empty directories
// will vanish when the drive is unmounted. It may be better to do something like persist the "empty directories" via blobs with metadata
// (IsEmptyDirectory=true).

// SCARY STATUS
// ------------
// Plenty of work left. See all the TODOs in the code. :-) This is alpha code and probably works for a single user editing some blobs. Don't hold me
// responsible for data loss... have a backup for sure. Lots of places in this code only implement the paths I've tested. Things like
// seeking within a file during a write may totally destroy data. Consider yourself warned.
using System;
using System.Collections.Generic;
using System.Linq;
using DokanNet;
using System.IO;
using System.Security.AccessControl;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Shared.Protocol;
using FileAccess = DokanNet.FileAccess;

namespace BlobMount
{
    public class WindowsAzureBlobFS : IDokanOperations
    {
        // Mount takes the following arguments:
        //
        // * Mount point
        // * Storage account
        // * Storage key
        //
        // Translates Dokan error messages to friendly text.
        public static void Mount(string[] args)
        {
            try
            {
                var fs = new WindowsAzureBlobFS(args[1]);
                fs.Mount(args[0], DokanOptions.DebugMode | DokanOptions.StderrOutput);
                Console.WriteLine("Success");
            }
            catch (DokanException e)
            {
                Console.WriteLine("Error: " + e.Message);
            }
        }

        private CloudBlobClient blobs;

        // Initialize with a cloud storage account and key.
        public WindowsAzureBlobFS(string connectionString)
        {
            blobs = CloudStorageAccount.Parse(connectionString).CreateCloudBlobClient();
        }

        // Close the corresponding BlobStream, if any, to commit the changes.
        public void Cleanup(string fileName, DokanFileInfo info)
        {
            if (readBlobs.ContainsKey(fileName)) readBlobs[fileName].Close();
            if (writeBlobs.ContainsKey(fileName)) writeBlobs[fileName].Close();
        }

        // Call close on the underlying BlobStream (if any) so writes are committed and buffered reads are discarded.
        public void CloseFile(string fileName, DokanFileInfo info)
        {
            if (writeBlobs.ContainsKey(fileName))
            {
                writeBlobs[fileName].Close();
                writeBlobs.Remove(fileName);
            }
            if (readBlobs.ContainsKey(fileName))
            {
                readBlobs[fileName].Close();
                readBlobs.Remove(fileName);
            }
        }

        // Keep track of empty directories (in memory).
        private HashSet<string> emptyDirectories = new HashSet<string>();
        private IEnumerable<string> EnumeratePathAndParents(string directory)
        {
            var split = directory.Split(new[] { '\\' }, StringSplitOptions.RemoveEmptyEntries);
            var path = "\\" + split.FirstOrDefault() ?? "";
            foreach (var segment in split.Skip(1))
            {
                path += "\\" + segment;
                yield return path;
            }
        }

        private bool AddEmptyDirectories(string directory)
        {
            bool alreadyExisted = false;
            foreach (var path in EnumeratePathAndParents(directory))
            {
                alreadyExisted |= emptyDirectories.Add(path);
            }
            Console.WriteLine("emptyDirectories:\n\t" + string.Join("\n\t", emptyDirectories));
            return alreadyExisted;
        }

        private void RemoveEmptyDirectories(string directory)
        {
            foreach (var path in EnumeratePathAndParents(directory))
            {
                emptyDirectories.Remove(path);
            }
            Console.WriteLine("emptyDirectories:\n\t" + string.Join("\n\t", emptyDirectories));
        }

        // CreateDirectory creates a container if at the top level and otherwise creates an empty directory (tracked in memory only).
        private NtStatus CreateDirectory(string fileName, DokanFileInfo info)
        {
            var split = fileName.Split(new[] { Path.DirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries);

            if (split.Length == 1)
            {
                try
                {
                    blobs.GetContainerReference(split[0]).Create();
                }
                catch (StorageException e)
                {
                    if (e.RequestInformation.ExtendedErrorInformation.ErrorCode == StorageErrorCodeStrings.ContainerAlreadyExists)
                    {
                        // TODO: This doesn't seem to give the right error message when I try to "md" a container that already exists.
                        return DokanResult.AlreadyExists;
                    }
                    throw;
                }
                return 0;
            }
            else
            {
                // Use OpenDirectory as a way to test for existence of a directory.
                if (OpenDirectory(fileName, info) == NtStatus.Success)
                {
                    return DokanResult.AlreadyExists;
                }
                else
                {
                    // Track the empty directory in memory.
                    if (!AddEmptyDirectories(fileName))
                    {
                        return DokanResult.AlreadyExists;
                    }
                    else
                    {
                        return DokanResult.Success;
                    }
                }
            }
        }

        // Create file does nothing except validate that the file requested is okay to create.
        // Actual creation of a corresponding blob in cloud storage is done when the file is actually written.
        public NtStatus CreateFile(string fileName, FileAccess access, FileShare share, FileMode mode, FileOptions options, FileAttributes attributes, DokanFileInfo info)
        {
            if (info.IsDirectory)
            {
                return CreateDirectory(fileName, info);
            }

            // When trying to open a file for reading, succeed only if the file already exists.
            if (mode == FileMode.Open && (access == FileAccess.GenericRead || access == FileAccess.GenericWrite))
            {
                FileInformation fileInfo;
                if (GetFileInformation(fileName, out fileInfo, info) == 0)
                {
                    return 0;
                }
                else
                {
                    return DokanResult.FileNotFound;
                }
            }
            // When creating a file, always succeed. (Empty directories will be implicitly created as needed.)
            else if (mode == FileMode.Create || mode == FileMode.OpenOrCreate)
            {
                // Since we're creating a file, we don't need to track the parents (up the tree) as empty directories any longer.
                RemoveEmptyDirectories(Path.GetDirectoryName(fileName));
                return 0;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        // DeleteDirectory removes a container if it's at the root level, fails if a directory is not empty,
        // and removes a tracked empty directory if one exists.
        public NtStatus DeleteDirectory(string fileName, DokanFileInfo info)
        {
            var split = fileName.Trim('\\').Split(Path.DirectorySeparatorChar);
            if (split.Length == 1)
            {
                try
                {
                    blobs.GetContainerReference(split[0]).Delete();
                    return 0;
                }
                catch { return DokanResult.Error; }
            }
            if (blobs.ListBlobs(fileName.Trim('\\').Replace('\\', '/')).Any())
            {
                // TODO: Revisit what a better error code might be.
                return DokanResult.Error;
            }
            else if (emptyDirectories.Any(f => f.StartsWith(fileName + "\\")))
            {
                // TODO: Revisit what a better error code might be.
                return DokanResult.Error;
            }
            else
            {
                emptyDirectories.Remove(fileName);
                return DokanResult.Success;
            }
        }

        // DeleteFile tries to delete the corresponding blob and ensures the parent directory is tracked as empty.
        public NtStatus DeleteFile(string fileName, DokanFileInfo info)
        {
            try
            {
                var blob = blobs.GetBlobReferenceFromServer(blobs.ListBlobs(fileName.Trim('\\')).First().StorageUri);
                blob.Delete();
                if (!blobs.ListBlobs(Path.GetDirectoryName(fileName).Trim('\\').Replace('\\', '/')).Any())
                {
                    AddEmptyDirectories(Path.GetDirectoryName(fileName));
                }
                return DokanResult.Success;
            }
            catch { return DokanResult.Error; }
        }

        // FindFiles enumerates blobs and blob prefixes and represents them as files and directories.
        public NtStatus FindFiles(string fileName, out IList<FileInformation> files, DokanFileInfo info)
        {
            files = new List<FileInformation>();

            if (fileName == "\\")
            {
                foreach (var blob in blobs.ListContainers())
                {
                    files.Add(new FileInformation
                    {
                        FileName = blob.Name,
                        Attributes = FileAttributes.Directory,
                        CreationTime = blob.Properties.LastModified?.UtcDateTime,
                        LastAccessTime = blob.Properties.LastModified?.UtcDateTime,
                        LastWriteTime = blob.Properties.LastModified?.UtcDateTime
                    });
                }
            }
            else
            {
                var split = fileName.Trim('\\').Split(Path.DirectorySeparatorChar);
                var container = blobs.GetContainerReference(split[0]);
                IEnumerable<IListBlobItem> items =
                    split.Length > 1
                        ? container.GetDirectoryReference(string.Join("/", split.Skip(1).Take(split.Length - 1)))
                            .ListBlobs()
                        : container.ListBlobs();

                foreach (var blob in items)
                {
                    files.Add(new FileInformation
                    {
                        FileName = blob.Uri.AbsolutePath.Substring(fileName.Length + 1).TrimEnd('/'),
                        Attributes = (blob is CloudBlobDirectory) ? FileAttributes.Directory : FileAttributes.Normal,
                        CreationTime = (blob is CloudBlob) ? ((CloudBlob)blob).Properties.LastModified?.UtcDateTime : DateTime.UtcNow,
                        LastAccessTime = (blob is CloudBlob) ? ((CloudBlob)blob).Properties.LastModified?.UtcDateTime : DateTime.UtcNow,
                        LastWriteTime = (blob is CloudBlob) ? ((CloudBlob)blob).Properties.LastModified?.UtcDateTime : DateTime.UtcNow,
                        Length = (blob is CloudBlob) ? ((CloudBlob)blob).Properties.Length : 0
                    });
                }
                foreach (
                    var dir in
                    emptyDirectories.Where(
                        f => f.StartsWith(fileName + "\\") && !f.Substring(fileName.Length + 1).Contains("\\")))
                {
                    files.Add(new FileInformation
                    {
                        FileName = dir.Substring(fileName.Length + 1),
                        Attributes = FileAttributes.Directory,
                        CreationTime = DateTime.UtcNow,
                        LastAccessTime = DateTime.UtcNow,
                        LastWriteTime = DateTime.UtcNow,
                        Length = 0
                    });
                }
            }
            return DokanResult.Success;
        }

        public NtStatus FlushFileBuffers(string fileName, DokanFileInfo info)
        {
            return DokanResult.Success;
        }

        // GetDiskFreeSpace returns hardcoded values.
        public NtStatus GetDiskFreeSpace(out long freeBytesAvailable, out long totalNumberOfBytes, out long totalNumberOfFreeBytes, DokanFileInfo info)
        {
            freeBytesAvailable = 512 * 1024 * 1024;
            totalNumberOfBytes = 1024 * 1024 * 1024;
            totalNumberOfFreeBytes = 512 * 1024 * 1024;
            return DokanResult.Success;
        }

        // GetFileInformation returns information about a container at the top level, blob prefixes at lower levels,
        // and empty directories (tracked in memory). File times are all specified as `DateTime.UtcNow`.
        public NtStatus GetFileInformation(string fileName, out FileInformation fileInfo, DokanFileInfo info)
        {
            fileInfo = new FileInformation();
            var split = fileName.Split(new[] { Path.DirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries);
            if (split.Length == 0)
            {
                fileInfo.Attributes = FileAttributes.Directory;
                fileInfo.CreationTime = DateTime.UtcNow;
                fileInfo.LastAccessTime = DateTime.UtcNow;
                fileInfo.LastWriteTime = DateTime.UtcNow;
                return 0;
            }
            if (split.Length == 1)
            {
                var container = blobs.ListContainers(split[0]).FirstOrDefault();
                if (container != null && container.Name == split[0])
                {
                    fileInfo.Attributes = FileAttributes.Directory;
                    fileInfo.CreationTime = DateTime.UtcNow;
                    fileInfo.LastAccessTime = DateTime.UtcNow;
                    fileInfo.LastWriteTime = DateTime.UtcNow;
                    return 0;
                }
                else
                {
                    return DokanResult.Error;
                }
            }
            var blob = blobs.GetBlobReferenceFromServer(blobs.ListBlobs(fileName.Trim('\\')).First().StorageUri);
            try
            {
                blob.FetchAttributes();
                fileInfo.CreationTime = blob.Properties.LastModified?.UtcDateTime;
                fileInfo.LastWriteTime = blob.Properties.LastModified?.UtcDateTime;
                fileInfo.LastAccessTime = DateTime.UtcNow;
                fileInfo.Length = blob.Properties.Length;
                return 0;
            }
            catch
            {
                if (emptyDirectories.Contains(fileName) || blobs.ListBlobs(fileName.Trim('\\').Replace('\\', '/')).Any())
                {
                    fileInfo.Attributes = FileAttributes.Directory;
                    fileInfo.CreationTime = DateTime.UtcNow;
                    fileInfo.LastAccessTime = DateTime.UtcNow;
                    fileInfo.LastWriteTime = DateTime.UtcNow;
                    return 0;
                }
                else
                {
                    return DokanResult.FileNotFound;
                }
            }
        }

        public NtStatus FindFilesWithPattern(string fileName, string searchPattern, out IList<FileInformation> files, DokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus FindStreams(string fileName, out IList<FileInformation> streams, DokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus GetFileSecurity(string fileName, out FileSystemSecurity security, AccessControlSections sections, DokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus GetVolumeInformation(out string volumeLabel, out FileSystemFeatures features, out string fileSystemName, DokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        // TODO: Perhaps use leases on blobs to do locking?
        public NtStatus LockFile(string fileName, long offset, long length, DokanFileInfo info)
        {
            return DokanResult.Success;
        }

        public NtStatus Mounted(DokanFileInfo info)
        {
            return DokanResult.Success;
        }

        // TODO: Use copy and then delete to do an efficient move where possible.
        public NtStatus MoveFile(string oldName, string newName, bool replace, DokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        // OpenDirectory succeeds as long as the specified path is an empty directory (tracked in memory)
        // or a blob prefix containing blobs (discovered via GetFileInformation returning a success code with a directory).
        private NtStatus OpenDirectory(string fileName, DokanFileInfo info)
        {
            if (emptyDirectories.Contains(fileName)) return 0;
            FileInformation fileinfo;
            var status = GetFileInformation(fileName, out fileinfo, info);
            if ((status == 0) && (fileinfo.Attributes == FileAttributes.Directory)) return 0;
            return DokanResult.FileNotFound;
        }

        // Keep track of open streams for reading blobs. (Done this way instead of making an HTTP call for each read
        // operation so that we can do read-ahead (significant perf gain for normal operations like reading an entire file).
        private Dictionary<string, Stream> readBlobs = new Dictionary<string, Stream>();
        public NtStatus ReadFile(string fileName, byte[] buffer, out int bytesRead, long offset, DokanFileInfo info)
        {
            if (!readBlobs.ContainsKey(fileName))
            {
                var blobUri = blobs.ListBlobs(fileName.Trim('\\')).First().StorageUri;
                readBlobs[fileName] = blobs.GetBlobReferenceFromServer(blobUri).OpenRead();
            }
            readBlobs[fileName].Position = offset;
            bytesRead = readBlobs[fileName].Read(buffer, 0, buffer.Length);
            return DokanResult.Success;
        }

        // TODO: Figure out what this is supposed to do and maybe do it. :)
        public NtStatus SetAllocationSize(string fileName, long length, DokanFileInfo info)
        {
            return 0;
        }

        // TODO: This is presumably to truncate a file? This could be implemented in the future.
        public NtStatus SetEndOfFile(string fileName, long length, DokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        // TODO: Consider implementing this.
        public NtStatus SetFileAttributes(string fileName, FileAttributes attributes, DokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        public NtStatus SetFileSecurity(string fileName, FileSystemSecurity security, AccessControlSections sections, DokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        // SetFileTime isn't supported, since we don't actually track meaningful times for most of this.
        public NtStatus SetFileTime(string fileName, DateTime? creationTime, DateTime? lastAccessTime, DateTime? lastWriteTime,
            DokanFileInfo info)
        {
            throw new NotImplementedException();
        }

        // TODO: Perhaps use leases on blobs to do locking?
        public NtStatus UnlockFile(string fileName, long offset, long length, DokanFileInfo info)
        {
            return DokanResult.Success;
        }

        // Close everything.
        public NtStatus Unmounted(DokanFileInfo info)
        {
            foreach (var filename in readBlobs.Keys) CloseFile(filename, info);
            foreach (var filename in writeBlobs.Keys) CloseFile(filename, info);
            return DokanResult.Success;
        }

        private Dictionary<string, Stream> writeBlobs = new Dictionary<string, Stream>();
        public NtStatus WriteFile(string fileName, byte[] buffer, out int bytesWritten, long offset, DokanFileInfo info)
        {
            if (!writeBlobs.ContainsKey(fileName))
            {
                var blobUri = blobs.ListBlobs(fileName.TrimStart('\\')).First().StorageUri;
                var blob = blobs.GetBlobReferenceFromServer(blobUri).Container.GetBlockBlobReference(fileName.TrimStart('\\'));
                writeBlobs[fileName] = blob.OpenWrite();
                if (offset != 0)
                {
                    blob.FetchAttributes();
                    if (offset == blob.Properties.Length)
                    {
                        // TODO: This is a really inefficient way to do this.
                        // The right thing to do is to start writing new blocks and then commit *old blocks* + *new blocks*.
                        // This method is okay for small files and "works."
                        var previousBytes = new byte[blob.Properties.Length];
                        blob.DownloadToByteArray(previousBytes, 0);
                        writeBlobs[fileName].Write(previousBytes, 0, previousBytes.Length);
                    }
                    else
                    {
                        // TODO: Handle arbitrary seeks during writing.
                        throw new NotImplementedException();
                    }
                }
            }
            writeBlobs[fileName].Write(buffer, 0, buffer.Length);
            bytesWritten = buffer.Length;
            return DokanResult.Success;
        }
    }

}
