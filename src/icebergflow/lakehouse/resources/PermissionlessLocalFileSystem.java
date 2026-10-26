package icebergflow.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Local file system for Windows hosts without Hadoop's native winutils.exe.
 *
 * <p>Hadoop's RawLocalFileSystem shells out to winutils.exe to apply POSIX permissions and to
 * read file ownership on Windows. Those permissions carry no meaning for a single-user local
 * warehouse, so this implementation skips them and derives file status from java.io.File.
 * It is only registered for the file:// scheme when IcebergFlow runs on Windows.
 */
public class PermissionlessLocalFileSystem extends LocalFileSystem {
  public PermissionlessLocalFileSystem() {
    super(new Raw());
  }

  public static class Raw extends RawLocalFileSystem {
    @Override
    public void setPermission(Path p, FsPermission permission) {}

    @Override
    public void setOwner(Path p, String username, String groupname) {}

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      return mkdirs(f);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      File file = pathToFile(f);
      if (!file.exists()) {
        throw new FileNotFoundException("File " + f + " does not exist");
      }
      return toStatus(file, f.makeQualified(getUri(), getWorkingDirectory()));
    }

    @Override
    public FileStatus getFileLinkStatus(Path f) throws IOException {
      return getFileStatus(f);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      File file = pathToFile(f);
      if (!file.exists()) {
        throw new FileNotFoundException("File " + f + " does not exist");
      }
      Path qualified = f.makeQualified(getUri(), getWorkingDirectory());
      if (!file.isDirectory()) {
        return new FileStatus[] {toStatus(file, qualified)};
      }
      String[] names = file.list();
      if (names == null) {
        throw new IOException("Cannot list " + f);
      }
      FileStatus[] result = new FileStatus[names.length];
      for (int i = 0; i < names.length; i++) {
        result[i] = toStatus(new File(file, names[i]), new Path(qualified, names[i]));
      }
      return result;
    }

    private FileStatus toStatus(File file, Path path) {
      boolean dir = file.isDirectory();
      FsPermission perm = dir ? FsPermission.getDirDefault() : FsPermission.getFileDefault();
      String user = System.getProperty("user.name");
      return new FileStatus(
          dir ? 0 : file.length(),
          dir,
          1,
          getDefaultBlockSize(path),
          file.lastModified(),
          file.lastModified(),
          perm,
          user,
          user,
          path);
    }
  }
}
