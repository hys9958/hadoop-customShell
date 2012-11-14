/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gruter.hadoop.customShell;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomShell extends Configured implements Tool {
  private CompressionCodec snappyCodec = null;
  protected FileSystem fs;
  public static final SimpleDateFormat dateForm = 
    new SimpleDateFormat("yyyy-MM-dd HH:mm");
  protected static final SimpleDateFormat modifFmt =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  static final int BORDER =2;
  static {
    modifFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  /**
   */
  public CustomShell() {
    this(null);
  }

  public CustomShell(Configuration conf) {
    super(conf);
    fs = null;
  }
  
  protected void init() throws IOException {
    getConf().setQuietMode(true);
    if (this.fs == null) {
     this.fs = FileSystem.get(getConf());
    }
  }

    /** 
   * Print from src to stdout.
   */
  private void printToStdout(InputStream in) throws IOException {
    try {
      IOUtils.copyBytes(in, System.out, getConf(), false);
    } finally {
      in.close();
    }
  }


  /**
   * Return the {@link FileSystem} specified by src and the conf.
   * It the {@link FileSystem} supports checksum, set verifyChecksum.
   */
  private FileSystem getSrcFileSystem(Path src, boolean verifyChecksum
      ) throws IOException { 
    FileSystem srcFs = src.getFileSystem(getConf());
    srcFs.setVerifyChecksum(verifyChecksum);
    return srcFs;
  }

  /**
   * Fetch all files that match the file pattern <i>srcf</i> and display
   * their content on stdout. 
   * @param srcf: a file pattern specifying source files
   * @exception: IOException
   * @see org.apache.hadoop.fs.FileSystem.globStatus 
   */
  void cat(final String src, boolean verifyChecksum) throws IOException {
    //cat behavior in Linux
    //  [~/1207]$ ls ?.txt
    //  x.txt  z.txt
    //  [~/1207]$ cat x.txt y.txt z.txt
    //  xxx
    //  cat: y.txt: No such file or directory
    //  zzz

    Path srcPattern = new Path(src);
    new DelayedExceptionThrowing() {
      @Override
      void process(Path p, FileSystem srcFs) throws IOException {
    	  FSDataInputStream in = srcFs.open(p);
    	    /**
    	     * snappy
    	     */
    	    if(isSnappy(p.getName())){
    	    	printToStdout(getSnappyCodec().createInputStream(in));    	
    	    }else{
    	        printToStdout(in);    	    	
    	    }
    	    /**
    	     * end
    	     */
      }
    }.globAndProcess(srcPattern, getSrcFileSystem(srcPattern, verifyChecksum));
  }

  private class TextRecordInputStream extends InputStream {
    SequenceFile.Reader r;
    WritableComparable key;
    Writable val;

    DataInputBuffer inbuf;
    DataOutputBuffer outbuf;

    public TextRecordInputStream(FileStatus f) throws IOException {
      r = new SequenceFile.Reader(fs, f.getPath(), getConf());
      key = ReflectionUtils.newInstance(r.getKeyClass().asSubclass(WritableComparable.class),
                                        getConf());
      val = ReflectionUtils.newInstance(r.getValueClass().asSubclass(Writable.class),
                                        getConf());
      inbuf = new DataInputBuffer();
      outbuf = new DataOutputBuffer();
    }

    public int read() throws IOException {
      int ret;
      if (null == inbuf || -1 == (ret = inbuf.read())) {
        if (!r.next(key, val)) {
          return -1;
        }
        byte[] tmp = key.toString().getBytes();
        outbuf.write(tmp, 0, tmp.length);
        outbuf.write('\t');
        tmp = val.toString().getBytes();
        outbuf.write(tmp, 0, tmp.length);
        outbuf.write('\n');
        inbuf.reset(outbuf.getData(), outbuf.getLength());
        outbuf.reset();
        ret = inbuf.read();
      }
      return ret;
    }
  }

  private InputStream forMagic(Path p, FileSystem srcFs) throws IOException {
    FSDataInputStream i = srcFs.open(p);
    switch(i.readShort()) {
      case 0x1f8b: // RFC 1952
        i.seek(0);
        return new GZIPInputStream(i);
      case 0x5345: // 'S' 'E'
        if (i.readByte() == 'Q') {
          i.close();
          return new TextRecordInputStream(srcFs.getFileStatus(p));
        }
        break;
    }
    i.seek(0);
    /**
     * snappy
     */
    if(isSnappy(p.getName())){
    	return getSnappyCodec().createInputStream(i);    	
    }
    /**
     * end
     */
    return i;
  }

  void text(String srcf) throws IOException {
    Path srcPattern = new Path(srcf);
    new DelayedExceptionThrowing() {
      @Override
      void process(Path p, FileSystem srcFs) throws IOException {
        if (srcFs.isDirectory(p)) {
          throw new IOException("Source must be a file.");
        }
        printToStdout(forMagic(p, srcFs));
      }
    }.globAndProcess(srcPattern, srcPattern.getFileSystem(getConf()));
  }
    
  /**
   * Get a listing of all files in that match the file pattern <i>srcf</i>.
   * @param srcf a file pattern specifying source files
   * @param recursive if need to list files in subdirs
   * @throws IOException  
   * @see org.apache.hadoop.fs.FileSystem#globStatus(Path)
   */
  private int ls(String srcf, boolean recursive) throws IOException {
    Path srcPath = new Path(srcf);
    FileSystem srcFs = srcPath.getFileSystem(this.getConf());
    FileStatus[] srcs = srcFs.globStatus(srcPath);
    if (srcs==null || srcs.length==0) {
      throw new FileNotFoundException("Cannot access " + srcf + 
          ": No such file or directory.");
    }
 
    boolean printHeader = (srcs.length == 1) ? true: false;
    int numOfErrors = 0;
    for(int i=0; i<srcs.length; i++) {
      numOfErrors += ls(srcs[i], srcFs, recursive, printHeader);
    }
    return numOfErrors == 0 ? 0 : -1;
  }

  /* list all files under the directory <i>src</i>
   * ideally we should provide "-l" option, that lists like "ls -l".
   */
  private int ls(FileStatus src, FileSystem srcFs, boolean recursive,
      boolean printHeader) throws IOException {
    final String cmd = recursive? "lsr": "ls";
    final FileStatus[] items = shellListStatus(cmd, srcFs, src);
    if (items == null) {
      return 1;
    } else {
      int numOfErrors = 0;
      if (!recursive && printHeader) {
        if (items.length != 0) {
          System.out.println("Found " + items.length + " items");
        }
      }
      
      int maxReplication = 3, maxLen = 10, maxOwner = 0,maxGroup = 0;

      for(int i = 0; i < items.length; i++) {
        FileStatus stat = items[i];
        int replication = String.valueOf(stat.getReplication()).length();
        int len = String.valueOf(stat.getLen()).length();
        int owner = String.valueOf(stat.getOwner()).length();
        int group = String.valueOf(stat.getGroup()).length();
        
        if (replication > maxReplication) maxReplication = replication;
        if (len > maxLen) maxLen = len;
        if (owner > maxOwner)  maxOwner = owner;
        if (group > maxGroup)  maxGroup = group;
      }
      
      for (int i = 0; i < items.length; i++) {
        FileStatus stat = items[i];
        Path cur = stat.getPath();
        String mdate = dateForm.format(new Date(stat.getModificationTime()));
        
        System.out.print((stat.isDir() ? "d" : "-") + 
          stat.getPermission() + " ");
        System.out.printf("%"+ maxReplication + 
          "s ", (!stat.isDir() ? stat.getReplication() : "-"));
        if (maxOwner > 0)
          System.out.printf("%-"+ maxOwner + "s ", stat.getOwner());
        if (maxGroup > 0)
          System.out.printf("%-"+ maxGroup + "s ", stat.getGroup());
        System.out.printf("%"+ maxLen + "d ", stat.getLen());
        System.out.print(mdate + " ");
        System.out.println(cur.toUri().getPath());
        if (recursive && stat.isDir()) {
          numOfErrors += ls(stat,srcFs, recursive, printHeader);
        }
      }
      return numOfErrors;
    }
  }

  /**
   * This class runs a command on a given FileStatus. This can be used for
   * running various commands like chmod, chown etc.
   */
  static abstract class CmdHandler {
    
    protected int errorCode = 0;
    protected boolean okToContinue = true;
    protected String cmdName;
    
    int getErrorCode() { return errorCode; }
    boolean okToContinue() { return okToContinue; }
    String getName() { return cmdName; }
    
    protected CmdHandler(String cmdName, FileSystem fs) {
      this.cmdName = cmdName;
    }
    
    public abstract void run(FileStatus file, FileSystem fs) throws IOException;
  }
  
  /** helper returns listStatus() */
  private static FileStatus[] shellListStatus(String cmd, 
                                                   FileSystem srcFs,
                                                   FileStatus src) {
    if (!src.isDir()) {
      FileStatus[] files = { src };
      return files;
    }
    Path path = src.getPath();
    try {
      FileStatus[] files = srcFs.listStatus(path);
      if ( files == null ) {
        System.err.println(cmd + 
                           ": could not get listing for '" + path + "'");
      }
      return files;
    } catch (IOException e) {
      System.err.println(cmd + 
                         ": could not get get listing for '" + path + "' : " +
                         e.getMessage().split("\n")[0]);
    }
    return null;
  }
  
  
  /**
   * Runs the command on a given file with the command handler. 
   * If recursive is set, command is run recursively.
   */                                       
  private static int runCmdHandler(CmdHandler handler, FileStatus stat, 
                                   FileSystem srcFs, 
                                   boolean recursive) throws IOException {
    int errors = 0;
    handler.run(stat, srcFs);
    if (recursive && stat.isDir() && handler.okToContinue()) {
      FileStatus[] files = shellListStatus(handler.getName(), srcFs, stat);
      if (files == null) {
        return 1;
      }
      for(FileStatus file : files ) {
        errors += runCmdHandler(handler, file, srcFs, recursive);
      }
    }
    return errors;
  }

  ///top level runCmdHandler
  int runCmdHandler(CmdHandler handler, String[] args,
                                   int startIndex, boolean recursive) 
                                   throws IOException {
    int errors = 0;
    
    for (int i=startIndex; i<args.length; i++) {
      Path srcPath = new Path(args[i]);
      FileSystem srcFs = srcPath.getFileSystem(getConf());
      Path[] paths = FileUtil.stat2Paths(srcFs.globStatus(srcPath), srcPath);
      // if nothing matches to given glob pattern then increment error count
      if(paths.length==0) {
        System.err.println(handler.getName() + 
            ": could not get status for '" + args[i] + "'");
        errors++;
      }
      for(Path path : paths) {
        try {
          FileStatus file = srcFs.getFileStatus(path);
          if (file == null) {
            System.err.println(handler.getName() + 
                               ": could not get status for '" + path + "'");
            errors++;
          } else {
            errors += runCmdHandler(handler, file, srcFs, recursive);
          }
        } catch (IOException e) {
          String msg = (e.getMessage() != null ? e.getLocalizedMessage() :
            (e.getCause().getMessage() != null ? 
                e.getCause().getLocalizedMessage() : "null"));
          System.err.println(handler.getName() + ": could not get status for '"
                                        + path + "': " + msg.split("\n")[0]);
          errors++;
        }
      }
    }
    
    return (errors > 0 || handler.getErrorCode() != 0) ? 1 : 0;
  }
  
  private void printHelp(String cmd) {
    String summary = "hadoop fs is the command to execute fs commands. " +
      "The full syntax is: \n\n" +
      "hadoop fs [-fs <local | file system URI>] [-conf <configuration file>]\n\t" +
      "[-D <property=value>] [-ls <path>]\n\t" + 
      "[-cat <src>]\n\t" +
      "[-help [cmd]]\n";

    String conf ="-conf <configuration file>:  Specify an application configuration file.";
 
    String D = "-D <property=value>:  Use value for given property.";
  
    String fs = "-fs [local | <file system URI>]: \tSpecify the file system to use.\n" + 
      "\t\tIf not specified, the current configuration is used, \n" +
      "\t\ttaken from the following, in increasing precedence: \n" + 
      "\t\t\tcore-default.xml inside the hadoop jar file \n" +
      "\t\t\tcore-site.xml in $HADOOP_CONF_DIR \n" +
      "\t\t'local' means use the local file system as your DFS. \n" +
      "\t\t<file system URI> specifies a particular file system to \n" +
      "\t\tcontact. This argument is optional but if used must appear\n" +
      "\t\tappear first on the command line.  Exactly one additional\n" +
      "\t\targument must be specified. \n";

        
    String ls = "-ls <path>: \tList the contents that match the specified file pattern. If\n" + 
      "\t\tpath is not specified, the contents of /user/<currentUser>\n" +
      "\t\twill be listed. Directory entries are of the form \n" +
      "\t\t\tdirName (full path) <dir> \n" +
      "\t\tand file entries are of the form \n" + 
      "\t\t\tfileName(full path) <r n> size \n" +
      "\t\twhere n is the number of replicas specified for the file \n" + 
      "\t\tand size is the size of the file, in bytes.\n";

    String cat = "-cat <src>: \tFetch all files that match the file pattern <src> \n" +
      "\t\tand display their content on stdout.\n";

    
    String text = "-text <src>: \tTakes a source file and outputs the file in text format.\n" +
      "\t\tThe allowed formats are zip and TextRecordInputStream.\n";
         
    
    String help = "-help [cmd]: \tDisplays help for given command or all commands if none\n" +
      "\t\tis specified.\n";

    if ("fs".equals(cmd)) {
      System.out.println(fs);
    } else if ("conf".equals(cmd)) {
      System.out.println(conf);
    } else if ("D".equals(cmd)) {
      System.out.println(D);
    } else if ("ls".equals(cmd)) {
      System.out.println(ls);
    } else if ("text".equals(cmd)) {
      System.out.println(text);
    } else if ("cat".equals(cmd)) {
      System.out.println(cat);
    }else if ("help".equals(cmd)) {
      System.out.println(help);
    } else {
      System.out.println(summary);
      System.out.println(fs);
      System.out.println(ls);
      System.out.println(cat);
      System.out.println(text);
      System.out.println(help);
    }                                
  }

  /**
   * Apply operation specified by 'cmd' on all parameters
   * starting from argv[startindex].
   */
  private int doall(String cmd, String argv[], int startindex) {
    int exitCode = 0;
    int i = startindex;
    boolean rmSkipTrash = false;
    
    // Check for -skipTrash option in rm/rmr
    if(("-rm".equals(cmd) || "-rmr".equals(cmd)) 
        && "-skipTrash".equals(argv[i])) {
      rmSkipTrash = true;
      i++;
    }

    //
    // for each source file, issue the command
    //
    for (; i < argv.length; i++) {
      try {
        //
        // issue the command to the fs
        //
        if ("-cat".equals(cmd)) {
          cat(argv[i], true);
        } else if ("-ls".equals(cmd)) {
          exitCode = ls(argv[i], false);
        } else if ("-text".equals(cmd)) {
          text(argv[i]);
        }
      } catch (RemoteException e) {
        //
        // This is a error returned by hadoop server. Print
        // out the first line of the error message.
        //
        exitCode = -1;
        try {
          String[] content;
          content = e.getLocalizedMessage().split("\n");
          System.err.println(cmd.substring(1) + ": " +
                             content[0]);
        } catch (Exception ex) {
          System.err.println(cmd.substring(1) + ": " +
                             ex.getLocalizedMessage());
        }
      } catch (IOException e) {
        //
        // IO exception encountered locally.
        //
        exitCode = -1;
        String content = e.getLocalizedMessage();
        if (content != null) {
          content = content.split("\n")[0];
        }
        System.err.println(cmd.substring(1) + ": " +
                          content);
      }
    }
    return exitCode;
  }

  /**
   * Displays format of commands.
   * 
   */
  private static void printUsage(String cmd) {
    if ("-fs".equals(cmd)) {
      System.err.println("Usage: java CustomShell" + 
                         " [-fs <local | file system URI>]");
    } else if ("-conf".equals(cmd)) {
      System.err.println("Usage: java CustomShell" + 
                         " [-conf <configuration file>]");
    } else if ("-D".equals(cmd)) {
      System.err.println("Usage: java CustomShell" + 
                         " [-D <[property=value>]");
    } else if ("-ls".equals(cmd) || "-lsr".equals(cmd) ||
               "-text".equals(cmd)) {
      System.err.println("Usage: java CustomShell" + 
                         " [" + cmd + " <path>]");
    } else if ("-cat".equals(cmd)) {
      System.err.println("Usage: java CustomShell" + 
                         " [" + cmd + " <src>]");
    } else {
      System.err.println("Usage: java CustomShell");
      System.err.println("           [-ls <path>]");
      System.err.println("           [-cat <src>]");
      System.err.println("           [-text <src>]");
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  /**
   * run
   */
  public int run(String argv[]) throws Exception {

    if (argv.length < 1) {
      printUsage(""); 
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];

    //
    // verify that we have enough command line parameters
    //


    if ("-cat".equals(cmd) || "-text".equals(cmd)) {
      if (argv.length < 2) {
        printUsage(cmd);
        return exitCode;
      }
    }

    // initialize CustomShell
    try {
      init();
    } catch (RPC.VersionMismatch v) { 
      System.err.println("Version Mismatch between client and server" +
                         "... command aborted.");
      return exitCode;
    } catch (IOException e) {
      System.err.println("Bad connection to FS. command aborted. exception: " +
          e.getLocalizedMessage());
      return exitCode;
    }

    exitCode = 0;
    try {
      if ("-cat".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-text".equals(cmd)) {
        exitCode = doall(cmd, argv, i);
      } else if ("-ls".equals(cmd)) {
        if (i < argv.length) {
          exitCode = doall(cmd, argv, i);
        } else {
          exitCode = ls(Path.CUR_DIR, false);
        } 
      } else if ("-help".equals(cmd)) {
        if (i < argv.length) {
          printHelp(argv[i]);
        } else {
          printHelp("");
        }
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(cmd);
    } catch (RemoteException e) {
      //
      // This is a error returned by hadoop server. Print
      // out the first line of the error mesage, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(cmd.substring(1) + ": " + 
                           content[0]);
      } catch (Exception ex) {
        System.err.println(cmd.substring(1) + ": " + 
                           ex.getLocalizedMessage());  
      }
    } catch (IOException e) {
      //
      // IO exception encountered locally.
      // 
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + 
                         e.getLocalizedMessage());  
    } catch (Exception re) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + re.getLocalizedMessage());  
    } finally {
    }
    return exitCode;
  }

  public void close() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
  }

  /**
   * main() has some simple utility methods
   */
  public static void main(String argv[]) throws Exception {
    CustomShell shell = new CustomShell();
    int res;
    try {
      res = ToolRunner.run(shell, argv);
    } finally {
      shell.close();
    }
    System.exit(res);
  }

  /**
   * Accumulate exceptions if there is any.  Throw them at last.
   */
  private abstract class DelayedExceptionThrowing {
    abstract void process(Path p, FileSystem srcFs) throws IOException;

    final void globAndProcess(Path srcPattern, FileSystem srcFs
        ) throws IOException {
      List<IOException> exceptions = new ArrayList<IOException>();
      for(Path p : FileUtil.stat2Paths(srcFs.globStatus(srcPattern), 
                                       srcPattern))
        try { process(p, srcFs); } 
        catch(IOException ioe) { exceptions.add(ioe); }
    
      if (!exceptions.isEmpty())
        if (exceptions.size() == 1)
          throw exceptions.get(0);
        else 
          throw new IOException("Multiple IOExceptions: " + exceptions);
    }
  }
  /**
   * snappy codec.
   */
	private void setCodec(Configuration conf){
		this.snappyCodec = ReflectionUtils.newInstance(SnappyCodec.class, conf);
	}
	private CompressionCodec getSnappyCodec() {
	    if(this.snappyCodec == null){
	    	setCodec(getConf());
	    }
	    return this.snappyCodec;
	}
	private boolean isSnappy(String fileName){
		boolean result = false;
	    int pos = fileName.lastIndexOf('.');
	    if(pos != -1){
	        String extension = fileName.substring(pos + 1);
	        if(extension.equals("snappy")){
	        	result = true;
	        }
	    }
	    return result;
	}
}
