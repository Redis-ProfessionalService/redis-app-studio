/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.redis.foundation.std;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * The FilUtl class provides utility methods for copying and zipping files.
 * The goal of this class is to centralize commonly used file operations
 * and to encourage code re-use.
 * <p>
 * <b>Note:</b> The Apache Commons class library contains a large collection
 * of methods that perform similar tasks.  Consult this class library before
 * adding any new methods to this class.
 * </p>
 * <p>
 * http://commons.apache.org/io/description.html - Basic I/O operations
 * http://commons.apache.org/compress/ - Special compression features
 * </p>
 *
 * @author Al Cole
 * @version 1.0 Jan 4, 2014
 * @since 1.0
 */
public class FilUtl
{
    static final int FILE_IO_BUFFER_SIZE = 8192;

    /**
     * Convenience method that determines if a path/file name exists in the
     * file system.
     * @param aPathFileName Path file name.
     * @return true if it exists and false otherwise
     */
    static public boolean exists(String aPathFileName)
    {
        if (StringUtils.isNotEmpty(aPathFileName))
        {
            File osFile = new File(aPathFileName);
            return osFile.exists();
        }
        else
            return false;
    }

    /**
     * Generates a unique path based on the parameters provided.
     *
     * @param aPathName Optional path name - if not provided, then the current working directory will be used.
     * @param aPrefix Path name prefix (appended with random id)
     *
     * @return A unique path name.
     */
    static public String generateUniquePathName(String aPathName, String aPrefix)
    {
        if (StringUtils.isEmpty(aPathName))
            aPathName = getWorkingDirectory();
        if (StringUtils.isEmpty(aPathName))
            aPrefix = "subpath";

// http://www.javapractices.com/topic/TopicAction.do?Id=56

        UUID uniqueId = UUID.randomUUID();
        byte[] idBytes = uniqueId.toString().getBytes();
        Checksum checksumValue = new CRC32();
        checksumValue.update(idBytes, 0, idBytes.length);
        long longValue = checksumValue.getValue();

        return String.format("%s%c%s_%d", aPathName, File.separatorChar, aPrefix, longValue);
    }

    /**
     * Generates a unique path and file name combination based on the parameters
     * provided.
     *
     * @param aPathName Path name.
     * @param aFilePrefix File name prefix (appended with random id)
     * @param aFileExtension File name extension.
     *
     * @return A unique path and file name combination.
     */
    static public String generateUniquePathFileName(String aPathName, String aFilePrefix,
                                                    String aFileExtension)
    {
        String pathFileName;

        if (StringUtils.isNotEmpty(aPathName))
            pathFileName = String.format("%s%c%s", aPathName, File.separatorChar, aFilePrefix);
        else
            pathFileName = aFilePrefix;

// http://www.javapractices.com/topic/TopicAction.do?Id=56

        UUID uniqueId = UUID.randomUUID();
        byte[] idBytes = uniqueId.toString().getBytes();
        Checksum checksumValue = new CRC32();
        checksumValue.update(idBytes, 0, idBytes.length);
        long longValue = checksumValue.getValue();

        if (StringUtils.startsWith(aFileExtension, "."))
            return String.format("%s_%d%s", pathFileName, longValue, aFileExtension);
        else
            return String.format("%s_%d.%s", pathFileName, longValue, aFileExtension);
    }

    /**
     * Convenience method that returns the current working directory/folder.
     * @return Current working directory/folder.
     */
    static public String getWorkingDirectory()
    {
        return System.getProperty("user.dir");
    }

    /**
     * Combines the current working directory/folder with a relative path file
     * name.
     * <p>
     * Invoking <code>getWDPathFileName("resources\\somefile.txt")</code>
     * would produce: <i>"C:\\current\\workdir\\resources\\somefile.txt"</i>
     * </p>
     * @param aRelativeName A relative path file name.
     * @return A concatenated string: WD + RN
     */
    static public String getWDPathFileName(String aRelativeName)
    {
        if (StringUtils.isNotEmpty(aRelativeName))
        {
            int strLength = aRelativeName.length();
            if (aRelativeName.charAt(strLength-1) == File.separatorChar)
                return getWorkingDirectory() + aRelativeName;
            else
                return String.format("%s%c%s", getWorkingDirectory(),
                                     File.separatorChar, aRelativeName);
        }
        else
            return getWorkingDirectory();
    }

    /**
     * Create a set of file names inside the specified path name.
     *
     * @param aPathName Absolute path name
     * @param aIsAbsolute If <i>true</i> then absolute names will be included
     *
     * @return Set of file names within the path name.
     *
     * @throws IOException File system exception
     */
    static public Set<String> getFileListByPathName(String aPathName, boolean aIsAbsolute)
        throws IOException
    {
        String fileName;

        Set<String> fileList = new HashSet<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(aPathName)))
        {
            for (Path path : stream)
            {
                if (! Files.isDirectory(path))
                {
                    if (aIsAbsolute)
                        fileName = String.format("%s%c%s", aPathName, File.separatorChar, path.getFileName().toString());
                    else
                        fileName = path.getFileName().toString();
                    fileList.add(fileName);
                }
            }
        }
        return fileList;
    }

    /**
     * Copies an input file to the output file.  In an effort to promote
     * better performance, I/O channels are used for the operations.
     *
     * @param anInFile Represents the source file.
     * @param anOutFile Represents the destination file.
     * @throws IOException Related to stream building and read/write operations.
     */
    static public void copy(File anInFile, File anOutFile)
        throws IOException

    {
        FileChannel srcChannel, dstChannel;

        srcChannel = new FileInputStream(anInFile).getChannel();
        dstChannel = new FileOutputStream(anOutFile).getChannel();

        srcChannel.transferTo(0, srcChannel.size(), dstChannel);
        srcChannel.close();
        dstChannel.close();
    }

    /**
     * Copies an input file to the output file.  In an effort to promote
     * better performance, I/O channels are used for the operations.
     *
     * @param anInPathFileName Represents the source file.
     * @param anOutPathFileName Represents the destination file.
     * @throws IOException Related to stream building and read/write operations.
     */
    static public void copy(String anInPathFileName,
                            String anOutPathFileName)
        throws IOException
    {
        File inPathFile, outPathFile;

        inPathFile = new File(anInPathFileName);
        outPathFile = new File(anOutPathFileName);

        copy(inPathFile, outPathFile);
    }

    /**
     * Compresses the input file into a ZIP file container.
     *
     * @param anInFile The file to be compressed.
     * @param aZipFile The file representing the ZIP container.
     * @throws IOException Related to opening the file streams and
     * related read/write operations.
     */
    static public void zipFile(File anInFile, File aZipFile)
        throws IOException
    {
        zipFile(anInFile.getAbsolutePath(), aZipFile.getAbsolutePath());
    }

    /**
     * Compresses the input file into a ZIP file container.
     *
     * @param aInFileName The file name to be compressed.
     * @param aZipFileName The file name of the ZIP container.
     * @throws IOException Related to opening the file streams and
     * related read/write operations.
     */
    static public void zipFile(String aInFileName, String aZipFileName)
        throws IOException
    {
        File inFile;
        int byteCount;
        byte[] ioBuf;
        FileInputStream fileIn;
        ZipOutputStream zipOut;
        FileOutputStream fileOut;

        inFile = new File(aInFileName);
        if (inFile.isDirectory())
            return;

        ioBuf = new byte[FILE_IO_BUFFER_SIZE];
        fileIn = new FileInputStream(inFile);
        fileOut = new FileOutputStream(aZipFileName);
        zipOut = new ZipOutputStream(fileOut);
        zipOut.putNextEntry(new ZipEntry(inFile.getName()));
        byteCount = fileIn.read(ioBuf);
        while (byteCount > 0)
        {
            zipOut.write(ioBuf, 0, byteCount);
            byteCount = fileIn.read(ioBuf);
        }
        fileIn.close();
        zipOut.closeEntry();
        zipOut.close();
    }

    /**
     * Compresses the input path file into a ZIP output file stream.  The
     * method will recursively process all files and subfolders within
     * the input path file.
     *
     * @param aPathFile Identifies the folder to compress.
     * @param aZipOutStream A previously opened ZIP output file stream.
     * @throws IOException Related to opening the file streams and
     * related read/write operations.
     */
    static public void zipPath(File aPathFile, ZipOutputStream aZipOutStream)
        throws IOException
    {
        zipPath(aPathFile.getAbsolutePath(), aZipOutStream);
    }

    /**
     * Compresses the input path name into a ZIP output file stream.  The
     * method will recursively process all files and sub folders within
     * the input path file.
     *
     * @param aPathName Identifies the name of the folder to compress.
     * @param aZipOutStream A previously opened ZIP output file stream.
     * @throws IOException Related to opening the file streams and
     * related read/write operations.
     */
    static public void zipPath(String aPathName, ZipOutputStream aZipOutStream)
        throws IOException
    {
        int byteCount;
        byte[] ioBuf;
        String[] fileList;
        File zipFile, inFile;
        FileInputStream fileIn;

        zipFile = new File(aPathName);
        if (! zipFile.isDirectory())
            return;

        fileList = zipFile.list();
        if (fileList == null)
            return;

        for (String fileName : fileList)
        {
            inFile = new File(aPathName, fileName);
            if (inFile.isDirectory())
                zipPath(inFile.getPath(), aZipOutStream);
            else
            {
                ioBuf = new byte[FILE_IO_BUFFER_SIZE];
                fileIn = new FileInputStream(inFile);
                aZipOutStream.putNextEntry(new ZipEntry(inFile.getName()));
                byteCount = fileIn.read(ioBuf);
                while (byteCount > 0)
                {
                    aZipOutStream.write(ioBuf, 0, byteCount);
                    byteCount = fileIn.read(ioBuf);
                }
                fileIn.close();
            }
        }
    }

    /**
     * Compresses the input file into a GZIP file container.
     *
     * @param anInPathFileName The file name to be compressed.
     * @param aGzipPathFileName The file name of the ZIP container.
     *
     * @throws IOException Related to opening the file streams and
     * related read/write operations.
     */
    public static void gzipFile(String anInPathFileName, String aGzipPathFileName)
        throws IOException
    {
        int byteCount;

        FileInputStream fileInputStream = new FileInputStream(anInPathFileName);
        FileOutputStream fileOutputStream = new FileOutputStream(aGzipPathFileName);
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fileOutputStream);

        byte[] ioBuf = new byte[FILE_IO_BUFFER_SIZE];
        byteCount = fileInputStream.read(ioBuf);
        while (byteCount > 0)
        {
            gzipOutputStream.write(ioBuf, 0, byteCount);
            byteCount = fileInputStream.read(ioBuf);
        }

        gzipOutputStream.close();
        fileOutputStream.close();
        fileInputStream.close();
    }

    /**
     * Decompresses the GZIP file back to its original form.
     *
     * @param aGzipPathFileName The GZIP file name.
     * @param anOutputPathFileName The output file name.
     *
     * @throws IOException Related to opening the file streams and
     * related read/write operations.
     */
    public static void ungzipFile(String aGzipPathFileName, String anOutputPathFileName)
        throws IOException
    {
        int byteCount;

        FileInputStream fileInputStream = new FileInputStream(aGzipPathFileName);
        GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
        FileOutputStream fileOutputStream = new FileOutputStream(anOutputPathFileName);

        byte[] ioBuf = new byte[FILE_IO_BUFFER_SIZE];
        byteCount = gzipInputStream.read(ioBuf);
        while (byteCount > 0)
        {
            fileOutputStream.write(ioBuf, 0, byteCount);
            byteCount = gzipInputStream.read(ioBuf);
        }

        fileOutputStream.close();
        gzipInputStream.close();
    }
}
