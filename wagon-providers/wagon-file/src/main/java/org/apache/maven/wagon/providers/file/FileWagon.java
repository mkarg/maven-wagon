package org.apache.maven.wagon.providers.file;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.maven.wagon.ConnectionException;
import org.apache.maven.wagon.InputData;
import org.apache.maven.wagon.LazyFileOutputStream;
import org.apache.maven.wagon.OutputData;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.StreamWagon;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.resource.Resource;
import org.codehaus.plexus.util.FileUtils;

/**
 * Wagon Provider for Local File System
 *
 * @author <a href="michal.maczka@dimatics.com">Michal Maczka</a>
 *
 * @plexus.component role="org.apache.maven.wagon.Wagon" role-hint="file" instantiation-strategy="per-lookup"
 */
public class FileWagon
    extends StreamWagon
{
    public void fillInputData( InputData inputData )
        throws TransferFailedException, ResourceDoesNotExistException
    {
        if ( getRepository().getBasedir() == null )
        {
            throw new TransferFailedException( "Unable to operate with a null basedir." );
        }

        Resource resource = inputData.getResource();

        Path file = Paths.get( getRepository().getBasedir(), resource.getName() );

        if ( Files.notExists( file ) )
        {
            throw new ResourceDoesNotExistException( "File: " + file + " does not exist" );
        }

        try
        {
            InputStream in = new BufferedInputStream( Files.newInputStream( file ) );

            inputData.setInputStream( in );

            resource.setContentLength( Files.size( file ) );

            resource.setLastModified( Files.getLastModifiedTime( file ).toMillis() );
        }
        catch ( IOException e )
        {
            throw new TransferFailedException( "Could not read from file: " + file.toAbsolutePath(), e );
        }
    }

    public void fillOutputData( OutputData outputData )
        throws TransferFailedException
    {
        if ( getRepository().getBasedir() == null )
        {
            throw new TransferFailedException( "Unable to operate with a null basedir." );
        }

        Resource resource = outputData.getResource();

        File file = new File( getRepository().getBasedir(), resource.getName() );

        createParentDirectories( file );

        OutputStream outputStream = new BufferedOutputStream( new LazyFileOutputStream( file ) );

        outputData.setOutputStream( outputStream );
    }

    protected void openConnectionInternal()
        throws ConnectionException
    {
        if ( getRepository() == null )
        {
            throw new ConnectionException( "Unable to operate with a null repository." );
        }

        if ( getRepository().getBasedir() == null )
        {
            // This condition is possible when using wagon-file under integration testing conditions.
            fireSessionDebug( "Using a null basedir." );
            return;
        }

        // Check the File repository exists
        Path basedir = Paths.get( getRepository().getBasedir() );
        if ( Files.notExists( basedir ) )
        {
            try
            {
                Files.createDirectories( basedir );
            }
            catch ( IOException e )
            {
                throw new ConnectionException( "Repository path " + basedir + " does not exist,"
                                               + " and cannot be created." );
            }
        }

        if ( ! Files.isReadable( basedir ) )
        {
            throw new ConnectionException( "Repository path " + basedir + " cannot be read" );
        }
    }

    public void closeConnection()
    {
    }

    public boolean supportsDirectoryCopy()
    {
        // TODO: should we test for null basedir here?
        return true;
    }

    public void putDirectory( File sourceDirectory, String destinationDirectory )
        throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException
    {
        if ( getRepository().getBasedir() == null )
        {
            throw new TransferFailedException( "Unable to putDirectory() with a null basedir." );
        }

        Path path = resolveDestinationPath( destinationDirectory );

        try
        {
            /*
             * Done to address issue found in HP-UX with regards to "." directory references. Details found in ..
             * WAGON-30 - wagon-file failed when used by maven-site-plugin WAGON-33 - FileWagon#putDirectory() fails in
             * HP-UX if destinationDirectory is "."
             * http://www.nabble.com/With-maven-2.0.2-site%3Adeploy-doesn%27t-work-t934716.html for details. Using
             * path.getCanonicalFile() ensures that the path is fully resolved before an attempt to create it. TODO:
             * consider moving this to FileUtils.mkdirs()
             */
            Path realFile = path.normalize();
            Files.createDirectories( realFile );
        }
        catch ( IOException e )
        {
            // Fall back to standard way if normalize() fails.
            try 
            {
                Files.createDirectories( path );
            } 
            catch ( IOException e1 ) 
            {
                // ignored
            }
        }

        if ( Files.notExists( path ) || ! Files.isDirectory( path ) )
        {
            String emsg = "Could not make directory '" + path.toAbsolutePath() + "'.";

            // Add assistive message in case of failure.
            Path basedir = Paths.get( getRepository().getBasedir() );
            if ( ! Files.isWritable( basedir ) )
            {
                emsg += "  The base directory " + basedir + " is read-only.";
            }

            throw new TransferFailedException( emsg );
        }

        try
        {
            FileUtils.copyDirectoryStructure( sourceDirectory, path.toFile() );
        }
        catch ( IOException e )
        {
            throw new TransferFailedException( "Error copying directory structure", e );
        }
    }

    private Path resolveDestinationPath( String destinationPath )
    {
        String basedir = getRepository().getBasedir();

        // TODO presumably not needed anymore with Java 7's NIO file API
        destinationPath = destinationPath.replace( "\\", "/" );

        Path path = Paths.get( basedir, destinationPath ).normalize();

        return path;
    }

    public List<String> getFileList( String destinationDirectory )
        throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException
    {
        if ( getRepository().getBasedir() == null )
        {
            throw new TransferFailedException( "Unable to getFileList() with a null basedir." );
        }

        Path path = resolveDestinationPath( destinationDirectory );

        if ( Files.notExists( path ) )
        {
            throw new ResourceDoesNotExistException( "Directory does not exist: " + destinationDirectory );
        }

        if ( ! Files.isDirectory( path ) )
        {
            throw new ResourceDoesNotExistException( "Path is not a directory: " + destinationDirectory );
        }

        List<String> list = new ArrayList<>();
        try ( DirectoryStream<Path> directoryStream = Files.newDirectoryStream( path ) ) 
        {
            for ( Path file : directoryStream ) 
            {
                String name = file.getFileName().toString();
                if ( Files.isDirectory( file ) && !name.endsWith( "/" ) )
                {
                    name += "/";
                }
                list.add( name );
            }
        } catch ( IOException e )
        {
            // nothing to do as this ONLY happens in case directoryStream.close() fails, so the list is correct
        }
        return list;
    }

    public boolean resourceExists( String resourceName )
        throws TransferFailedException, AuthorizationException
    {
        if ( getRepository().getBasedir() == null )
        {
            throw new TransferFailedException( "Unable to getFileList() with a null basedir." );
        }

        Path file = resolveDestinationPath( resourceName );

        if ( resourceName.endsWith( "/" ) )
        {
            return Files.isDirectory( file );
        }

        return Files.exists( file );
    }
}
