var config = require('nconf'),
    yargs = require('yargs'),
    colors = require('colors/safe'),
    azure = require('azure-storage'),
    util = require('util'),
    path = require('path'),
    fs = require('fs'),
    streamBuffers = require('stream-buffers');

var isVerboseOn = false;

process.on('uncaughtException', function(err) {
    
    if(isVerboseOn){
        console.log(colors.red(err.stack));      
    }
    else{
        console.log(colors.red(err));  
    }
    
});

var configFilePath = path.join(__dirname, 'config', 'config.json');
if(fs.existsSync(path.join(process.cwd(), 'config.json'))){
    configFilePath = path.join(process.cwd(), 'config.json');
}

if(isVerboseOn){
    console.log(colors.grey('Config file path: ' + configFilePath));     
}
   
config.argv()
    .env()
    .file({ file: configFilePath });

var argv = yargs.usage('Usage: $0 -f [file] -n [lines] -c [connectionString] -a [accountName] -k [key]')
     .demand(['f'])
     .alias('f', 'file')
     .describe('f', 'Path to Azure blob, e.g. <container name>/path/to/blob')
     .alias('n', 'lines')
     .describe('n', 'Output the first N lines of the file')
     .default('n', 10)
     .alias('c', 'connectionString')
     .describe('c', 'Connection string to the Azure storage account, can be retrieved from the Azure portal')
     .alias('a', 'accountName')
     .describe('a', 'Azure storage account name, required if \'connectionString\' is not provided')
     .alias('k', 'key')
     .describe('k', 'Key for the Azure storage account')
     .alias('v', 'verbose')
     .help('h')
     .alias('h', 'help')
     .epilog('Copyright 2015')
     .argv;

if(argv.verbose){
    isVerboseOn = true;
}

var linesToRead = parseInt(10);
if(argv.lines){
    linesToRead = parseInt(argv.lines)
}

if(argv.connectionString){
    process.env['AZURE_STORAGE_CONNECTION_STRING'] = argv.connectionString;  
}
if(argv.accountName && argv.key){
    process.env['AZURE_STORAGE_ACCOUNT'] = argv.accoutName;
    process.env['AZURE_STORAGE_ACCESS_KEY'] = argv.key;
}
if(argv.accountName && argv.key == null){
    throw new Error('Must provide -k or --key option to provide key value for storage account');
}

if(argv.connectionString == null && argv.accountName == null){   
    throw new Error('Must provide either --connectionString or --accountName');
}

var blobService = azure.createBlobService();
    
if(argv.file){
        
    var regex = /(?:[^\/\\]+|\\.)+/g,
        containerName = null,
        blobName = null,
        fullPath = __dirname;
        
    var splitChar = '/';
    if(argv.file.indexOf('/') >= 0){
        splitChar = '/';
    }
    else{
        splitChar = '\\';
    }
    var parts = argv.file.split(splitChar);
    
    //console.log(argv.file);
    //console.log(util.inspect(parts));
    
    containerName = parts[0];
    blobName =  parts.slice(1, parts.length).join('/');   
   
    //console.info('Container name: ' + containerName);
   // console.info('Blob name: ' + blobName);

    var blobProperties = blobService.getBlobProperties(containerName, blobName, null, function (error, blob) {
        if (error) {
            throw error;
        }
        else {
            blobSize = blob.contentLength;
            fullPath = fullPath + '\\temp.txt';
            
            if(isVerboseOn){
                console.log(colors.gray('Blob size: ' + blobSize));   
            }

            getChunkAndPrint(fullPath, blobSize, containerName, blobName);
        }
    });  

}

function getChunkAndPrint(fullPath, blobSize, containerName, blobName, readLines, position, length) {
    
    var myWritableStreamBuffer = new streamBuffers.WritableStreamBuffer({
        initialSize: (100 * 1024),   // start at 100 kilobytes.
        incrementAmount: (10 * 1024) // grow by 10 kilobytes each time buffer overflows.
    });

    //var stream = fs.createWriteStream(fullPath, {flags: 'a'});
    
    var startPos = position ? position : 0;
    var chunkSize = length ? length : 2048;
    
    var endPos = startPos + chunkSize;
    if (endPos > blobSize) {
        endPos = blobSize;
    }
    
    readLines = readLines ? readLines : 0;
    
    var currPosition = 0,
        lines = [];
    
    //console.log(colors.gray('Downloading ' + (endPos - startPos) + ' bytes starting from ' + startPos + ' marker.'));
    
    blobService.getBlobToStream(containerName, blobName, myWritableStreamBuffer, 
        { 'rangeStart': startPos, 'rangeEnd': endPos - 1 }, function(error) {
        if (error) {
            throw error;
        }
        else if (!error) {
            
            currPosition = endPos - 1;            
            
            var contents = myWritableStreamBuffer.getContentsAsString('utf-8');
            
            //console.log(contents);            
            //console.log(util.inspect(contents.split(/\r\n|\r|\n/g)));
            
            lines = lines.concat(contents.split(/\r\n|\r|\n/g));           
                                   
            // Assume binary file and just spit out everything and exit
            if(lines.length == 1){
               console.log(contents); 
            }
            else if(readLines + lines.length < linesToRead && endPos != blobSize){
                
                for(var i = 0; i < lines.length; i++){
                    //console.log('[ ' + (readLines + i) + '] ' + lines[i]);
                    console.log(lines[i]);
                }
                
                readLines += lines.length;
                //console.log(colors.gray('Lines length: ' + lines.length + '; readLines: ' + readLines + '; lines to read: ' + linesToRead));
            
                // console.error('Need to download another chunk of the file...');
                getChunkAndPrint(fullPath, blobSize, containerName, blobName, readLines, currPosition, chunkSize * 2);    
            }
            else if(readLines + lines.length < linesToRead && endPos == blobSize){
                // Nothing else to do...
                
                for(var i = 0; i < lines.length; i++){
                    //console.log('[ ' + (readLines + i) + '] ' + lines[i]);
                    console.log(lines[i]);
                }
                
                console.log(colors.gray('Unable to read ' + argv.lines + ' lines, end of file'));
            }
            else if(readLines + lines.length >= linesToRead){
                for(var i = 0; i < linesToRead - readLines; i++){
                    //console.log('[ ' + (readLines + i) + '] ' + lines[i]);
                    console.log(lines[i]);
                }
            }
            
        }
    });
}