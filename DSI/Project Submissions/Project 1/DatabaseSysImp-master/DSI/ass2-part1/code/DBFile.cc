#include <stdio.h>

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "string.h"
#include "fstream"


// Constructor.
DBFile::DBFile () {
    this -> fileBuffer = new File();
    this -> writeBuffer = new Page(); 
    this -> readBuffer = new Page();
    this -> isEmptyWriteBuffer = true;
    this -> endoffile = false;
    this -> pageIdx = 0;
}


// Create normal file. 
int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    char metadata[100];
    strcpy(metadata , f_path);
    strcat(metadata , "-metadata.header");

    // Initially keeping the flag as true for write buffer.
    this -> isEmptyWriteBuffer = true;
    ofstream fileOutputStream;
    fileOutputStream.open(metadata);

    // f_type is added to metadata file.
    if (f_type == heap) {
        fileOutputStream << "Heap file created.\n";
    }
    // Open operation on the file is called.
    fileBuffer -> Open(0, (char *)f_path);  
    fileOutputStream.close();

    return 1;
}


// Load the DBFile instance .tbl files.
void DBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE  *fileInputStream = fopen(loadpath , "r");
    Record currRecord;
    // Records are read using SuckNextRecord function from Record.h.
    while(currRecord.SuckNextRecord( &f_schema , fileInputStream ) == 1){
        // Add function is called to add the record to DBFile instance
        this -> Add(currRecord);
    }
    fclose(fileInputStream);
}


// Assumes Heap file exists and DBFile instance points to the heap file.
int DBFile::Open (const char *f_path) {
    //Open function from File class on the existing bin file created
    this -> fileBuffer -> Open(1, (char *)f_path);
    //initially keep the end of file as false and page Index to zero
    this -> endoffile = false;
    this -> pageIdx = 0;

    return 1;
}


// Function points to the first record in the file.
void DBFile::MoveFirst () {
    // On existing file, we are extracting the first page into read buffer.
    this -> fileBuffer -> GetPage(this -> readBuffer, 0);
}


// Closes the file.
int DBFile::Close () {
    // Checking if the write buffer is empty to write the last page to file.
    if(this -> isEmptyWriteBuffer == false) {
        off_t no_of_pages = fileBuffer -> GetLength();
        if(no_of_pages != 0) {
            no_of_pages = no_of_pages - 1;
        }
        this -> fileBuffer -> AddPage(writeBuffer, no_of_pages);
        writeBuffer -> EmptyItOut();
    }
    this -> fileBuffer -> Close();
    
    // Delete all the object instances.
    delete fileBuffer;
    delete writeBuffer;
    delete readBuffer;
    isEmptyWriteBuffer = true;

    return 1;
}

// Add new record to end of file.
void DBFile::Add (Record &rec) {
    Record write;
    write.Consume(&rec);
    off_t no_of_pages = (*fileBuffer).GetLength();

    this -> isEmptyWriteBuffer = false;

    // Append operation failed.
    if(writeBuffer -> Append(&write) == 0) {
        if(no_of_pages == 0) {
            // First page adding to file.
            fileBuffer -> AddPage(writeBuffer, 0);
        }
        else {
            // Adding page to file.
            fileBuffer -> AddPage(writeBuffer , no_of_pages - 1); 
        }

        // Empty the write buffer.
        writeBuffer -> EmptyItOut();
        // Append the record to the write buffer.
        writeBuffer -> Append(&write);
    }
}


// Gets the next record present in heap file and returns.
int DBFile::GetNext (Record &fetchme) {
    // Checking end of file. 
    if(this -> endoffile == false) {
        // Extracting the next record from readbuffer.
        int result = this -> readBuffer -> GetFirst(&fetchme);
        // Checking if the readbuffer has any records.
        if(result == 0) {
            // Incrementing the page index.
            pageIdx++;
            // Checking if the end of file has reached.
            if(pageIdx >= this -> fileBuffer -> GetLength() - 1) {
                // Updating end of file as true.
                this -> endoffile = true;

                return 0; // End of file has been reached.
            } else {
                // Extracting next page from file into read buffer.
                this -> fileBuffer -> GetPage(this -> readBuffer,pageIdx);
                this -> readBuffer -> GetFirst(&fetchme);
            }
        }

        return 1; // Next record is fed into fetchme.
    }

    return 0; // End of file has been reached.
}


// Gets the next record from the Heap file if the accepted by the predicate.
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine cmpengine;

    while(GetNext(fetchme)) {
        if(cmpengine.Compare(&fetchme,&literal,&cnf))
            return 1;   // Record accepted by the predicate.
    }

    return 0; // End of file reached and no more accepted by predicate.
}


// Return the size of the current DBFile.
int DBFile::getSize() {
    off_t noof_pages = this -> fileBuffer -> GetLength();

    if(this -> isEmptyWriteBuffer == false) {
        if(noof_pages == 0) {
            this -> fileBuffer -> AddPage(this -> writeBuffer, noof_pages - 2);
        } else {
            this -> fileBuffer -> AddPage(this -> writeBuffer, noof_pages - 1);
        }
        this -> isEmptyWriteBuffer = true;
        this -> writeBuffer -> EmptyItOut();
    }

    return this -> fileBuffer -> GetLength() - 1;
}