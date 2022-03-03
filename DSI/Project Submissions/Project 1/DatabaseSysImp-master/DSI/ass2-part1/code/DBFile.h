#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;

// Class for dbfile.
class DBFile {

	private:
		File *fileBuffer;	// File instance. 
		Page *writeBuffer;	// Page instance, write buffer. 
		Page *readBuffer;	// Page instance, read buffer.
		bool isEmptyWriteBuffer;	// To check if write buffer is empty.
		bool endoffile;	// To check end of file has reached.
		int pageIdx;	// To track number of pages on file.

	public:
		DBFile (); 
		// Create normal file.
		int Create (const char *fpath, fType file_type, void *startup);
		
		// Assumes heap file exists and DBFile instance points to the Heap file.
		int Open (const char *fpath);

		// Closes the file.
		int Close ();

		// Load the DBFile instance .tbl files.
		void Load (Schema &myschema, const char *loadpath);
		
		// Method points to the first record in the file.
		void MoveFirst ();
		
		// Add new record to end of file.
		void Add (Record &addme);
		
		// Gets the next record present in heap file and returns.
		int GetNext (Record &fetchme);
		
		// Gets the next record from the Heap file if the accepted by the predicate.
		int GetNext (Record &fetchme, CNF &cnf, Record &literal);
		
		// Return size of the current DBFile.
		int getSize();
};

#endif
