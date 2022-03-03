#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>

#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;


// Class for BigQ.
class BigQ {

	private:
		Pipe *inputPipe;	// Input pipe to get records. 
		Pipe *outputPipe;	// Output pipe to push records.
		OrderMaker *sortedOrder;	// Sorted order required for sorting.
		int *runLength;	// Run length. 
		File *runsFile;	// File pointer for the runs file. 
		vector<int> runPointers;	// List of pointers to all the runs.

	public:
		// Contructor to intialize the fields of BigQ.
		BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);

		// Worker function which does the TPMMS sorting.
		void worker();
		
		// Start of the TPMMS Algorithm from the new thread.
		static void* invoke_tpmmsAlgo( void *args );
		
		// Sort the current run.
		void sortRun( vector <Record*> &);
		
		// Write the current sorted run to the file.
		int addRuntoFile(vector <Record*> & );

		~BigQ ();
};

#endif