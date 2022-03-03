#include <vector>
#include <queue>
#include <algorithm>

#include "BigQ.h"


/* Struct required for priority queue which encloses 
the current record with it current page and the run Number */
struct PriorityQueue_Record {
	Record *currRecord;
	int pageNum;
	int bufferNum;
};


// Comparator class for priority queue to maintain the min heap property.
class PriorityQueue_Comparator {
	private:
		OrderMaker *sortOrder;
		ComparisonEngine cmp;

	public:
		PriorityQueue_Comparator(OrderMaker *sortOrder) {
			this -> sortOrder = sortOrder;
		}

		bool operator()(PriorityQueue_Record *r1, PriorityQueue_Record *r2) {
			if (cmp.Compare(r1 -> currRecord, r2 -> currRecord, sortOrder) > 0)
				return true;

			return false;
		}
};


// Class for record comparator.
class RecordComparator {
	private:
		OrderMaker *sortOrder;
		ComparisonEngine cmp;

	public:
		RecordComparator(OrderMaker *sortOrder) {
			this -> sortOrder = sortOrder;
		}

		bool operator()(Record *r1, Record *r2) {
			if (cmp.Compare(r1, r2, sortOrder) < 0)
				return true;

			return false;
		}
};


// Constructor for BigQ class.
BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	this -> inputPipe = &in;
	this -> outputPipe = &out;
	this -> sortedOrder = &sortorder;
	this -> runLength = &runlen;

	// Creating new File to store runs.
	this -> runsFile = new File();
	this -> runsFile -> Open(0, "runs.bin");
	this -> runsFile -> Close();
	this -> runsFile -> Open(1, "runs.bin");

	pthread_t workerthread;
	pthread_create(&workerthread, NULL, BigQ::invoke_tpmmsAlgo, (void *)this);
	pthread_join(workerthread, NULL);

	// Shut down the out pipe.
	out.ShutDown();
}


// Start function from the new worker thread.
void *BigQ::invoke_tpmmsAlgo(void *args) {
	// Type casting the void arguments and calling the worker function.
	((BigQ *)args) -> worker();

	return nullptr;
}


// Worker function that does TPMMS sorting.
void BigQ::worker() {

	int runlen = *runLength;

	// Count number of pages in each run.
	int pagesInRun = 0;

	// Pages is used to full records in a run.
	Page *buffer = new Page();

	// Record memory to get a record from input pipe.
	Record *currRecord = new Record();

	// Initial pointer to first run is zero.
	runPointers.push_back(0);
	
	// Vector created to store records of a run.
	vector<Record *> curr_run_vector;
	
	// Count num of runs.
	int num_of_runs = 0;

	// Temporary record used to push records to current run vector.
	Record *tempRec = new Record();

	// Remove records from input pipe and create the sorted runs.
	while (inputPipe -> Remove(currRecord)) {

		// Pages in run is less than run length.
		if (pagesInRun < runlen) {

			int result = buffer -> Append(currRecord);
			if (result == 0) {
				pagesInRun++;

				while (buffer -> GetFirst(tempRec)) {
					curr_run_vector.push_back(tempRec);
					tempRec = new Record();
				}

				buffer -> EmptyItOut();
				buffer -> Append(currRecord);
			}
		} else {
			// Pages in run is equal to run length.
			pagesInRun = 0;

			// Sort a current run.
			this -> sortRun(curr_run_vector);
			num_of_runs++;
			
			// Write the sorted run to the file.
			int runhead = this -> addRuntoFile(curr_run_vector);

			// Adding the head of the current run to list.
			this -> runPointers.push_back(runhead);
			curr_run_vector.clear();
			buffer -> Append(currRecord);
		}
	}

	// Sorting and adding the last run to file.
	num_of_runs++;
	while (buffer -> GetFirst(tempRec)) {
		curr_run_vector.push_back(tempRec);
		tempRec = new Record();
	}

	this -> sortRun(curr_run_vector);
	int runhead = this -> addRuntoFile(curr_run_vector);
	this -> runPointers.push_back(runhead);
	curr_run_vector.clear();
	this -> runsFile -> Close();

	/******** Merge Runs Operation  *********/

	// Runs file to read the sorted runs.
	this -> runsFile = new File();
	runsFile -> Open(1, "runs.bin");
	typedef priority_queue<PriorityQueue_Record *, std::vector<PriorityQueue_Record *>, PriorityQueue_Comparator> pq_merger_type;
	pq_merger_type pq_merger(sortedOrder);
	
	// Array of buffers get pages from runs.
	Page *runBuffers[num_of_runs];

	// Priority queue used to merge the records from the runs.
	PriorityQueue_Record *pq_record = new PriorityQueue_Record();
	currRecord = new Record();

	int index = 0;
	// Pushing the first record of every run to priority queue.
	while (index < num_of_runs) {
		runBuffers[index] = new Page();
		runsFile -> GetPage(runBuffers[index], this -> runPointers[index]);
		runBuffers[index] -> GetFirst(currRecord);
		pq_record -> currRecord = currRecord;
		pq_record -> pageNum = this -> runPointers[index];
		pq_record -> bufferNum = index;
		pq_merger.push(pq_record);
		pq_record = new PriorityQueue_Record();
		currRecord = new Record();
		index++;
	}

	// Retrieving minimum record from priority queue and pushing to output pipe.
	while (!pq_merger.empty()) {
		pq_record = pq_merger.top();
		int pageNum = pq_record -> pageNum;
		int bufferNum = pq_record -> bufferNum;
		this -> outputPipe -> Insert(pq_record -> currRecord);
		pq_merger.pop();

		Record *newRecord = new Record();
		if (runBuffers[bufferNum] -> GetFirst(newRecord) == 0) {

			pageNum = pageNum + 1;
			// Moving to next page in the current run of the min record in priority queue.
			if (pageNum < (runsFile -> GetLength() - 1) && (pageNum < this -> runPointers[bufferNum + 1])) {
				runBuffers[bufferNum] -> EmptyItOut();
				runsFile -> GetPage(runBuffers[bufferNum], pageNum);

				if (runBuffers[bufferNum] -> GetFirst(newRecord) != 0) {
					pq_record -> currRecord = newRecord;
					pq_record -> bufferNum = bufferNum;
					pq_record -> pageNum = pageNum;
					pq_merger.push(pq_record);
				}
			}
		} else {
			pq_record -> currRecord = newRecord;
			pq_record -> bufferNum = bufferNum;
			pq_record -> pageNum = pageNum;
			pq_merger.push(pq_record);
		}
	}

	runsFile -> Close();
}

// Writes the sorted vector to file and the returns the current size of file.
int BigQ::addRuntoFile(vector<Record *> &vector) {
	Page *buffer = new Page();

	for (int index = 0; index < vector.size(); index++) {
		if (buffer -> Append(vector[index]) == 0) {
			if (this -> runsFile -> GetLength() == 0) {
				this -> runsFile -> AddPage(buffer, 0);
			} else {
				this -> runsFile -> AddPage(buffer, this -> runsFile -> GetLength() - 1);
			}
			buffer -> EmptyItOut();
			buffer -> Append(vector[index]);
		}
	}

	if (this -> runsFile -> GetLength() == 0) {
		this -> runsFile -> AddPage(buffer, 0);
	} else {
		this -> runsFile -> AddPage(buffer, this -> runsFile -> GetLength() - 1);
	}

	// Return the size of file which is runHead for the next run. 
	return this -> runsFile -> GetLength() - 1; 
}


// Sort the vector based on the input sorted order using comparator.
void BigQ::sortRun(vector<Record *> &vector) {
	sort(vector.begin(), vector.end(), RecordComparator(sortedOrder));
}


BigQ::~BigQ()
{
	this -> outputPipe -> ShutDown();
}