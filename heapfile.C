#include "heapfile.h"
#include "error.h"
#include <cstring>
#include <iostream>

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File*       file;
    Status      status;
    FileHdrPage*    hdrPage;
    int         hdrPageNo;
    int         newPageNo;
    Page*       newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
        // file doesn't exist. First create it and allocate
        // an empty header page and data page.

        // create and re-open the DB file
        status = db.createFile(fileName);
        if (status != OK) return status;

        status = db.openFile(fileName, file);
        if (status != OK) return status;

        // allocate header page
        Page* rawHdr = nullptr;
        status = bufMgr->allocPage(file, hdrPageNo, rawHdr);
        if (status != OK) { db.closeFile(file); return status; }

        // fill in header fields
        hdrPage = (FileHdrPage*) rawHdr;
        // zero the struct so no stale bits linger
        memset(hdrPage, 0, sizeof(FileHdrPage));

        // copy file name safely into header (truncate if needed)
        {
            size_t n = fileName.size();
            if (n >= sizeof(hdrPage->fileName)) n = sizeof(hdrPage->fileName) - 1;
            memcpy(hdrPage->fileName, fileName.data(), n);
            hdrPage->fileName[n] = '\0';
        }
        hdrPage->firstPage = -1;
        hdrPage->lastPage  = -1;
        hdrPage->recCnt    = 0;

        // allocate the first data page and link it
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK) {
            bufMgr->unPinPage(file, hdrPageNo, false);
            db.closeFile(file);
            return status;
        }
        // singly-linked list of data pages; no prev field in this API
        newPage->setNextPage(-1);

        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage  = newPageNo;

        // write both pages back
        Status s1 = bufMgr->unPinPage(file, newPageNo, true);
        Status s2 = bufMgr->unPinPage(file, hdrPageNo, true);
        if (s1 != OK) { db.closeFile(file); return s1; }
        if (s2 != OK) { db.closeFile(file); return s2; }

        // close the freshly created file
        return db.closeFile(file);
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
    return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status  status;
    Page*   pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        // most projects pin header page #1
        headerPageNo = 1;

        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
        if (status != OK) { returnStatus = status; return; }

        headerPage = (FileHdrPage*) pagePtr;
        hdrDirtyFlag = false;

        // if there is at least one data page, pin the first
        if (headerPage->firstPage != -1) {
            curPageNo = headerPage->firstPage;
            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK) {
                // drop header before bailing
                bufMgr->unPinPage(filePtr, headerPageNo, false);
                returnStatus = status;
                return;
            }
            curDirtyFlag = false;
            // position "before-first" record on this page
            curRec.pageNo = curPageNo;
            curRec.slotNo = -1;
        } else {
            // empty file—no data page pinned
            curPage = nullptr;
            curPageNo = 0;
            curDirtyFlag = false;
            curRec.pageNo = -1;
            curRec.slotNo = -1;
        }

        returnStatus = OK;
    }
    else
    {
        cerr << "open of heap file failed\n";
        returnStatus = status;
        return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        if (status != OK) cerr << "error in unpin of date page\n";
    }
    
     // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
    
    // status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
    // if (status != OK) cerr << "error in flushFile call\n";
    // before close the file
    status = db.closeFile(filePtr);
    if (status != OK)
    {
        cerr << "error in closefile call\n";
        Error e;
        e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // if target record isn't on the current page, move to its page
    if (curPage == nullptr || rid.pageNo != curPageNo) {
        if (curPage != nullptr) {
            Status u = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (u != OK) return u;
            curPage = nullptr;
            curDirtyFlag = false;
        }
        curPageNo = rid.pageNo;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;
        curDirtyFlag = false;
    }

    // remember where we are
    curRec = rid;

    // fetch the record bytes
    return curPage->getRecord(rid, rec);
}


HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;

    // loop until we find the matching record or we reach end of all files
    while (true) {
        // first time we call scan, so we need get the first page
        if (curPage == NULL) {
            // read in first page from file
            status = bufMgr->readPage(filePtr, headerPage->firstPage, curPage);
            if (status != OK) {
                return status;
            }
            
            // get page numebr
            curPageNo = headerPage->firstPage;
            curDirtyFlag = false; // page hasnt been changed 
            
            // get the first record of this page
            status = curPage->firstRecord(curRec);
        }
        else {
            // get next record
            status = curPage->nextRecord(curRec, nextRid);
        }

        // no records in this page, move to next
        if (status == OK) {
            // move to next record
            if (nextRid.pageNo != curRec.pageNo || nextRid.slotNo != curRec.slotNo) {
                curRec = nextRid;
            }
        }
        else if (status == NORECORDS) {
            // reading pinned the page, so we need to unpin it
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) {
                return status;
            }

            // get and read netx page
            status = curPage->getNextPage(nextPageNo); 
            if (status != OK || nextPageNo == -1) {
                return FILEEOF;
            }
            
            status = bufMgr->readPage(filePtr, nextPageNo, curPage);
            if (status != OK) {
                return status;
            }
            
            curPageNo = nextPageNo;
            curDirtyFlag = false;
            
            status = curPage->firstRecord(curRec);
            if (status != OK) {
                return FILEEOF;
            }
        }
        else {
            return status;
        }
        
        // get the current record
        status = curPage->getRecord(curRec, rec);
        if (status != OK) {
            return status;
        }
        
        // see if it matches the specified filter
        if (matchRec(rec)) {
            outRid = curRec;
            return OK;
        }
    }
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

  
  
  
  
  
  
  
  
  
  
  
  
}


