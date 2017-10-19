#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <cmath>

#include "../rbf/pfm.h"

using namespace std;

// size of page space storing page and record info
const unsigned NUM_OF_FIELDS_SZ = 2;     // size of space storing the number of fields in a record
const unsigned FIELD_OFFSET_SZ = 2;      // size of space storing the offset of a field in a record
const unsigned FREE_SPACE_SZ = 2;        // size of space storing the number of free bytes in a page
const unsigned NUM_OF_SLOTS_SZ = 2;      // size of space storing the number of slots in a page
const unsigned SLOT_OFFSET_SZ = 2;       // size of space storing the offset of a record in a page
const unsigned SLOT_LENGTH_SZ = 2;       // size of space storing the length of a record in a page
const unsigned PAGE_NUM_SZ = sizeof(PageNum);
const unsigned SLOT_NUM_SZ = NUM_OF_SLOTS_SZ;

const unsigned MAX_NUM_OF_ENTRIES = (PAGE_SIZE - PAGE_NUM_SZ) / (PAGE_NUM_SZ + FREE_SPACE_SZ);  // max number of entries in a directory page

// Record ID
typedef struct
{
    unsigned pageNum;    // page number
    unsigned slotNum;    // slot number in the page
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute
{
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum
{
    EQ_OP = 0,  // =
    LT_OP,      // <
    LE_OP,      // <=
    GT_OP,      // >
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP       // no condition
} CompOp;


/********************************************************************************
The scan iterator is NOT required to be implemented for the part 1 of the project 
********************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RecordBasedFileManager;

class RBFM_ScanIterator
{
    friend class RecordBasedFileManager;
public:
    RBFM_ScanIterator();
    ~RBFM_ScanIterator();

    // Never keep the results in the memory. When getNextRecord() is called,
    // a satisfying record needs to be fetched from the file.
    // "data" follows the same format as RecordBasedFileManager::insertRecord().
    RC getNextRecord(RID &rid, void *data);

    RC close();

private:
    RecordBasedFileManager *rbfm;

    vector<Attribute> recordDescriptor;
    vector<unsigned> attrNums;
    unsigned conditionAttrNum;
    CompOp compOp;
    const void *value;

    FileHandle *fileHandle = nullptr;
    byte page[PAGE_SIZE];
    bool containData;
    PageNum numOfPages;
    PageNum pageNum;
    unsigned numOfSlots;
    unsigned slotNum;

    bool isHeaderPage(PageNum pageNum)
    {
        return pageNum % (MAX_NUM_OF_ENTRIES + 1) == 0;
    }

    bool compare(AttrType type, const void *op1, const void *op2, CompOp compOp);
};


class RecordBasedFileManager
{
    friend class RBFM_ScanIterator;
public:
    static RecordBasedFileManager* instance();

    RC createFile(const string &fileName);
  
    RC destroyFile(const string &fileName);
  
    RC openFile(const string &fileName, FileHandle &fileHandle);
  
    RC closeFile(FileHandle &fileHandle);

    //  Format of the data passed into the function is the following:
    //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
    //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
    //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
    //     Each bit represents whether each field value is null or not.
    //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
    //     If k-th bit from the left is set to 0, k-th field contains non-null values.
    //     If there are more than 8 fields, then you need to find the corresponding byte first,
    //     then find a corresponding bit inside that byte.
    //  2) Actual data is a concatenation of values of the attributes.
    //  3) For Int and Real: use 4 bytes to store the value;
    //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
    //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
    // For example, refer to the Q8 of Project 1 wiki page.
    RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

    RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  
    // This method will be mainly used for debugging/testing.
    // The format is as follows:
    // field1-name: field1-value  field2-name: field2-value ... \n
    // (e.g., age: 24  height: 6.1  salary: 9000
    //        age: NULL  height: 7.5  salary: 7500)
    RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

/******************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
******************************************************************************************************************************************************************/
    RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

    // Assume the RID does not change after an update
    RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

    RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    RC scan(FileHandle &fileHandle,
            const vector<Attribute> &recordDescriptor,
            const string &conditionAttribute,
            const CompOp compOp,                  // comparision type such as "<" and "="
            const void *value,                    // used in the comparison
            const vector<string> &attributeNames, // a list of projected attributes
            RBFM_ScanIterator &rbfm_ScanIterator);

protected:
    RecordBasedFileManager();
    ~RecordBasedFileManager();

private:
    static RecordBasedFileManager *_rbf_manager;



    unsigned computeRecordLength(const vector<Attribute> &recordDescriptor, const void *data);

    // Two cases for this function:
    // 1) if there is a free page, set pageNum and freeBytes normally
    // 2) if there is no free page, set pageNum to the page number of the next added page (>= current number of pages), and freeBytes to the initial value,
    // Note: when there is no free directory header page, this function will add a new one automatically
    RC seekFreePage(FileHandle &fileHandle, unsigned recordLength, PageNum &pageNum);

    RC writeRecord(byte *page, unsigned recordOffset, const vector<Attribute> &recordDescriptor, const void *data);

    void updateFreeSpace(FileHandle &fileHandle, byte *page, PageNum pageNum, unsigned freeBytes);

    void transmuteRecord(byte *page, unsigned recordOffset, const vector<Attribute> &recordDescriptor, void *data);

    unsigned getFreeBytes(const byte *page)
    {
        return *((uint16_t*) (page + PAGE_SIZE - FREE_SPACE_SZ));
    }

    unsigned getNumOfSlots(const byte *page)
    {
        return *((uint16_t*) (page + PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ));
    }

    void setNumOfSlots(byte *page, unsigned numOfSlots)
    {
        *((uint16_t*) (page + PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ)) = numOfSlots;
    }

    unsigned getRecordOffset(const byte *page, unsigned slotNum)
    {
        return *((uint16_t*) (page
                              + PAGE_SIZE
                              - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ
                              - slotNum*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)));
    }

    void setRecordOffset(byte *page, unsigned slotNum, unsigned recordOffset)
    {
        *((uint16_t*) (page
                       + PAGE_SIZE
                       - FREE_SPACE_SZ
                       - NUM_OF_SLOTS_SZ
                       - slotNum*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ))) = recordOffset;
    }

    unsigned getRecordLength(const byte *page, unsigned slotNum)
    {
        return *((uint16_t*) (page
                              + PAGE_SIZE
                              - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ
                              - slotNum*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)
                              + SLOT_OFFSET_SZ));
    }

    void setRecordLength(byte *page, unsigned slotNum, unsigned recordLength)
    {
        *((uint16_t*) (page
                       + PAGE_SIZE
                       - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ
                       - slotNum*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)
                       + SLOT_OFFSET_SZ)) = recordLength;
    }

    unsigned getBytesOfNullFlags(unsigned numOfFields)
    {
        return ceil(numOfFields / 8.0);
    }

    // return the offset relative to the begin of field data in the record
    unsigned getFieldOffset(const byte *page, unsigned recordOffset, unsigned numOfFields, unsigned fieldNum)
    {
//        uint16_t prefixOffset = recordOffset + NUM_OF_FIELDS_SZ + (int) ceil(numOfFields / 8.0);
        return *((uint16_t*) (page
                              + recordOffset
                              + NUM_OF_FIELDS_SZ
                              + (int) ceil(numOfFields / 8.0)
                              + fieldNum * FIELD_OFFSET_SZ));
    }

    bool readField(const byte *page, unsigned recordOffset, unsigned fieldNum, const Attribute &attribute, void *data);
};

#endif
