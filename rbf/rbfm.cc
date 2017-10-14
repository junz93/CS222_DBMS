#include <cmath>
#include <cstring>
#include <iostream>
#include "pfm.h"
#include "rbfm.h"
using namespace std;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = nullptr;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return PagedFileManager::instance()->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    if (PagedFileManager::instance()->openFile(fileName, fileHandle) == -1) {
        return -1;
    }
    if (fileHandle.getNumberOfPages() == 0) {
        // add a new directory header page
        byte header[PAGE_SIZE] = {0};
        fileHandle.appendPage(header);
    }
    return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance()->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
                                        const vector<Attribute> &recordDescriptor,
                                        const void *data,
                                        RID &rid)
{
    PageNum numOfPages = fileHandle.getNumberOfPages();

    // compute the length of the new record
    auto recordLength = computeRecordLength(recordDescriptor, data);

    // look for a page with enough free space for the new record
    PageNum headerNum;      // the directory header page number
    unsigned entryNum;      // the entry number in a header page
    byte header[PAGE_SIZE];
    PageNum pageNum;
    uint16_t freeBytes;
    seekFreePage(fileHandle, recordLength, header, headerNum, entryNum, pageNum, freeBytes);

    byte page[PAGE_SIZE] = {0};
    if (pageNum >= numOfPages) {
        if (headerNum >= numOfPages) {
            // prepare the new directory header page
            memset(header, 0, PAGE_SIZE);
        }
        freeBytes = PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ;
    } else {
        fileHandle.readPage(pageNum, page);
    }

    uint16_t numOfSlots = *((uint16_t*) (page + PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ));    // number of slots (including slots that don't contain a valid record)
    uint16_t recordOffset = PAGE_SIZE
                            - freeBytes
                            - FREE_SPACE_SZ
                            - NUM_OF_SLOTS_SZ
                            - numOfSlots*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ);   // offset of the new record in the page

    // add offset and length info of the new record to slot directory in the page
    rid.pageNum = pageNum;
    uint16_t slotNum = 1;
    for (; slotNum <= numOfSlots; ++slotNum) {
        uint16_t sOffset = *((uint16_t*) (page
                                          + PAGE_SIZE
                                          - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ
                                          - slotNum*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)));
        if (sOffset == PAGE_SIZE) {
            break;
        }
    }
    rid.slotNum = slotNum;
    *((uint16_t*) (page + PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ - slotNum*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)))
            = recordOffset;
    *((uint16_t*) (page + PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ - slotNum*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ) + SLOT_OFFSET_SZ))
            = recordLength;

    // update number of free space and number of slots in slot directory and page directory
    if (slotNum > numOfSlots) {
        freeBytes -= recordLength + SLOT_OFFSET_SZ + SLOT_LENGTH_SZ;
        numOfSlots += 1;
    } else {
        freeBytes -= recordLength;
    }
    *((uint16_t*) (page + PAGE_SIZE - FREE_SPACE_SZ)) = freeBytes;
    *((uint16_t*) (page + PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ)) = numOfSlots;
    *((PageNum *) (header + entryNum*(PAGE_NUM_SZ + FREE_SPACE_SZ))) = pageNum;
    *((uint16_t*) (header + entryNum*(PAGE_NUM_SZ + FREE_SPACE_SZ) + PAGE_NUM_SZ)) = freeBytes;

    // write the new record to page
    appendRecord(page, recordOffset, recordDescriptor, data);

    // write the updated page to disk
    if (pageNum >= numOfPages) {
        if (headerNum >= numOfPages) {
            fileHandle.appendPage(header);
        } else {
            fileHandle.writePage(headerNum, header);
        }
        fileHandle.appendPage(page);
    } else {
        fileHandle.writePage(headerNum, header);
        fileHandle.writePage(pageNum, page);
    }

    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
                                      const vector<Attribute> &recordDescriptor,
                                      const RID &rid,
                                      void *data)
{
    byte page[PAGE_SIZE];
    fileHandle.readPage(rid.pageNum, page);
    uint16_t recordOffset = *((uint16_t*) (page
                                           + PAGE_SIZE
                                           - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ
                                           - rid.slotNum*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)));
    if (recordOffset == PAGE_SIZE) {
        // the record in this slot is invalid (deleted)
        return -1;
    }

    auto numOfFields = recordDescriptor.size();

    // copy the null flags from page to record data in memory
    memcpy(data, page + recordOffset + NUM_OF_FIELDS_SZ, (size_t) ceil(numOfFields / 8.0));

    // pointers to page data read from disk
    const byte *pOffsetP = page + recordOffset + NUM_OF_FIELDS_SZ + (int) ceil(numOfFields / 8.0);
    const byte *pFieldP = pOffsetP + numOfFields * FIELD_OFFSET_SZ;
    uint16_t fieldBegin = 0;    // begin offset of a field (relative to the start position of fields)

    // pointers to record data in memory
    byte *pFlag = (byte*) data;
    byte *pData = pFlag + (int) ceil(numOfFields / 8.0);
    uint8_t nullFlag = 0x80;

    for (const Attribute &attr : recordDescriptor) {
        if (!(*pFlag & nullFlag)) {
            uint16_t fieldEnd = *((uint16_t*) pOffsetP);    // end offset of the current field
            uint16_t fieldLength = fieldEnd - fieldBegin;
            switch (attr.type) {
                case TypeInt:
                case TypeReal:
                    memcpy(pData, pFieldP, fieldLength);
                    break;
                case TypeVarChar:
                    *((uint32_t *) pData) = fieldLength;
                    pData += 4;
                    memcpy(pData, pFieldP, fieldLength);
                    break;
            }
            fieldBegin = fieldEnd;      // end offset of current field is begin offset of next field
            pFieldP += fieldLength;
            pData += fieldLength;
        }

        pOffsetP += FIELD_OFFSET_SZ;
        if (nullFlag == 0x01) {
            nullFlag = 0x80;
            ++pFlag;
        } else {
            nullFlag = nullFlag >> 1;
        }
    }
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
    const byte *pFlag = (const byte*) data;
    const byte *pData = pFlag + (int) ceil(recordDescriptor.size() / 8.0);
    uint8_t nullFlag = 0x80;     // cannot use (signed) byte
    for (const Attribute &attr : recordDescriptor) {
        if (*pFlag & nullFlag) {
            cout << attr.name << ": NULL\t";
        } else {
            switch (attr.type) {
                case TypeInt:
                    cout << attr.name << ": " << *((const int32_t *) pData) << '\t';
                    pData += 4;
                    break;
                case TypeReal:
                    cout << attr.name << ": " << *((const float *) pData) << '\t';
                    pData += 4;
                    break;
                case TypeVarChar:
                    uint32_t length = *((const uint32_t *) pData);
                    pData += 4;
                    char str[length + 1];
                    memcpy(str, pData, length);
                    str[length] = 0;
                    cout << attr.name << ": " << str << '\t';
                    pData += length;
                    break;
            }
        }

        if (nullFlag == 0x01) {
            nullFlag = 0x80;
            ++pFlag;
        } else {
            nullFlag = nullFlag >> 1;
        }
    }
    cout << endl;
    return 0;
}

uint16_t RecordBasedFileManager::computeRecordLength(const vector<Attribute> &recordDescriptor, const void *data)
{
    auto numOfFields = recordDescriptor.size();
    uint16_t recordLength = NUM_OF_FIELDS_SZ + ceil(numOfFields / 8.0) + numOfFields*FIELD_OFFSET_SZ;
    const byte *pFlag = (const byte*) data;         // pointer to null flags
    const byte *pData = pFlag + (int) ceil(numOfFields / 8.0);  // pointer to actual field data
    uint8_t nullFlag = 0x80;     // cannot use (signed) byte

    // compute the length of the new record
    for (const Attribute &attr : recordDescriptor) {
        if (!(*pFlag & nullFlag)) {
            switch (attr.type) {
                case TypeInt:
                case TypeReal:
                    recordLength += attr.length;
                    pData += attr.length;
                    break;
                case TypeVarChar:
                    uint32_t length = *((const uint32_t *) pData);
                    recordLength += length;
                    pData += 4 + length;
                    break;
            }
        }

        if (nullFlag == 0x01) {
            nullFlag = 0x80;
            ++pFlag;
        } else {
            nullFlag = nullFlag >> 1;
        }
    }

    return recordLength;
}

RC RecordBasedFileManager::seekFreePage(FileHandle &fileHandle,
                                        uint16_t recordLength,
                                        byte *header,
                                        PageNum &headerNum,
                                        unsigned &entryNum,
                                        PageNum &pageNum,
                                        uint16_t &freeBytes)
{
    auto numOfPages = fileHandle.getNumberOfPages();
    bool hasFreeHeader = false;
    bool hasFreePage = false;

    // scan through all the directory header pages to look for a page with enough free space
    headerNum = 0;
    while (headerNum < numOfPages) {
        fileHandle.readPage(headerNum, header);
        for (entryNum = 0; entryNum < MAX_NUM_OF_ENTRIES; ++entryNum) {
            pageNum = *((PageNum*) (header + 6*entryNum));
            if (pageNum == 0) {
                hasFreeHeader = true;
                break;
            }
            freeBytes = *((uint16_t*) (header + 6*entryNum + sizeof(PageNum)));
            if (freeBytes >= recordLength + SLOT_OFFSET_SZ + SLOT_LENGTH_SZ) {
                hasFreePage = true;
                break;
            }
        }
        // if hasFreePage is true, there is no need to continue scanning
        // if hasFreeHeader is true, we have reached the last directory header page
        if (hasFreePage || hasFreeHeader) {
            break;
        }
        PageNum nextHeaderNum = *((PageNum*) (header + PAGE_SIZE - sizeof(PageNum)));   // next directory header page
        if (nextHeaderNum == 0) {
            // we have reached the last directory header page
            break;
        }
        headerNum = nextHeaderNum;
    }

    if (!hasFreePage) {
        if (!hasFreeHeader) {
            // update the pointer to the next directory header page
            *((PageNum*) (header + PAGE_SIZE - sizeof(PageNum))) = numOfPages;
            fileHandle.writePage(headerNum, header);

            // set numbers for new directory header page and new record page
            headerNum = numOfPages;
            entryNum = 0;
            pageNum = numOfPages + 1;
        } else {
            // set number for new record page
            pageNum = numOfPages;
        }

        // initialize freeBytes for new record page
        freeBytes = PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ;
    }

    return 0;
}

RC RecordBasedFileManager::appendRecord(byte *page,
                                        uint16_t recordOffset,
                                        const vector<Attribute> &recordDescriptor,
                                        const void *data)
{
    auto numOfFields = recordDescriptor.size();
    // write the number of fields of the new record to page
    *((uint16_t*) (page + recordOffset)) = numOfFields;

    // copy the null flags from record data in memory to page
    memcpy(page + recordOffset + NUM_OF_FIELDS_SZ, data, ceil(numOfFields / 8.0));

    // pointers to page data read from disk
    byte *pOffsetP = page + recordOffset + NUM_OF_FIELDS_SZ + (int) ceil(numOfFields / 8.0);   // pointer to field offset
    byte *pFieldP = pOffsetP + numOfFields*FIELD_OFFSET_SZ;     // pointer to actual field data
    uint16_t fieldBegin = 0;    // begin offset of a field (relative to the start position of actual field data)

    // pointers to record data in memory
    const byte *pFlag = (const byte*) data;
    const byte *pData = pFlag + (int) ceil(recordDescriptor.size() / 8.0);
    uint8_t nullFlag = 0x80;

    for (const Attribute &attr : recordDescriptor) {
        if (*pFlag & nullFlag) {
            // this field is NULL
            *((uint16_t*) pOffsetP) = fieldBegin;
        } else {
            switch (attr.type) {
                case TypeInt:
                case TypeReal:
                    *((uint16_t*) pOffsetP) = (fieldBegin += attr.length);
                    memcpy(pFieldP, pData, attr.length);
                    pFieldP += attr.length;
                    pData += attr.length;
                    break;
                case TypeVarChar:
                    uint32_t length = *((const uint32_t*) pData);
                    pData += 4;
                    *((uint16_t*) pOffsetP) = (fieldBegin += length);
                    memcpy(pFieldP, pData, length);
                    pFieldP += length;
                    pData += length;
                    break;
            }
        }

        pOffsetP += 2;
        if (nullFlag == 0x01) {
            nullFlag = 0x80;
            ++pFlag;
        } else {
            nullFlag = nullFlag >> 1;
        }
    }

    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle,
                                        const vector<Attribute> &recordDescriptor,
                                        const void *data,
                                        const RID &rid)
{

}


RC RecordBasedFileManager::scan(FileHandle &fileHandle,
                                const vector<Attribute> &recordDescriptor,
                                const string &conditionAttribute,
                                const CompOp compOp,
                                const void *value,
                                const vector<string> &attributeNames,
                                RBFM_ScanIterator &rbfm_ScanIterator)
{

}