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
    return PagedFileManager::instance()->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance()->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
                                        const vector<Attribute> &recordDescriptor,
                                        const void *data,
                                        RID &rid) {
    auto numOfFields = recordDescriptor.size();
    uint16_t recordLength = 2 + ceil(numOfFields / 8.0) + 2*numOfFields;
    const byte *pFlag = (const byte*) data;         // pointer to null flags
    const byte *pData = pFlag + (int) ceil(numOfFields / 8.0);  // pointer to actual field data
    unsigned char nullFlag = 0x80;     // cannot use (signed) byte

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

    // look for the page with enough free space for the new record
    byte page[PAGE_SIZE];
    PageNum numOfPages = fileHandle.getNumberOfPages();
    PageNum pageNum = 0;
    uint16_t freeBytes;
    for (; pageNum < numOfPages; ++pageNum) {
        fileHandle.readPage(pageNum, page);
        freeBytes = *((uint16_t*) (page+PAGE_SIZE-2));
        if (freeBytes >= recordLength + 4) {
            break;
        }
    }

    // if no such page, create a new empty page
    if (pageNum >= numOfPages) {
        memset(page, 0, PAGE_SIZE);
        freeBytes = PAGE_SIZE - 4;
//        *((uint16_t*) (page+PAGE_SIZE-2)) = freeBytes;
    }

    uint16_t numOfSlots = *((uint16_t*) (page+PAGE_SIZE-4));    // number of slots (including slots that don't contain a valid record)
    uint16_t recordOffset = PAGE_SIZE - freeBytes - 4 - 4*numOfSlots;   // offset of the new record in the page

    // add offset and length info of the new record to slot directory in the page
    rid.pageNum = pageNum;
    uint16_t slotNum = 1;
    for (; slotNum <= numOfSlots; ++slotNum) {
        if (*((uint16_t*) (page+PAGE_SIZE-4-4*slotNum+2)) == 0) {
            break;
        }
    }
    rid.slotNum = slotNum;
    *((uint16_t*) (page+PAGE_SIZE-4-4*slotNum)) = recordOffset;
    *((uint16_t*) (page+PAGE_SIZE-4-4*slotNum+2)) = recordLength;

    // update number of free space and number of slots in slot directory
    if (slotNum > numOfSlots) {
        *((uint16_t*) (page+PAGE_SIZE-2)) = freeBytes - recordLength - 4;
        *((uint16_t*) (page+PAGE_SIZE-4)) = numOfSlots + 1;
    } else {
        *((uint16_t*) (page+PAGE_SIZE-2)) = freeBytes - recordLength;
    }

    // write the number of fields of the new record to page
    *((uint16_t*) (page + recordOffset)) = numOfFields;

    // copy the null flags from record data in memory to page
    memcpy(page + recordOffset + 2, data, ceil(numOfFields / 8.0));

    // pointers to page data read from disk
    byte *pOffsetP = page + recordOffset + 2 + (int) ceil(numOfFields / 8.0);   // pointer to field offset
    byte *pFieldP = pOffsetP + 2 * numOfFields;     // pointer to actual field data
    uint16_t fieldBegin = 0;    // begin offset of a field (relative to the start position of actual field data)

    // pointers to record data in memory
    pFlag = (const byte*) data;
    pData = pFlag + (int) ceil(recordDescriptor.size() / 8.0);
    nullFlag = 0x80;

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

    // write the updated page to disk
    if (pageNum >= numOfPages) {
        fileHandle.appendPage(page);
    } else {
        fileHandle.writePage(pageNum, page);
    }

    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
                                      const vector<Attribute> &recordDescriptor,
                                      const RID &rid,
                                      void *data) {
    byte page[PAGE_SIZE];
    fileHandle.readPage(rid.pageNum, page);
    uint16_t recordLength = *((uint16_t*) (page+PAGE_SIZE-4-4*rid.slotNum+2));
    if (recordLength == 0) {
        // the record with this slot number is invalid (deleted)
        return -1;
    }

    uint16_t recordOffset = *((uint16_t*) (page+PAGE_SIZE-4-4*rid.slotNum));
    auto numOfFields = recordDescriptor.size();

    // copy the null flags from page to record data in memory
    memcpy(data, page + recordOffset + 2, ceil(numOfFields / 8.0));

    // pointers to page data read from disk
    const byte *pOffsetP = page + recordOffset + 2 + (int) ceil(numOfFields / 8.0);
    const byte *pFieldP = pOffsetP + 2 * numOfFields;
    uint16_t fieldBegin = 0;    // begin offset of a field (relative to the start position of fields)

    // pointers to record data in memory
    byte *pFlag = (byte*) data;
    byte *pData = pFlag + (int) ceil(numOfFields / 8.0);
    unsigned char nullFlag = 0x80;

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

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    const byte *pFlag = (const byte*) data;
    const byte *pData = pFlag + (int) ceil(recordDescriptor.size() / 8.0);
    unsigned char nullFlag = 0x80;     // cannot use (signed) byte
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
