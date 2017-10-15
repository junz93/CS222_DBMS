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
                                        RID &rid)
{
    PageNum numOfPages = fileHandle.getNumberOfPages();

    // compute the length of the new record
    auto recordLength = computeRecordLength(recordDescriptor, data);

    // look for a page with enough free space for the new record
    PageNum pageNum;
    uint16_t freeBytes;
    seekFreePage(fileHandle, recordLength, pageNum, freeBytes);

    byte page[PAGE_SIZE] = {0};
    if (pageNum >= numOfPages) {
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
        uint16_t sLength = *((uint16_t*) (page
                                          + PAGE_SIZE
                                          - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ
                                          - slotNum*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)
                                          + SLOT_OFFSET_SZ));
        if (sLength == 0) {
            break;
        }
    }
    rid.slotNum = slotNum;
    *((uint16_t*) (page + PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ - slotNum*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)))
            = recordOffset;
    *((uint16_t*) (page + PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ - slotNum*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ) + SLOT_OFFSET_SZ))
            = recordLength;

    // update number of free space and number of slots in slot directory and directory header page
    if (slotNum > numOfSlots) {
        freeBytes -= recordLength + SLOT_OFFSET_SZ + SLOT_LENGTH_SZ;
        numOfSlots += 1;
    } else {
        freeBytes -= recordLength;
    }
    updateFreeSpace(fileHandle, page, pageNum, freeBytes);
    *((uint16_t*) (page + PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ)) = numOfSlots;
//    updatePageDirectory(header, entryNum, pageNum, freeBytes);

    // write the new record to page
    writeRecord(page, recordOffset, recordDescriptor, data);

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
                                      void *data)
{
    byte page[PAGE_SIZE];
    fileHandle.readPage(rid.pageNum, page);
    uint16_t recordLength = *((uint16_t*) (page
                                           + PAGE_SIZE
                                           - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ
                                           - rid.slotNum*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)
                                           + SLOT_OFFSET_SZ));
    if (recordLength == 0) {
        // the record in this slot is invalid (deleted)
        return -1;
    }

    uint16_t recordOffset = *((uint16_t*) (page
                                           + PAGE_SIZE
                                           - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ
                                           - rid.slotNum*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)));

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
                    *((uint32_t*) pData) = fieldLength;
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
                    cout << attr.name << ": " << *((const int32_t*) pData) << '\t';
                    pData += 4;
                    break;
                case TypeReal:
                    cout << attr.name << ": " << *((const float*) pData) << '\t';
                    pData += 4;
                    break;
                case TypeVarChar:
                    uint32_t length = *((const uint32_t*) pData);
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

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle,
                                        const vector<Attribute> &recordDescriptor,
                                        const void *data,
                                        const RID &rid)
{
    PageNum pageNum = rid.pageNum;
    uint16_t slotNum = rid.slotNum;
    byte page[PAGE_SIZE];
    fileHandle.readPage(pageNum, page);

    // length of the old record
    uint16_t recordLength = *((uint16_t*) (page
                                           + PAGE_SIZE
                                           - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ
                                           - slotNum*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)
                                           + SLOT_OFFSET_SZ));
    if (recordLength == 0) {
        // this record has been deleted and should not be updated
        return -1;
    }

    // offset of the old record
    uint16_t recordOffset = *((uint16_t*) (page
                                           + PAGE_SIZE
                                           - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ
                                           - slotNum*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)));

    // length of the updated record
    uint16_t newRecordLength = computeRecordLength(recordDescriptor, data);

    // update the length of record in the original page
    *((uint16_t*) (page
                   + PAGE_SIZE
                   - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ
                   - slotNum*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)
                   + SLOT_OFFSET_SZ)) = newRecordLength;

    if (recordOffset >= PAGE_SIZE) {
        // === this record has been moved to another page (not in the original page) ===
        // in this case, (recordOffset - PAGE_SIZE) is the offset of the pointer to the actual record data

        // get the actual page containing this record
        PageNum actualPageNum = *((PageNum *) (page + recordOffset - PAGE_SIZE));
        uint16_t actualRecordOffset = *((uint16_t*) (page + recordOffset - PAGE_SIZE + sizeof(PageNum)));
        byte actualPage[PAGE_SIZE];
        fileHandle.readPage(actualPageNum, actualPage);
        uint16_t freeBytes = *((uint16_t*) (actualPage + PAGE_SIZE - FREE_SPACE_SZ));

        if (freeBytes + recordLength >= newRecordLength) {
            // === update the record in the actual page ===

            if (recordLength != newRecordLength) {
                freeBytes = freeBytes + recordLength - newRecordLength;
                *((uint16_t *) (actualPage + PAGE_SIZE - FREE_SPACE_SZ)) = freeBytes;
                byte header[PAGE_SIZE];
                PageNum headerNum = actualPageNum / (MAX_NUM_OF_ENTRIES + 1);
                unsigned entryNum = actualPageNum % (MAX_NUM_OF_ENTRIES + 1);
                fileHandle.readPage(headerNum, header);
                updatePageDirectory(header, entryNum, actualPageNum, freeBytes);

                uint16_t numOfSlots = *((uint16_t *) (actualPage + PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ));
                unsigned numOfShift = PAGE_SIZE
                                      - freeBytes
                                      - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ
                                      - numOfSlots * (SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)
                                      - actualRecordOffset
                                      - recordLength;
                // shift right to make room for the updated record
                memmove(actualPage + actualRecordOffset + newRecordLength,
                        actualPage + actualRecordOffset + recordLength,
                        numOfShift);
            }

            writeRecord(actualPage, actualRecordOffset, recordDescriptor, data);

            fileHandle.writePage(actualPageNum, actualPage);
            fileHandle.writePage(pageNum, page);
        } else {
            // === move the updated record to another page with enough space ===

            *((uint16_t*) (actualPage + PAGE_SIZE - FREE_SPACE_SZ)) = freeBytes + recordLength;
            uint16_t numOfSlots = *((uint16_t*) (actualPage + PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ));
            unsigned numOfShift = PAGE_SIZE
                                  -freeBytes
                                  -FREE_SPACE_SZ-NUM_OF_SLOTS_SZ
                                  -numOfSlots*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)
                                  -actualRecordOffset
                                  -recordLength;

            // move left to remove the old record from the actual page
            memmove(actualPage + actualRecordOffset,
                    actualPage + actualRecordOffset + recordLength,
                    numOfShift);
            fileHandle.writePage(actualPageNum, actualPage);

            auto numOfPages = fileHandle.getNumberOfPages();
            PageNum headerNum;
            unsigned entryNum;
            byte header[PAGE_SIZE];
//            seekFreePage(fileHandle, newRecordLength, header, headerNum, entryNum, actualPageNum, freeBytes);

            if (actualPageNum >= numOfPages) {
                if (headerNum >= numOfPages) {
                    memset(header, 0, PAGE_SIZE);
                }
                memset(actualPage, 0, PAGE_SIZE);
                freeBytes = PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ;
            } else {
                fileHandle.readPage(actualPageNum, actualPage);
            }

            numOfSlots = *((uint16_t*) (actualPage + PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ));    // number of slots (including slots that don't contain a valid record)
            actualRecordOffset = PAGE_SIZE
                                 - freeBytes
                                 - FREE_SPACE_SZ
                                 - NUM_OF_SLOTS_SZ
                                 - numOfSlots*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ);   // offset of the new record in the page

            // update the original page (update the pointer)
            *((PageNum*) (page + recordOffset)) = actualPageNum;
            *((uint16_t*) (page + recordOffset + PAGE_NUM_SZ)) = actualRecordOffset;

            // update free space in the actual page and the directory header page
            freeBytes -= newRecordLength;
            *((uint16_t*) (actualPage + PAGE_SIZE - FREE_SPACE_SZ)) = freeBytes;
            *((PageNum *) (header + entryNum*(PAGE_NUM_SZ + FREE_SPACE_SZ))) = actualPageNum;
            *((uint16_t*) (header + entryNum*(PAGE_NUM_SZ + FREE_SPACE_SZ) + PAGE_NUM_SZ)) = freeBytes;

            writeRecord(actualPage, actualRecordOffset, recordDescriptor, data);

            // write the updated pages to disk
            if (pageNum >= numOfPages) {
                if (headerNum >= numOfPages) {
                    fileHandle.appendPage(header);
                } else {
                    fileHandle.writePage(headerNum, header);
                }
                fileHandle.appendPage(actualPage);
            } else {
                fileHandle.writePage(headerNum, header);
                fileHandle.writePage(actualPageNum, actualPage);
            }
            fileHandle.writePage(pageNum, page);
        }

    } else {
        // === this record is in the original page ===
        // TODO: not implemented yet

    }

    return 0;
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
                    uint32_t length = *((const uint32_t*) pData);
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
//                                        byte *header,
//                                        PageNum &headerNum,
//                                        unsigned &entryNum,
                                        PageNum &pageNum,
                                        uint16_t &freeBytes)
{
    auto numOfPages = fileHandle.getNumberOfPages();
    bool hasFreeHeader = false;
    bool hasFreePage = false;

    // scan through all the directory header pages to look for a page with enough free space
    PageNum headerNum = 0;
    byte header[PAGE_SIZE];
    while (headerNum < numOfPages) {
        fileHandle.readPage(headerNum, header);
        for (unsigned entryNum = 0; entryNum < MAX_NUM_OF_ENTRIES; ++entryNum) {
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
            if (numOfPages > 0) {
                // update the pointer to the next directory header page
                *((PageNum*) (header + PAGE_SIZE - sizeof(PageNum))) = numOfPages;
                fileHandle.writePage(headerNum, header);
            }

            // set numbers for new record page
            memset(header, 0, PAGE_SIZE);
            fileHandle.appendPage(header);
            pageNum = numOfPages + 1;
        } else {
            // set number for new record page
            pageNum = numOfPages;
        }

        // initialize freeBytes for new record page
//        freeBytes = PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ;
    }

    return 0;
}

// TODO: is it necessary to store the number of fields in the record itself?
RC RecordBasedFileManager::writeRecord(byte *page,
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

void RecordBasedFileManager::updatePageDirectory(byte *header, unsigned entryNum, PageNum pageNum, uint16_t freeBytes)
{
    *((PageNum*) (header + entryNum*(PAGE_NUM_SZ + FREE_SPACE_SZ))) = pageNum;
    *((uint16_t*) (header + entryNum*(PAGE_NUM_SZ + FREE_SPACE_SZ) + PAGE_NUM_SZ)) = freeBytes;
}

void RecordBasedFileManager::updateFreeSpace(FileHandle &fileHandle, byte *page, PageNum pageNum, uint16_t freeBytes)
{
    *((uint16_t *) (page + PAGE_SIZE - FREE_SPACE_SZ)) = freeBytes;
    unsigned entryNum = pageNum % (MAX_NUM_OF_ENTRIES + 1) - 1;
    PageNum headerNum = pageNum - (entryNum + 1);
    byte header[PAGE_SIZE];
    fileHandle.readPage(headerNum, header);
    updatePageDirectory(header, entryNum, pageNum, freeBytes);
    fileHandle.writePage(headerNum, header);
}
