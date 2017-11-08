#include <algorithm>
#include <cmath>
#include <cstring>
#include <iostream>
#include <memory>
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

RC RecordBasedFileManager::createFile(const string &fileName)
{
    return PagedFileManager::instance()->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName)
{
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    if (PagedFileManager::instance()->openFile(fileName, fileHandle) == FAIL) {
        return FAIL;
    }
    if (fileHandle.getNumberOfPages() == 0) {
        // add a new directory header page
        byte header[PAGE_SIZE] = {0};
        fileHandle.appendPage(header);
    }
    return SUCCESS;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle)
{
    return PagedFileManager::instance()->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
                                        const vector<Attribute> &recordDescriptor,
                                        const void *data,
                                        RID &rid)
{
    // compute the length of the new record
    unsigned recordLength = max(POINTER_SZ, computeRecordLength(recordDescriptor, data));
    if (recordLength + SLOT_OFFSET_SZ + SLOT_LENGTH_SZ > PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ) {
        return FAIL;
    }

    // look for a page with enough free space for the new record
    PageNum pageNum;
    seekFreePage(fileHandle, recordLength + SLOT_OFFSET_SZ + SLOT_LENGTH_SZ, pageNum);
    PageNum numOfPages = fileHandle.getNumberOfPages();
    byte page[PAGE_SIZE];
    unsigned freeBytes;
    if (pageNum >= numOfPages) {    // there is no free page, so we have to append a new page
        memset(page, 0, PAGE_SIZE);

        // initialize the free space for a new page
        freeBytes = PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ;
    } else {
        // read the free page from disk
        fileHandle.readPage(pageNum, page);
        freeBytes = getFreeBytes(page);
    }

    unsigned numOfSlots = getNumOfSlots(page);    // number of slots (including slots that don't contain a valid record)
    unsigned recordOffset = PAGE_SIZE
                            - freeBytes
                            - FREE_SPACE_SZ
                            - NUM_OF_SLOTS_SZ
                            - numOfSlots*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ);   // offset of the new record in the page

    // add offset and length info of the new record to slot directory
    rid.pageNum = pageNum;
    unsigned slotNum = 0;   // slot number starts from 1 (not from 0)
    for (; slotNum < numOfSlots; ++slotNum) {
        unsigned sLength = getRecordLength(page, slotNum);
        if (sLength == 0) {
            break;
        }
    }
    rid.slotNum = slotNum;
    setRecordOffset(page, slotNum, recordOffset);
    setRecordLength(page, slotNum, recordLength);

    // update number of free space and number of slots in slot directory and directory header page
    if (slotNum >= numOfSlots) {
        freeBytes -= recordLength + SLOT_OFFSET_SZ + SLOT_LENGTH_SZ;
        setNumOfSlots(page, numOfSlots + 1);
    } else {
        freeBytes -= recordLength;
    }
    updateFreeSpace(fileHandle, page, pageNum, freeBytes);

    // write the new record to page
    writeRecord(page, recordOffset, recordDescriptor, data);

    // write the updated page to disk
    if (pageNum >= numOfPages) {
        fileHandle.appendPage(page);
    } else {
        fileHandle.writePage(pageNum, page);
    }

    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
                                      const vector<Attribute> &recordDescriptor,
                                      const RID &rid,
                                      void *data)
{
    byte page[PAGE_SIZE];
    fileHandle.readPage(rid.pageNum, page);
    unsigned recordLength = getRecordLength(page, rid.slotNum);
    if (recordLength == 0) {    // the record in this slot is invalid (deleted)
        return FAIL;
    }

    unsigned recordOffset = getRecordOffset(page, rid.slotNum);
    if (recordOffset >= PAGE_SIZE) {    // this record has been moved to another page
        PageNum pageNum = *((PageNum*) (page + recordOffset - PAGE_SIZE));
        unsigned slotNum = *((uint16_t*) (page + recordOffset - PAGE_SIZE + PAGE_NUM_SZ));
        fileHandle.readPage(pageNum, page);
        recordOffset = getRecordOffset(page, slotNum);
    }
    readRecord(page, recordOffset, recordDescriptor, data);

    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
    const byte *pFlag = (const byte*) data;
    const byte *pData = pFlag + getBytesOfNullFlags(recordDescriptor.size());
    uint8_t flagMask = 0x80;     // cannot use (signed) byte
    for (const Attribute &attr : recordDescriptor) {
        cout << attr.name << ": ";
        if (*pFlag & flagMask) {
            cout << "NULL  ";
        } else {
            switch (attr.type) {
                case TypeInt:
                    cout << *((const int32_t*) pData) << "  ";
                    pData += 4;
                    break;
                case TypeReal:
                    cout << *((const float*) pData) << "  ";
                    pData += 4;
                    break;
                case TypeVarChar:
                    uint32_t length = *((const uint32_t*) pData);
                    pData += 4;
                    string s(pData, length);
                    cout << s << "  ";
                    pData += length;
                    break;
            }
        }

        if (flagMask == 0x01) {
            flagMask = 0x80;
            ++pFlag;
        } else {
            flagMask = flagMask >> 1;
        }
    }
    cout << endl;

    return SUCCESS;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,
                                        const vector<Attribute> &recordDescriptor,
                                        const RID &rid)
{
    PageNum pageNum = rid.pageNum;
    unsigned slotNum = rid.slotNum;
    byte page[PAGE_SIZE];
    fileHandle.readPage(pageNum, page);

    unsigned recordLength = getRecordLength(page, slotNum);
    if (recordLength == 0) {    // this record has been deleted and should not be deleted again
        return FAIL;
    }

    unsigned recordOffset = getRecordOffset(page, slotNum);
    if (recordOffset >= PAGE_SIZE) {    // this record has been moved to another page (not in the original page)
        recordOffset -= PAGE_SIZE;
        pageNum = *((PageNum*) (page + recordOffset));
        slotNum = *((uint16_t*) (page + recordOffset + PAGE_NUM_SZ));

        // delete the pointer in the original page
        setRecordLength(page, rid.slotNum, 0);
        unsigned freeBytes = getFreeBytes(page);
        unsigned numOfSlots = getNumOfSlots(page);
        unsigned numOfShift = PAGE_SIZE
                              - freeBytes
                              - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ
                              - numOfSlots*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)
                              - recordOffset - POINTER_SZ;
        memmove(page + recordOffset, page + recordOffset + POINTER_SZ, numOfShift);
        for (unsigned slot = 0; slot < numOfSlots; ++slot) {
            unsigned offset = getRecordOffset(page, slot);
            if (offset > PAGE_SIZE + recordOffset || (offset < PAGE_SIZE && offset > recordOffset)) {
                setRecordOffset(page, slot, offset - POINTER_SZ);
            }
        }

        updateFreeSpace(fileHandle, page, rid.pageNum, freeBytes + POINTER_SZ);
        fileHandle.writePage(rid.pageNum, page);

        fileHandle.readPage(pageNum, page);
        recordOffset = getRecordOffset(page, slotNum);
    }

    setRecordLength(page, slotNum, 0);
    unsigned freeBytes = getFreeBytes(page);
    unsigned numOfSlots = getNumOfSlots(page);
    unsigned numOfShift = PAGE_SIZE
                          - freeBytes
                          - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ
                          - numOfSlots*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)
                          - recordOffset - recordLength;

    memmove(page + recordOffset, page + recordOffset + recordLength, numOfShift);
    for (unsigned slot = 0; slot < numOfSlots; ++slot) {
        unsigned offset = getRecordOffset(page, slot);
        if (offset > PAGE_SIZE + recordOffset || (offset < PAGE_SIZE && offset > recordOffset)) {
            setRecordOffset(page, slot, offset - recordLength);
        }
    }

    updateFreeSpace(fileHandle, page, pageNum, freeBytes + recordLength);
    fileHandle.writePage(pageNum, page);

    return SUCCESS;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle,
                                        const vector<Attribute> &recordDescriptor,
                                        const void *data,
                                        const RID &rid)
{
    PageNum pageNum = rid.pageNum;
    unsigned slotNum = rid.slotNum;
    byte page[PAGE_SIZE];
    fileHandle.readPage(pageNum, page);

    // length of the old record
    unsigned recordLength = getRecordLength(page, slotNum);
    if (recordLength == 0) {    // this record has been deleted and should not be updated
        return FAIL;
    }

    // length of the updated record
    unsigned newRecordLength = max(POINTER_SZ, computeRecordLength(recordDescriptor, data));

    if (newRecordLength + SLOT_OFFSET_SZ + SLOT_LENGTH_SZ > PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ) {
        return FAIL;
    }

    // update the length of record in the original page
    if (recordLength != newRecordLength) {
        setRecordLength(page, slotNum, newRecordLength);
    }

    // offset of the old record (or offset of pointer to the old record)
    unsigned recordOffset = getRecordOffset(page, rid.slotNum);

    PageNum dataPageNum;
    unsigned dataSlotNum;
    byte *dataPage = nullptr;     // the page that contains the actual record data
    if (recordOffset >= PAGE_SIZE) {    // this record has been moved to another page (not in the original page)
        recordOffset -= PAGE_SIZE;
        dataPageNum = *((PageNum*) (page + recordOffset));
        dataSlotNum = *((uint16_t*) (page + recordOffset + PAGE_NUM_SZ));
        dataPage = new byte[PAGE_SIZE];
        fileHandle.readPage(dataPageNum, dataPage);
        recordOffset = getRecordOffset(dataPage, dataSlotNum);
    } else {    // this record is in the original page
        dataPageNum = pageNum;
        dataSlotNum = slotNum;
        dataPage = page;
    }
    unsigned freeBytes = getFreeBytes(dataPage);
    unsigned numOfSlots = getNumOfSlots(dataPage);

    // number of bytes shifted when updating or removing the record
    unsigned numOfShift = PAGE_SIZE
                          - freeBytes
                          - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ
                          - numOfSlots*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ)
                          - recordOffset
                          - recordLength;

    if (freeBytes + recordLength >= newRecordLength) {  // update the record in place
        if (recordLength != newRecordLength) {
            if (dataPage != page) { // this record has been moved to another page
                fileHandle.writePage(pageNum, page);
                setRecordLength(dataPage, dataSlotNum, newRecordLength);
            }

            // shift records on the right of the updated record to their new positions
            memmove(dataPage + recordOffset + newRecordLength, dataPage + recordOffset + recordLength, numOfShift);

            // update offset of the shifted records
            for (unsigned slot = 0; slot < numOfSlots; ++slot) {
                unsigned offset = getRecordOffset(dataPage, slot);
                if (offset > PAGE_SIZE + recordOffset || (offset < PAGE_SIZE && offset > recordOffset)) {
                    setRecordOffset(dataPage, slot, offset + newRecordLength - recordLength);
                }
            }

            updateFreeSpace(fileHandle, dataPage, dataPageNum, freeBytes + recordLength - newRecordLength);
        }
        writeRecord(dataPage, recordOffset, recordDescriptor, data);

        fileHandle.writePage(dataPageNum, dataPage);
    } else {    // move the updated record to another page with enough space
        unsigned ptrLength = 0;
        if (dataPage == page) { // replace the old record with a pointer to updated record
            ptrLength = PAGE_NUM_SZ + SLOT_NUM_SZ;  // pointer: (page number, slot number)
            setRecordOffset(page, slotNum, recordOffset + PAGE_SIZE);
        } else {
            setRecordLength(dataPage, dataSlotNum, 0);
        }

        // move left to remove the old record
        memmove(dataPage + recordOffset + ptrLength, dataPage + recordOffset + recordLength, numOfShift);

        // update offset of the shifted records
        for (unsigned slot = 0; slot < numOfSlots; ++slot) {
            unsigned offset = getRecordOffset(dataPage, slot);
            if (offset > PAGE_SIZE + recordOffset || (offset < PAGE_SIZE && offset > recordOffset)) {
                setRecordOffset(dataPage, slot, offset + ptrLength - recordLength);
            }
        }

        updateFreeSpace(fileHandle, dataPage, dataPageNum, freeBytes + recordLength - ptrLength);
        if (dataPage != page) {
            fileHandle.writePage(dataPageNum, dataPage);
        }

        RID newRid;
        insertRecord(fileHandle, recordDescriptor, data, newRid);

        // update the pointer in the original page
        recordOffset = getRecordOffset(page, slotNum);
        *((PageNum*) (page + recordOffset - PAGE_SIZE)) = newRid.pageNum;
        *((uint16_t*) (page + recordOffset - PAGE_SIZE + PAGE_NUM_SZ)) = newRid.slotNum;

        fileHandle.writePage(pageNum, page);
    }

    if (dataPage != page) {
        delete[] dataPage;
    }

    return SUCCESS;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle,
                                         const vector<Attribute> &recordDescriptor,
                                         const RID &rid,
                                         const string &attributeName,
                                         void *data)
{
    auto numOfFields = recordDescriptor.size();
    unsigned attrNum = 0;
    for (; attrNum < numOfFields; ++attrNum) {
        if (recordDescriptor[attrNum].name == attributeName) {
            break;
        }
    }
    if (attrNum == numOfFields) {   // the given attribute name does not exist
        return FAIL;
    }

    PageNum pageNum = rid.pageNum;
    unsigned slotNum = rid.slotNum;
    byte page[PAGE_SIZE];
    fileHandle.readPage(pageNum, page);
    unsigned recordLength = getRecordLength(page, slotNum);
    if (recordLength == 0) {
        return FAIL;
    }
    unsigned recordOffset = getRecordOffset(page, slotNum);
    if (recordOffset >= PAGE_SIZE) {
        recordOffset -= PAGE_SIZE;
        pageNum = *((PageNum*) (page + recordOffset));
        slotNum = *((uint16_t*) (page + recordOffset + PAGE_NUM_SZ));
        fileHandle.readPage(pageNum, page);
        recordOffset = getRecordOffset(page, slotNum);
    }

    byte *pData = (byte*) data + 1;
    if (readField(page, recordOffset, attrNum, numOfFields, recordDescriptor[attrNum], pData) == nullptr) {
        memset(data, 0x80, 1);
    } else {
        memset(data, 0, 1);
    }

    return SUCCESS;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
                                const vector<Attribute> &recordDescriptor,
                                const string &conditionAttribute,
                                const CompOp compOp,
                                const void *value,
                                const vector<string> &attributeNames,
                                RBFM_ScanIterator &rbfm_ScanIterator)
{
    if (!rbfm_ScanIterator.attrNums.empty()) {
        rbfm_ScanIterator.attrNums.clear();
    }

    for (const string &attrName : attributeNames) {
        for (unsigned i = 0; i < recordDescriptor.size(); ++i) {
            if (recordDescriptor[i].name == attrName) {
                rbfm_ScanIterator.attrNums.push_back(i);
                break;
            }
        }
    }
    if (attributeNames.size() != rbfm_ScanIterator.attrNums.size()) {
        return FAIL;
    }

    if (compOp != NO_OP) {
        auto &conditionAttrNum = rbfm_ScanIterator.conditionAttrNum;
        for (conditionAttrNum = 0; conditionAttrNum < recordDescriptor.size(); ++conditionAttrNum) {
            if (recordDescriptor[conditionAttrNum].name == conditionAttribute) {
                break;
            }
        }
        if (conditionAttrNum == recordDescriptor.size()) {
            return FAIL;
        }
    }

    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.value = value;
    rbfm_ScanIterator.fileHandle = &fileHandle;
    rbfm_ScanIterator.containData = false;
    rbfm_ScanIterator.numOfPages = fileHandle.getNumberOfPages();
    rbfm_ScanIterator.pageNum = 0;

    return SUCCESS;
}

unsigned RecordBasedFileManager::computeRecordLength(const vector<Attribute> &recordDescriptor, const void *data)
{
    auto numOfFields = recordDescriptor.size();
    unsigned recordLength = getBytesOfNullFlags(numOfFields) + numOfFields*FIELD_OFFSET_SZ;
    const byte *pFlag = (const byte*) data;         // pointer to null flags
    const byte *pData = pFlag + getBytesOfNullFlags(numOfFields);  // pointer to actual field data
    uint8_t flagMask = 0x80;     // cannot use (signed) byte

    // compute the length of the new record
    for (const Attribute &attr : recordDescriptor) {
        if (!(*pFlag & flagMask)) {
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

        if (flagMask == 0x01) {
            flagMask = 0x80;
            ++pFlag;
        } else {
            flagMask = flagMask >> 1;
        }
    }

    return recordLength;
}

RC RecordBasedFileManager::seekFreePage(FileHandle &fileHandle, unsigned size, PageNum &pageNum)
{
    auto numOfPages = fileHandle.getNumberOfPages();
    bool hasFreeHeader = false;
    bool hasFreePage = false;

    // scan all the directory header pages to look for a page with enough free space
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
            unsigned freeBytes = *((uint16_t*) (header + 6*entryNum + sizeof(PageNum)));
            if (freeBytes >= size) {
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

            // set numbers for new record page
            memset(header, 0, PAGE_SIZE);
            fileHandle.appendPage(header);
            pageNum = numOfPages + 1;
        } else {
            // set number for new record page
            pageNum = numOfPages;
        }
    }

    return SUCCESS;
}

RC RecordBasedFileManager::updateFreeSpace(FileHandle &fileHandle, byte *page, PageNum pageNum, unsigned freeBytes)
{
    *((uint16_t*) (page + PAGE_SIZE - FREE_SPACE_SZ)) = freeBytes;
    unsigned entryNum = pageNum % (MAX_NUM_OF_ENTRIES + 1) - 1;
    PageNum headerNum = pageNum - (entryNum + 1);
    byte header[PAGE_SIZE];
    fileHandle.readPage(headerNum, header);
    *((PageNum*) (header + entryNum*(PAGE_NUM_SZ + FREE_SPACE_SZ))) = pageNum;
    *((uint16_t*) (header + entryNum*(PAGE_NUM_SZ + FREE_SPACE_SZ) + PAGE_NUM_SZ)) = freeBytes;
    fileHandle.writePage(headerNum, header);

    return SUCCESS;
}

void RecordBasedFileManager::writeRecord(byte *page,
                                         unsigned recordOffset,
                                         const vector<Attribute> &recordDescriptor,
                                         const void *data)
{
    auto numOfFields = recordDescriptor.size();

    // copy the null flags from record data in memory to page
    memcpy(page + recordOffset, data, getBytesOfNullFlags(numOfFields));

    // pointers to page data read from disk
    byte *pOffset = page + recordOffset + getBytesOfNullFlags(numOfFields);   // pointer to field offset
    byte *pField = pOffset + numOfFields*FIELD_OFFSET_SZ;     // pointer to actual field data
    unsigned fieldBegin = 0;    // begin offset of a field (relative to the start position of actual field data)

    // pointers to record data in memory
    const byte *pFlag = (const byte*) data;
    const byte *pData = pFlag + getBytesOfNullFlags(numOfFields);
    uint8_t flagMask = 0x80;

    for (const Attribute &attr : recordDescriptor) {
        if (*pFlag & flagMask) {    // this field is NULL
            *((uint16_t*) pOffset) = fieldBegin;
        } else {
            unsigned fieldLength;
            switch (attr.type) {
                case TypeInt:
                case TypeReal:
                    fieldLength = attr.length;
                    break;
                case TypeVarChar:
                    fieldLength = *((const uint32_t*) pData);
                    pData += 4;
                    break;
            }
            *((uint16_t*) pOffset) = (fieldBegin += fieldLength);
            memcpy(pField, pData, fieldLength);
            pField += fieldLength;
            pData += fieldLength;
        }

        pOffset += FIELD_OFFSET_SZ;
        if (flagMask == 0x01) {
            flagMask = 0x80;
            ++pFlag;
        } else {
            flagMask = flagMask >> 1;
        }
    }
}

void RecordBasedFileManager::readRecord(const byte *page,
                                        unsigned recordOffset,
                                        const vector<Attribute> &recordDescriptor,
                                        void *data)
{
    auto numOfFields = recordDescriptor.size();

    // copy the null flags from page to record data returned to caller
    memcpy(data, page + recordOffset, getBytesOfNullFlags(numOfFields));

    // pointers to page data
    const byte *pOffset = page + recordOffset + getBytesOfNullFlags(numOfFields);
    const byte *pField = pOffset + numOfFields * FIELD_OFFSET_SZ;
    unsigned fieldBegin = 0;    // begin offset of a field (relative to the start position of fields)

    // pointers to record data that are returned to caller
    byte *pFlag = (byte*) data;
    byte *pData = pFlag + getBytesOfNullFlags(numOfFields);
    uint8_t flagMask = 0x80;

    for (const auto &attr : recordDescriptor) {
        if (!(*pFlag & flagMask)) {
            unsigned fieldEnd = *((uint16_t*) pOffset);    // end offset of the current field
            unsigned fieldLength = fieldEnd - fieldBegin;
            switch (attr.type) {
                case TypeInt:
                case TypeReal:
                    break;
                case TypeVarChar:
                    *((uint32_t*) pData) = fieldLength;
                    pData += 4;
                    break;
            }
            memcpy(pData, pField, fieldLength);
            fieldBegin = fieldEnd;      // end offset of current field is begin offset of next field
            pField += fieldLength;
            pData += fieldLength;
        }

        pOffset += FIELD_OFFSET_SZ;
        if (flagMask == 0x01) {
            flagMask = 0x80;
            ++pFlag;
        } else {
            flagMask = flagMask >> 1;
        }
    }
}

void* RecordBasedFileManager::readField(const byte *page,
                                        unsigned recordOffset,
                                        unsigned fieldNum,
                                        unsigned numOfFields,
                                        const Attribute &attribute,
                                        void *data)
{
    const byte *pFlag = page + recordOffset + fieldNum / 8;
    uint8_t flagMask = 0x80 >> (fieldNum % 8);
    if (*pFlag & flagMask) {
        return nullptr;
    }

    unsigned beginOffset = getFieldBeginOffset(page, recordOffset, fieldNum, numOfFields);
    unsigned fieldLength = getFieldEndOffset(page, recordOffset, fieldNum, numOfFields) - beginOffset;
    byte *pData = (byte*) data;
    switch (attribute.type) {
        case TypeInt:
        case TypeReal:
            break;
        case TypeVarChar:
            *((uint32_t*) pData) = fieldLength;
            pData += 4;
            break;
    }
    memcpy(pData, page + recordOffset + beginOffset, fieldLength);
    return pData + fieldLength;
}

RBFM_ScanIterator::RBFM_ScanIterator(): rbfm(RecordBasedFileManager::instance())
{
}

RBFM_ScanIterator::~RBFM_ScanIterator()
{
    close();
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
    if (fileHandle == nullptr) {
        return RBFM_EOF;
    }

    for (; pageNum < numOfPages; ++pageNum) {
        if (isHeaderPage(pageNum)) {
            continue;
        }

        if (!containData) {
            containData = true;
            fileHandle->readPage(pageNum, page);
            numOfSlots = rbfm->getNumOfSlots(page);
            slotNum = 0;
        }

        for (; slotNum < numOfSlots; ++slotNum) {
            unsigned recordLength = rbfm->getRecordLength(page, slotNum);
            if (recordLength == 0) {
                continue;
            }
            unsigned recordOffset = rbfm->getRecordOffset(page, slotNum);
            if (recordOffset >= PAGE_SIZE) {
                continue;
            }

            bool compareResult;
            if (compOp == NO_OP) {
                compareResult = true;
            } else {
                unique_ptr<byte[]> field(new byte[recordLength]);
                Attribute conditionAttr = recordDescriptor[conditionAttrNum];
                auto numOfFields = recordDescriptor.size();
                if (!rbfm->readField(page, recordOffset, conditionAttrNum, numOfFields, conditionAttr, field.get())) {
                    field.reset();
                }
                compareResult = compare(conditionAttr.type, compOp, field.get(), value);
            }
            if (compareResult) {
                transmuteRecord(recordOffset, data);
                rid.pageNum = pageNum;
                rid.slotNum = slotNum++;
                return SUCCESS;
            }
        }

        // have scanned all slots in this page
        containData = false;
    }

    return RBFM_EOF;
}

RC RBFM_ScanIterator::close()
{
    delete fileHandle;
    fileHandle = nullptr;
    return SUCCESS;
}

bool RBFM_ScanIterator::compare(AttrType type, CompOp compOp, const void *op1, const void *op2)
{
    if (compOp == NO_OP) {
        return true;
    }
    if (op1 == nullptr && op2 == nullptr) {
        return compOp == EQ_OP;
    }
    if (op1 == nullptr || op2 == nullptr) {
        return compOp == NE_OP;
    }

    switch (type) {
        case TypeInt: {
            int32_t i1 = *((const int32_t*) op1);
            int32_t i2 = *((const int32_t*) op2);
            return compare(compOp, i1, i2);
        }
        case TypeReal: {
            float r1 = *((const float *) op1);
            float r2 = *((const float *) op2);
            return compare(compOp, r1, r2);
        }
        case TypeVarChar: {
            uint32_t len1 = *((const uint32_t*) op1);
            uint32_t len2 = *((const uint32_t*) op2);
            string vc1((const byte*) op1 + 4, len1);
            string vc2((const byte*) op2 + 4, len2);
            return compare(compOp, vc1, vc2);
        }
    }
}

void RBFM_ScanIterator::transmuteRecord(unsigned recordOffset, void *data)
{
    memset(data, 0, rbfm->getBytesOfNullFlags(attrNums.size()));

    byte *pFlag = (byte*) data;
    byte *pData = pFlag + rbfm->getBytesOfNullFlags(attrNums.size());
    uint8_t flagMask = 0x80;
    auto numOfFields = recordDescriptor.size();
    for (auto attrNum : attrNums) {
        Attribute attr = recordDescriptor[attrNum];
        void *pNext = rbfm->readField(page, recordOffset, attrNum, numOfFields, attr, pData);
        if (pNext == nullptr) {
            *pFlag = *pFlag | flagMask;
        } else {
            pData = (byte*) pNext;
        }

        if (flagMask == 0x01) {
            flagMask = 0x80;
            ++pFlag;
        } else {
            flagMask = flagMask >> 1;
        }
    }
}
