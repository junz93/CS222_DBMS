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
    // compute the length of the new record
    auto recordLength = computeRecordLength(recordDescriptor, data);

    // look for a page with enough free space for the new record
    PageNum pageNum;
    uint16_t freeBytes;
    seekFreePage(fileHandle, recordLength, pageNum, freeBytes);
    PageNum numOfPages = fileHandle.getNumberOfPages();
    byte page[PAGE_SIZE];
    if (pageNum >= numOfPages) {    // there is no free page, so we have to append a new page
        memset(page, 0, PAGE_SIZE);

        // initialize the free space for a new page
        freeBytes = PAGE_SIZE - FREE_SPACE_SZ - NUM_OF_SLOTS_SZ;
    } else {
        // read the free page from disk
        fileHandle.readPage(pageNum, page);
    }

    uint16_t numOfSlots = getNumOfSlots(page);    // number of slots (including slots that don't contain a valid record)
    uint16_t recordOffset = PAGE_SIZE
                            - freeBytes
                            - FREE_SPACE_SZ
                            - NUM_OF_SLOTS_SZ
                            - numOfSlots*(SLOT_OFFSET_SZ + SLOT_LENGTH_SZ);   // offset of the new record in the page

    // add offset and length info of the new record to slot directory
    rid.pageNum = pageNum;
    uint16_t slotNum = 1;   // slot number starts from 1 (not from 0)
    for (; slotNum <= numOfSlots; ++slotNum) {
        uint16_t sLength = getRecordLength(page, slotNum);
        if (sLength == 0) {
            break;
        }
    }
    rid.slotNum = slotNum;
    setRecordOffset(page, slotNum, recordOffset);
    setRecordLength(page, slotNum, recordLength);

    // update number of free space and number of slots in slot directory and directory header page
    if (slotNum > numOfSlots) {
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

    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
                                      const vector<Attribute> &recordDescriptor,
                                      const RID &rid,
                                      void *data)
{
    byte page[PAGE_SIZE];
    fileHandle.readPage(rid.pageNum, page);
    uint16_t recordLength = getRecordLength(page, rid.slotNum);
    if (recordLength == 0) {    // the record in this slot is invalid (deleted)
        return -1;
    }

    uint16_t recordOffset = getRecordOffset(page, rid.slotNum);
    if (recordOffset >= PAGE_SIZE) {    // this record has been moved to another page
        PageNum pageNum = *((PageNum*) (page + recordOffset - PAGE_SIZE));
        recordOffset = *((uint16_t*) (page + recordOffset - PAGE_SIZE + PAGE_NUM_SZ));
        fileHandle.readPage(pageNum, page);
    }
    transmuteRecord(page, recordOffset, recordDescriptor, data);

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

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,
                                        const vector<Attribute> &recordDescriptor,
                                        const RID &rid)
{

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
    uint16_t recordLength = getRecordLength(page, slotNum);
    if (recordLength == 0) {    // this record has been deleted and should not be updated
        return -1;
    }

    // length of the updated record
    uint16_t newRecordLength = computeRecordLength(recordDescriptor, data);

    // update the length of record in the original page
    if (recordLength != newRecordLength) {
        setRecordLength(page, slotNum, newRecordLength);
    }

    // offset of the old record (or offset of pointer to the old record)
    uint16_t recordOffset = getRecordOffset(page, slotNum);

    PageNum dataPageNum;
    uint16_t dataSlotNum;
    byte *dataPage;     // the page that contains the actual record data
    if (recordOffset >= PAGE_SIZE) {    // this record has been moved to another page (not in the original page)
        dataPageNum = *((PageNum*) (page + recordOffset - PAGE_SIZE));
        dataSlotNum = *((uint16_t*) (page + recordOffset - PAGE_SIZE + PAGE_NUM_SZ));
        dataPage = new byte[PAGE_SIZE];
        fileHandle.readPage(dataPageNum, dataPage);
        recordOffset = getRecordOffset(dataPage, dataSlotNum);
    } else {    // this record is in the original page
        dataPageNum = pageNum;
        dataSlotNum = slotNum;
        dataPage = page;
    }
    uint16_t freeBytes = getFreeBytes(dataPage);
    uint16_t numOfSlots = getNumOfSlots(dataPage);

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
            memmove(dataPage + recordOffset + newRecordLength,
                    dataPage + recordOffset + recordLength,
                    numOfShift);

            // update offset of the shifted records
            for (uint16_t slot = 1; slot <= numOfSlots; ++slot) {
                uint16_t offset = getRecordOffset(dataPage, slot);
                if (offset > recordOffset) {
                    setRecordOffset(dataPage, slot, offset + newRecordLength - recordLength);
                }
            }

            updateFreeSpace(fileHandle, dataPage, dataPageNum, freeBytes + recordLength - newRecordLength);
        }
        writeRecord(dataPage, recordOffset, recordDescriptor, data);

        fileHandle.writePage(dataPageNum, dataPage);
    } else {    // move the updated record to another page with enough space
        uint16_t ptrLength = 0;
        if (dataPage == page) {
            ptrLength = PAGE_NUM_SZ + SLOT_NUM_SZ;  // pointer: (page number, slot number)
            setRecordOffset(page, slotNum, recordOffset + PAGE_SIZE);
        } else {
            setRecordLength(dataPage, dataSlotNum, 0);
        }

        // move left to remove the old record
        memmove(dataPage + recordOffset + ptrLength,
                dataPage + recordOffset + recordLength,
                numOfShift);

        // update offset of the shifted records
        for (uint16_t slot = 1; slot <= numOfSlots; ++slot) {
            uint16_t offset = getRecordOffset(dataPage, slot);
            if (offset > recordOffset) {
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

    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle,
                                         const vector<Attribute> &recordDescriptor,
                                         const RID &rid,
                                         const string &attributeName,
                                         void *data)
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

    for (unsigned i = 0; i < recordDescriptor.size(); ++i) {
        if (recordDescriptor[i].name == conditionAttribute) {
            rbfm_ScanIterator.conditionAttrNum = i;
            break;
        }
    }

    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.value = value;
    rbfm_ScanIterator.fileHandle = &fileHandle;
    rbfm_ScanIterator.containData = false;
    rbfm_ScanIterator.numOfPages = fileHandle.getNumberOfPages();
    rbfm_ScanIterator.pageNum = 0;
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

void RecordBasedFileManager::updateFreeSpace(FileHandle &fileHandle, byte *page, PageNum pageNum, uint16_t freeBytes)
{
    *((uint16_t*) (page + PAGE_SIZE - FREE_SPACE_SZ)) = freeBytes;
    unsigned entryNum = pageNum % (MAX_NUM_OF_ENTRIES + 1) - 1;
    PageNum headerNum = pageNum - (entryNum + 1);
    byte header[PAGE_SIZE];
    fileHandle.readPage(headerNum, header);
    *((PageNum*) (header + entryNum*(PAGE_NUM_SZ + FREE_SPACE_SZ))) = pageNum;
    *((uint16_t*) (header + entryNum*(PAGE_NUM_SZ + FREE_SPACE_SZ) + PAGE_NUM_SZ)) = freeBytes;
    fileHandle.writePage(headerNum, header);
}

void RecordBasedFileManager::transmuteRecord(byte *page,
                                             uint16_t recordOffset,
                                             const vector<Attribute> &recordDescriptor,
                                             void *data)
{
    auto numOfFields = recordDescriptor.size();

    // copy the null flags from page to record data returned to caller
    memcpy(data, page + recordOffset + NUM_OF_FIELDS_SZ, (size_t) ceil(numOfFields / 8.0));

    // pointers to page data
    const byte *pOffsetP = page + recordOffset + NUM_OF_FIELDS_SZ + (int) ceil(numOfFields / 8.0);
    const byte *pFieldP = pOffsetP + numOfFields * FIELD_OFFSET_SZ;
    uint16_t fieldBegin = 0;    // begin offset of a field (relative to the start position of fields)

    // pointers to record data that is returned to caller
    byte *pFlag = (byte*) data;
    byte *pData = pFlag + (int) ceil(numOfFields / 8.0);
    uint8_t nullFlag = 0x80;

    for (const auto &attr : recordDescriptor) {
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
}

bool RecordBasedFileManager::readField(const byte *page, unsigned recordOffset, unsigned fieldNum,
                                       const Attribute &attribute, void *data)
{
    const byte *pFlag = page + recordOffset + NUM_OF_FIELDS_SZ + fieldNum / 8;
    uint8_t nullFlag = 0x80;
    nullFlag = nullFlag >> (fieldNum % 8);
    if (*pFlag & nullFlag) {
        return false;
    }

    unsigned numOfFields = *((uint16_t*) (page + recordOffset));
    unsigned beginOffset = (fieldNum == 0 ? 0 : getFieldOffset(page, recordOffset, numOfFields, fieldNum - 1));
    unsigned fieldLength = getFieldOffset(page, recordOffset, numOfFields, fieldNum) - beginOffset;
    const byte *pField = page
                         + recordOffset
                         + NUM_OF_FIELDS_SZ + (int) ceil(numOfFields / 8.0) + numOfFields * FIELD_OFFSET_SZ
                         + beginOffset;
    switch (attribute.type) {
        case TypeInt:
        case TypeReal:
            memcpy(data, pField, fieldLength);
            break;
        case TypeVarChar:
            *((uint32_t*) data) = fieldLength;
            memcpy(data + 4, pField, fieldLength);
            break;
    }
    return true;
}

RBFM_ScanIterator::RBFM_ScanIterator():rbfm(RecordBasedFileManager::instance())
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
            slotNum = 1;
        }

        for (; slotNum <= numOfSlots; ++slotNum) {
            unsigned recordLength = rbfm->getRecordLength(page, slotNum);
            if (recordLength == 0) {
                continue;
            }
            unsigned recordOffset = rbfm->getRecordOffset(page, slotNum);
            if (recordOffset >= PAGE_SIZE) {
                continue;
            }

            void *field = new byte[recordLength];
            Attribute conditionAttr = recordDescriptor[conditionAttrNum];
            if (!rbfm->readField(page, recordOffset, conditionAttrNum, conditionAttr, field)) {
                delete[] field;
                field = nullptr;
            }
            if (compare(conditionAttr.type, field, value, compOp)) {
                // initialize the null flags
                memset(data, 0, (size_t) ceil(attrNums.size()/8.0));

                byte *pFlag = (byte*) data;
                byte *pData = pFlag + (int) ceil(attrNums.size()/8.0);
                uint8_t nullFlag = 0x80;
                for (auto attrNum : attrNums) {
                    Attribute attr = recordDescriptor[attrNum];
                    if (!rbfm->readField(page, recordOffset, attrNum, attr, pData)) { // null field
                        *pFlag = *pFlag | nullFlag;
                    } else {
                        switch (attr.type) {
                            case TypeInt:
                            case TypeReal:
                                pData += attr.length;
                                break;
                            case TypeVarChar:
                                uint32_t length = *((uint32_t*) pData);
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
                if (field != nullptr) {
                    delete[] field;
                }
                ++slotNum;
                return 0;
            }
            if (field != nullptr) {
                delete[] field;
            }
        }

        containData = false;
    }

    return RBFM_EOF;
}

RC RBFM_ScanIterator::close()
{
    fileHandle = nullptr;
    return -1;
}

bool RBFM_ScanIterator::compare(AttrType type, const void *op1, const void *op2, CompOp compOp)
{
    if (compOp == NO_OP) {
        return true;
    }
    if (op1 == nullptr && op2 == nullptr) {
        if (compOp == EQ_OP) {
            return true;
        }
        return false;
    }
    if (op1 == nullptr || op2 == nullptr) {
        if (compOp == NE_OP) {
            return true;
        }
        return false;
    }

    switch (type) {
        case TypeInt:
            int32_t i1 = *((int32_t*) op1);
            int32_t i2 = *((int32_t*) op2);
            switch (compOp) {
                case EQ_OP:
                    return i1 == i2;
                case LT_OP:
                    return i1 < i2;
                case LE_OP:
                    return i1 <= i2;
                case GT_OP:
                    return i1 > i2;
                case GE_OP:
                    return i1 >= i2;
                case NE_OP:
                    return i1 != i2;
            }
            break;
        case TypeReal:
            float r1 = *((float*) op1);
            float r2 = *((float*) op2);
            switch (compOp) {
                case EQ_OP:
                    return r1 == r2;
                case LT_OP:
                    return r1 < r2;
                case LE_OP:
                    return r1 <= r2;
                case GT_OP:
                    return r1 > r2;
                case GE_OP:
                    return r1 >= r2;
                case NE_OP:
                    return r1 != r2;
            }
        case TypeVarChar:
            uint32_t len1 = *((uint32_t*) op1);
            uint32_t len2 = *((uint32_t*) op2);
            string vc1((char*) op1 + 4, len1);
            string vc2((char*) op2 + 4, len2);
            switch (compOp) {
                case EQ_OP:
                    return vc1 == vc2;
                case LT_OP:
                    return vc1 < vc2;
                case LE_OP:
                    return vc1 <= vc2;
                case GT_OP:
                    return vc1 > vc2;
                case GE_OP:
                    return vc1 >= vc2;
                case NE_OP:
                    return vc1 != vc2;
            }
            break;
    }
}