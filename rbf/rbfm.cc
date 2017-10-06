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

    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
                                      const vector<Attribute> &recordDescriptor,
                                      const RID &rid,
                                      void *data) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    const byte *pFlag = (const byte*) data;
    const void *pData = data + (int) ceil(recordDescriptor.size() / 8.0);
    unsigned char nullFlag = 0x80;     // cannot use (signed) byte
    for (const Attribute &attr : recordDescriptor) {
        byte flag = *pFlag & nullFlag;
        if (nullFlag == 0x01) {
            nullFlag = 0x80;
            ++pFlag;
        } else {
            nullFlag = nullFlag >> 1;
        }
        if (flag) {
            cout << attr.name << ": NULL\t";
            continue;
        }
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
                char str[length+1];
                memcpy(str, pData, length);
                str[length] = 0;
                cout << attr.name << ": " << str << '\t';
                pData += length;
        }
        cout << endl;
    }
    return 0;
}
