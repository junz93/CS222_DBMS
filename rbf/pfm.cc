#include <cstdio>
#include <cstring>
#include <fstream>
#include <sys/stat.h>
#include "pfm.h"
using namespace std;

PagedFileManager* PagedFileManager::_pf_manager = nullptr;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}

RC PagedFileManager::createFile(const string &fileName)
{
    struct stat fileStat;
    if (stat(fileName.c_str(), &fileStat) == 0) {
        // the file has already existed, return error code
        return -1;
    }

    // create a new file with one header page
    ofstream file(fileName, fstream::out | fstream::binary);
    byte header[PAGE_SIZE] = {0};
    header[0] = FILE_IDEN;   // first byte of the header page is a fingerprint for identifying files created by this function
    file.write(header, PAGE_SIZE);
    return (file) ? 0 : (destroyFile(fileName), -1);
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    return (remove(fileName.c_str()) == 0) ? 0 : -1;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return fileHandle.openFile(fileName);
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    return fileHandle.closeFile();
}


FileHandle::FileHandle(): readPageCounter(0), writePageCounter(0), appendPageCounter(0)
{
}


FileHandle::~FileHandle()
{
    closeFile();
}


RC FileHandle::openFile(const string &fileName)
{
    if (file.is_open()) {
        return -1;
    }
    file.open(fileName, fstream::in | fstream::out | fstream::binary);
    if (!file.is_open()) {
        return -1;
    }
    byte header[PAGE_SIZE];
    file.read(header, PAGE_SIZE);
    if (!file || header[0] != FILE_IDEN) {
        file.close();
        return -1;
    }
    readPageCounter = *((unsigned*) (header + RD_OFFSET));
    writePageCounter = *((unsigned*) (header + WR_OFFSET));
    appendPageCounter = *((unsigned*) (header + APP_OFFSET));
    return 0;
}


RC FileHandle::closeFile()
{
    if (!file.is_open()) {
        return -1;
    }

    // update the header page
    file.seekg(0, fstream::beg);
    byte header[PAGE_SIZE];
    file.read(header, PAGE_SIZE);
    memcpy(header + RD_OFFSET, &readPageCounter, sizeof(unsigned));
    memcpy(header + WR_OFFSET, &writePageCounter, sizeof(unsigned));
    memcpy(header + APP_OFFSET, &appendPageCounter, sizeof(unsigned));
    file.seekp(0, fstream::beg);
    file.write(header, PAGE_SIZE);
    file.close();
    return (!file.is_open()) ? 0 : -1;
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if (pageNum >= getNumberOfPages()) {
        return -1;
    }
    file.seekg((pageNum+1) * PAGE_SIZE, fstream::beg);
    file.read((byte*) data, PAGE_SIZE);
    return (file) ? (++readPageCounter, 0) : -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if (pageNum >= getNumberOfPages()) {
        return -1;
    }
    file.seekp((pageNum+1) * PAGE_SIZE, fstream::beg);
    file.write((const byte*) data, PAGE_SIZE);
    return (file) ? (++writePageCounter, 0) : -1;
}


RC FileHandle::appendPage(const void *data)
{
    file.seekp(0, fstream::end);
    file.write((const byte*) data, PAGE_SIZE);
    return (file) ? (++appendPageCounter, 0) : -1;
}


unsigned FileHandle::getNumberOfPages()
{
    auto curPos = file.tellg();     // save the current position of file marker
    auto numOfBytes = file.seekg(0, fstream::end).tellg();
    file.seekg(curPos);     // restore the file marker
    return (numOfBytes <= 0) ? 0 : numOfBytes/PAGE_SIZE - 1;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}
