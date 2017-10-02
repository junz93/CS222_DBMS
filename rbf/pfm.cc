#include <fstream>
#include <cstdio>
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
    // create a new empty file
    // TODO: check success with 'is_open()' or 'stat()'?
    return (ofstream(fileName).is_open()) ? 0 : -1;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    return (remove(fileName.c_str()) == 0) ? 0 : -1;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    fstream &file = fileHandle.file;
    if (file.is_open()) {
        return -1;
    }
    file.open(fileName, fstream::in | fstream::out | fstream::binary);
    return (file.is_open()) ? 0 : -1;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    fstream &file = fileHandle.file;
    if (!file.is_open()) {
        return -1;
    }
    file.close();
    // TODO: check success with 'is_open()' or 'fstream object itself'?
    return (!file.is_open()) ? 0 : -1;
}


FileHandle::FileHandle(): readPageCounter(0), writePageCounter(0), appendPageCounter(0)
{
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if (pageNum >= getNumberOfPages()) {
        return -1;
    }
    file.seekg(pageNum * PAGE_SIZE, fstream::beg);
    file.read((byte*) data, PAGE_SIZE);
    return (file) ? (++readPageCounter, 0) : -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if (pageNum >= getNumberOfPages()) {
        return -1;
    }
    file.seekp(pageNum * PAGE_SIZE, fstream::beg);
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
    auto curPos = file.tellg();
    auto numOfBytes = file.seekg(0, fstream::end).tellg();
    file.seekg(curPos);
    return (numOfBytes < 0) ? 0 : numOfBytes / PAGE_SIZE;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}
