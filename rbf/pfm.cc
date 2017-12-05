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
        return FAIL;
    }

    // create a new file with one header page
    ofstream file(fileName, fstream::out | fstream::binary);
    byte header[PAGE_SIZE] = {0};
    header[0] = FILE_ID;   // first byte of the header page is a fingerprint for identifying files created by this function
    file.write(header, PAGE_SIZE);
    return (file) ? SUCCESS : (destroyFile(fileName), FAIL);
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    return (remove(fileName.c_str()) == 0) ? SUCCESS : FAIL;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return fileHandle.openFile(fileName);
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    return fileHandle.closeFile();
}


FileHandle::FileHandle()
{
}


FileHandle::~FileHandle()
{
    closeFile();
}

RC FileHandle::openFile(const string &fileName)
{
    if (file->is_open()) {
        return FAIL;
    }
    file->open(fileName, fstream::in | fstream::out | fstream::binary);
    if (!file->is_open()) {
        return FAIL;
    }
    byte header[PAGE_SIZE];
    file->read(header, PAGE_SIZE);
    if (!file || header[0] != FILE_ID) {
        file->close();
        return FAIL;
    }
    *readPageCounter = *((unsigned*) (header + RD_OFFSET));
    *writePageCounter = *((unsigned*) (header + WR_OFFSET));
    *appendPageCounter = *((unsigned*) (header + APP_OFFSET));
    *numOfPages = *((unsigned*) (header + NUM_OF_PAGES_OFFSET));
    return SUCCESS;
}


RC FileHandle::closeFile()
{
    if (!file->is_open()) {
        return FAIL;
    }

    // update the header page
    file->seekg(0, fstream::beg);
    byte header[PAGE_SIZE];
    file->read(header, PAGE_SIZE);
    memcpy(header + RD_OFFSET, readPageCounter.get(), sizeof(unsigned));
    memcpy(header + WR_OFFSET, writePageCounter.get(), sizeof(unsigned));
    memcpy(header + APP_OFFSET, appendPageCounter.get(), sizeof(unsigned));
    memcpy(header + NUM_OF_PAGES_OFFSET, numOfPages.get(), sizeof(unsigned));
    file->seekp(0, fstream::beg);
    file->write(header, PAGE_SIZE);
    if (!file) {
        return FAIL;
    }
    file = make_shared<fstream>();
    return SUCCESS;
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if (pageNum >= getNumberOfPages()) {
        return FAIL;
    }
    file->seekg((pageNum+1) * PAGE_SIZE, fstream::beg);
    file->read((char*) data, PAGE_SIZE);
    return (file) ? (++(*readPageCounter), SUCCESS) : FAIL;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if (pageNum >= getNumberOfPages()) {
        return FAIL;
    }
    file->seekp((pageNum+1) * PAGE_SIZE, fstream::beg);
    file->write((const char*) data, PAGE_SIZE);
    return (file) ? (++(*writePageCounter), SUCCESS) : FAIL;
}


RC FileHandle::appendPage(const void *data)
{
    file->seekp(0, fstream::end);
    file->write((const char*) data, PAGE_SIZE);
    return (file) ? (++(*appendPageCounter), ++(*numOfPages), SUCCESS) : FAIL;
}


unsigned FileHandle::getNumberOfPages()
{
    return *numOfPages;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = *readPageCounter;
    writePageCount = *writePageCounter;
    appendPageCount = *appendPageCounter;
    return SUCCESS;
}

RC FileHandle::readHeaderPage(void *data)
{
    file->seekg(0, fstream::end);
    if (file->tellg() <= 0) {
        return FAIL;
    }
    file->seekg(0, fstream::beg);
    file->read((char*) data, PAGE_SIZE);
    return (file) ? SUCCESS : FAIL;
}

RC FileHandle::writeHeaderPage(const void *data)
{
    file->seekp(0, fstream::beg);
    file->write((const char*) data, PAGE_SIZE);
    return (file) ? SUCCESS : FAIL;
}
