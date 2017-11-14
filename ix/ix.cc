
#include <cstring>
#include <iostream>
#include "ix.h"

IndexManager* IndexManager::_index_manager = nullptr;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    return pfm->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName)
{
    return pfm->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    if (pfm->openFile(fileName, ixfileHandle.fileHandle) == FAIL) {
        return FAIL;
    }

    // TODO: create the initial root node (leaf node) in openFile or insertEntry?
    if (ixfileHandle.getNumberOfPages() == 0) {
        byte root[PAGE_SIZE] = {0};
        setFreeSpace(root, PAGE_SIZE - LEAF_HEADER_SZ);
        setLeaf(root);
        ixfileHandle.appendPage(root);
    }
    return SUCCESS;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return pfm->closeFile(ixfileHandle.fileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    PageNum rootNum = getRoot(ixfileHandle);
    bool isSplit;
    byte *newChildKey = new byte[attribute.length + 4];
    RID newChildRid;
    PageNum newChildNum;
    if (insertEntry(ixfileHandle, rootNum, attribute, key, rid, isSplit, newChildKey, newChildRid, newChildNum) == FAIL) {
        return FAIL;
    }
    if (isSplit) {
        PageNum newRootNum = ixfileHandle.getNumberOfPages();
        byte newRoot[PAGE_SIZE] = {0};
        unsigned keyLength = getKeyLength(attribute, newChildKey);
        memcpy(newRoot + NONLEAF_HEADER_SZ, &rootNum, NODE_PTR_SZ);
        memcpy(newRoot + NONLEAF_HEADER_SZ + NODE_PTR_SZ, newChildKey, keyLength);
        *((PageNum*) (newRoot + NONLEAF_HEADER_SZ + NODE_PTR_SZ + keyLength)) = newChildRid.pageNum;
        *((uint16_t*) (newRoot + NONLEAF_HEADER_SZ + NODE_PTR_SZ + keyLength + PAGE_NUM_SZ)) = newChildRid.slotNum;
        *((PageNum*) (newRoot + NONLEAF_HEADER_SZ + NODE_PTR_SZ + keyLength + RID_SZ)) = newChildNum;
        setFreeSpace(newRoot, PAGE_SIZE - NONLEAF_HEADER_SZ - 2*NODE_PTR_SZ - keyLength - RID_SZ);
        ixfileHandle.appendPage(newRoot);
        setRoot(ixfileHandle, newRootNum);
    }

    delete[] newChildKey;
    return SUCCESS;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    PageNum nodeNum = getRoot(ixfileHandle);
    byte node[PAGE_SIZE];
    while (true) {
        ixfileHandle.readPage(nodeNum, node);
        if (!isLeaf(node)) {
            unsigned childNumOffset = findChildNumOffset(node, attribute, key, rid);
            nodeNum = *((PageNum*) (node + childNumOffset));
        } else {
            unsigned freeBytes = getFreeSpace(node);
            unsigned offset = LEAF_HEADER_SZ;
            while (offset < PAGE_SIZE - freeBytes) {
                const void *curKey = node + offset;
                unsigned keyLength = getKeyLength(attribute, curKey);
                RID curRid;
                loadRid(node, offset + keyLength, curRid);
                if (compareKey(attribute, key, rid, curKey, curRid) == 0) {
                    unsigned entryLength = keyLength + RID_SZ;
                    memmove(node + offset, node + offset + entryLength, PAGE_SIZE - freeBytes - offset - entryLength);
                    setFreeSpace(node, freeBytes + entryLength);
                    ixfileHandle.writePage(nodeNum, node);
                    return SUCCESS;
                }
                offset += keyLength + RID_SZ;
            }
            // the data entry (key, rid) does not exist
            return FAIL;
        }
    }
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{
    PageNum rootNum = getRoot(ixfileHandle);
    printBtree(ixfileHandle, rootNum, attribute, 0);
    cout << endl;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, PageNum nodeNum,
                              const Attribute &attribute, unsigned level) const
{
    byte node[PAGE_SIZE];
    ixfileHandle.readPage(nodeNum, node);
    unsigned freeBytes = getFreeSpace(node);
    if (!isLeaf(node)) {
        vector<PageNum> childNums;
        cout << string(4*level, ' ') << "{\"keys\": [";
        unsigned keyOffset = NONLEAF_HEADER_SZ + NODE_PTR_SZ;
        childNums.push_back(*((PageNum*) (node + keyOffset - NODE_PTR_SZ)));
        while (keyOffset < PAGE_SIZE - freeBytes) {
            if (childNums.size() != 1) {
                cout << ',';
            }
            cout << '\"';
            keyOffset += printKey(attribute, node + keyOffset);
            cout << '(' << *((PageNum*) (node + keyOffset)) << ',';
            cout << *((uint16_t*) (node + keyOffset + PAGE_NUM_SZ)) << ')';
            cout << '\"';
            keyOffset += RID_SZ;
            childNums.push_back(*((PageNum*) (node + keyOffset)));
            keyOffset += NODE_PTR_SZ;
        }
        cout << "]," << endl;
        cout << string(4*level, ' ') << " \"children\": [" << endl;
        for (unsigned i = 0; i < childNums.size(); ++i) {
            if (i != 0) {
                cout << ',' << endl;
            }
            printBtree(ixfileHandle, childNums[i], attribute, level + 1);
        }
        cout << endl << string(4*level, ' ') << "]}";
    } else {
        cout << string(4*level, ' ') << "{\"keys\": [";
        const void *curKey = nullptr;
        unsigned curKeyLength;
        bool isFirst = true;
        unsigned offset = LEAF_HEADER_SZ;
        while (offset < PAGE_SIZE - freeBytes) {
            if (curKey == nullptr || compareKey(attribute, node + offset, curKey) != 0) {
                if (curKey != nullptr) {
                    cout << "]\",";
                }
                curKey = node + offset;
                isFirst = true;
                cout << '\"';
                curKeyLength = printKey(attribute, curKey);
                cout << ":[";
            }

            if (!isFirst) {
                cout << ',';
            } else {
                isFirst = false;
            }
            cout << '(' << *((PageNum*) (node + offset + curKeyLength)) << ',';
            cout << *((uint16_t*) (node + offset + curKeyLength + PAGE_NUM_SZ)) << ')';
            offset += curKeyLength + RID_SZ;
        }
        cout << "]\"";
        cout << "]}";
    }
}

unsigned IndexManager::printKey(const Attribute &attribute, const void *key) const
{
    switch (attribute.type) {
        case TypeInt:
            cout << *((const int32_t*) key);
            return attribute.length;
        case TypeReal:
            cout << *((const float*) key);
            return attribute.length;
        case TypeVarChar:
            unsigned length = *((uint32_t*) key);
            cout << string((const char*) key + 4, length);
            return length + 4;
    }
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, PageNum nodeNum,
                             const Attribute &attribute, const void *key, const RID &rid,
                             bool &isSplit, void *newChildKey, RID &newChildRid, PageNum &newChildNum)
{
    byte node[2 * PAGE_SIZE];
    ixfileHandle.readPage(nodeNum, node);
    unsigned freeBytes = getFreeSpace(node);
    if (isLeaf(node)) { // leaf node
        // entryLength = keyLength + ridLength
        unsigned entryLength = getKeyLength(attribute, key) + RID_SZ;

        // insert the new data entry into the current (leaf) node
        if (insertDataEntry(node, entryLength, attribute, key, rid) == FAIL) {
            return FAIL;
        }

        if (entryLength <= freeBytes) {
            setFreeSpace(node, freeBytes - entryLength);
            ixfileHandle.writePage(nodeNum, node);
            isSplit = false;
            return SUCCESS;
        } else {    // split the current leaf node
            unsigned offset = LEAF_HEADER_SZ;
            unsigned keyLength;
            while (offset < PAGE_SIZE - freeBytes + entryLength) {
                keyLength = getKeyLength(attribute, node + offset);
                if (offset - LEAF_HEADER_SZ + keyLength + RID_SZ > MAX_LEAF_SPACE / 2) {
                    break;
                }
                offset += keyLength + RID_SZ;
            }
            unsigned numOfMove = PAGE_SIZE - freeBytes + entryLength - offset;  // number of bytes moved to the new leaf node
            assert((numOfMove <= MAX_LEAF_SPACE) && "The new data entry is too large!");
            memcpy(newChildKey, node + offset, keyLength);
            loadRid(node, offset + keyLength, newChildRid);
            newChildNum = ixfileHandle.getNumberOfPages();
            memmove(node + PAGE_SIZE + LEAF_HEADER_SZ, node + offset, numOfMove);   // move entries to the new leaf page
            memset(node + PAGE_SIZE, 0, LEAF_HEADER_SZ);    // initialize leaf node header
            setLeaf(node + PAGE_SIZE);
            setFreeSpace(node + PAGE_SIZE, PAGE_SIZE - LEAF_HEADER_SZ - numOfMove);
            setFreeSpace(node, PAGE_SIZE - offset);

            // set previous and next page pointers
            if (hasNext(node)) {
                PageNum nextNum = getNextNum(node);
                setNextNum(node + PAGE_SIZE, nextNum);
                byte nextNode[PAGE_SIZE];
                ixfileHandle.readPage(nextNum, nextNode);
                setPrevNum(nextNode, newChildNum);
                ixfileHandle.writePage(nextNum, nextNode);
            }
            setPrevNum(node + PAGE_SIZE, nodeNum);
            setNextNum(node, newChildNum);

            ixfileHandle.appendPage(node + PAGE_SIZE);
            ixfileHandle.writePage(nodeNum, node);
            isSplit = true;
            return SUCCESS;
        }
    } else {    // non-leaf node
        unsigned childNumOffset = findChildNumOffset(node, attribute, key, rid);
        PageNum childNum = *((PageNum*) (node + childNumOffset));
        if (insertEntry(ixfileHandle, childNum,
                        attribute, key, rid,
                        isSplit, newChildKey, newChildRid, newChildNum) == FAIL) {
            return FAIL;
        }
        if (!isSplit) {
            return SUCCESS;
        }
        // entryLength = keyLength + ridLength + childPointerLength
        unsigned entryLength = getKeyLength(attribute, newChildKey) + RID_SZ + NODE_PTR_SZ;

        // insert the new index entry into the current (non-leaf) node
        unsigned keyOffset = childNumOffset + NODE_PTR_SZ;
        memmove(node + keyOffset + entryLength, node + keyOffset, PAGE_SIZE - keyOffset - freeBytes);
        memcpy(node + keyOffset, newChildKey, entryLength - NODE_PTR_SZ - RID_SZ);
        *((PageNum*) (node + keyOffset + entryLength - NODE_PTR_SZ - SLOT_NUM_SZ - PAGE_NUM_SZ)) = newChildRid.pageNum;
        *((uint16_t*) (node + keyOffset + entryLength - NODE_PTR_SZ - SLOT_NUM_SZ)) = newChildRid.slotNum;
        *((PageNum*) (node + keyOffset + entryLength - NODE_PTR_SZ)) = newChildNum;

        if (entryLength <= freeBytes) {
            setFreeSpace(node, freeBytes - entryLength);
            ixfileHandle.writePage(nodeNum, node);
            isSplit = false;
            return SUCCESS;
        } else {    // split the current non-leaf node
            unsigned offset = NONLEAF_HEADER_SZ + NODE_PTR_SZ;
            unsigned keyLength;
            while (offset < PAGE_SIZE - freeBytes + entryLength) {
                keyLength = getKeyLength(attribute, node + offset);
                if (offset + keyLength + RID_SZ + NODE_PTR_SZ - NONLEAF_HEADER_SZ > MAX_NONLEAF_SPACE / 2) {
                    break;
                }
                offset += keyLength + RID_SZ + NODE_PTR_SZ;
            }
            unsigned numOfMove = PAGE_SIZE - freeBytes + entryLength - (offset + keyLength + RID_SZ);  // number of bytes moved to the new non-leaf node
            assert((numOfMove <= MAX_NONLEAF_SPACE) && "The new index entry is too large!");
            memcpy(newChildKey, node + offset, keyLength);
            newChildRid.pageNum = *((PageNum*) (node + offset + keyLength));
            newChildRid.slotNum = *((uint16_t*) (node + offset + keyLength + PAGE_NUM_SZ));
            newChildNum = ixfileHandle.getNumberOfPages();
            setFreeSpace(node, PAGE_SIZE - offset);
            offset += keyLength + RID_SZ;
            memmove(node + PAGE_SIZE + NONLEAF_HEADER_SZ, node + offset, numOfMove);   // move entries to the new leaf page
            memset(node + PAGE_SIZE, 0, NONLEAF_HEADER_SZ);    // initialize non-leaf node header
            setFreeSpace(node + PAGE_SIZE, PAGE_SIZE - NONLEAF_HEADER_SZ - numOfMove);

            ixfileHandle.appendPage(node + PAGE_SIZE);
            ixfileHandle.writePage(nodeNum, node);
            isSplit = true;
            return SUCCESS;
        }
    }
}

RC IndexManager::insertDataEntry(byte *node, unsigned entryLength,
                                 const Attribute &attribute, const void *key, const RID &rid)
{
    // search for the insert position
    unsigned freeBytes = getFreeSpace(node);
    unsigned offset = LEAF_HEADER_SZ;
    while (offset < PAGE_SIZE - freeBytes) {
        const void *curKey = node + offset;
        unsigned keyLength = getKeyLength(attribute, curKey);
        RID curRid;
        loadRid(node, offset + keyLength, curRid);
        int cmp = compareKey(attribute, key, rid, curKey, curRid);
        if (cmp == 0) {
            // error: this entry (key, rid) has already existed!
            return FAIL;
        } else if (cmp < 0) {
            break;
        }
        offset += keyLength + RID_SZ;
    }

    // insert the new data entry
    memmove(node + offset + entryLength, node + offset, PAGE_SIZE - offset - freeBytes);
    memcpy(node + offset, key, entryLength - PAGE_NUM_SZ - SLOT_NUM_SZ);
    *((PageNum*) (node + offset + entryLength - PAGE_NUM_SZ - SLOT_NUM_SZ)) = rid.pageNum;
    *((uint16_t*) (node + offset + entryLength - SLOT_NUM_SZ)) = rid.slotNum;
    return SUCCESS;
}

unsigned IndexManager::findChildNumOffset(const byte *node, const Attribute &attribute,
                                          const void *key, const RID &rid) const
{
    unsigned freeBytes = getFreeSpace(node);
    unsigned offset = NONLEAF_HEADER_SZ + NODE_PTR_SZ;
    while (offset < PAGE_SIZE - freeBytes) {
        const void *curKey = node + offset;
        unsigned keyLength = getKeyLength(attribute, curKey);
        RID curRid;
        loadRid(node, offset + keyLength, curRid);
        if (compareKey(attribute, key, rid, curKey, curRid) < 0) {
            break;
        }
        offset += keyLength + RID_SZ + NODE_PTR_SZ;
    }
    return offset - NODE_PTR_SZ;
}

unsigned IndexManager::findChildNumOffset(const byte *node, const Attribute &attribute, const void *key) const
{
    RID dummyRid;
    dummyRid.pageNum = 0;
    dummyRid.slotNum = 0;
    return findChildNumOffset(node, attribute, key, dummyRid);
}

int IndexManager::compareKey(const Attribute &attribute,
                             const void *key1, const RID &rid1,
                             const void *key2, const RID &rid2) const
{
    switch (attribute.type) {
        case TypeInt: {
            int32_t i1 = *((const int32_t*) key1);
            int32_t i2 = *((const int32_t*) key2);
            if (i1 < i2) return -1;
            if (i1 > i2) return 1;
            break;
        }
        case TypeReal: {
            float r1 = *((const float *) key1);
            float r2 = *((const float *) key2);
            if (r1 < r2) return -1;
            if (r1 > r2) return 1;
            break;
        }
        case TypeVarChar: {
            uint32_t len1 = *((const uint32_t*) key1);
            uint32_t len2 = *((const uint32_t*) key2);
            string vc1((const char*) key1 + 4, len1);
            string vc2((const char*) key2 + 4, len2);
            if (vc1 < vc2) return -1;
            if (vc1 > vc2) return 1;
            break;
        }
    }
    return compare(rid1, rid2);
}

int IndexManager::compareKey(const Attribute &attribute, const void *key1, const void *key2) const
{
    RID dummyRid;
    dummyRid.pageNum = 0;
    dummyRid.slotNum = 0;
    return compareKey(attribute, key1, dummyRid, key2, dummyRid);
}

PageNum IndexManager::getRoot(IXFileHandle &ixfileHandle) const
{
    byte header[PAGE_SIZE];
    ixfileHandle.readHeaderPage(header);
    return *((PageNum*) (header + PAGE_SIZE - NODE_PTR_SZ));
}

RC IndexManager::setRoot(IXFileHandle &ixfileHandle, PageNum rootNum)
{
    byte header[PAGE_SIZE];
    ixfileHandle.readHeaderPage(header);
    *((PageNum*) (header + PAGE_SIZE - NODE_PTR_SZ)) = rootNum;
    return ixfileHandle.writeHeaderPage(header);
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return IX_EOF;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
}

