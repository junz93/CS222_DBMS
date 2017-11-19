#ifndef _ix_h_
#define _ix_h_

#include <cassert>
#include <string>
#include <vector>

#include "../rbf/rbfm.h"

#define IX_EOF (-1)  // end of the index scan
const unsigned NODE_PTR_SZ = sizeof(PageNum);
const unsigned LEAF_FLAG_SZ = 1;
const unsigned LEAF_HEADER_SZ = FREE_SPACE_SZ + LEAF_FLAG_SZ + 2 * NODE_PTR_SZ;
const unsigned NONLEAF_HEADER_SZ = FREE_SPACE_SZ + LEAF_FLAG_SZ;
const unsigned MAX_LEAF_SPACE = PAGE_SIZE - LEAF_HEADER_SZ;
const unsigned MAX_NONLEAF_SPACE = PAGE_SIZE - NONLEAF_HEADER_SZ;

class IX_ScanIterator;

class IXFileHandle;

class IndexManager {
    friend class IX_ScanIterator;

public:
    static IndexManager *instance();

    // Create an index file.
    RC createFile(const string &fileName);

    // Delete an index file.
    RC destroyFile(const string &fileName);

    // Open an index and return an ixfileHandle.
    RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

    // Close an ixfileHandle for an index.
    RC closeFile(IXFileHandle &ixfileHandle);

    // Insert an entry into the given index that is indicated by the given ixfileHandle.
    RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Delete an entry from the given index that is indicated by the given ixfileHandle.
    RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Initialize and IX_ScanIterator to support a range search
    RC scan(IXFileHandle &ixfileHandle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

protected:
    IndexManager();

    ~IndexManager();

private:
    static IndexManager *_index_manager;
    PagedFileManager *pfm = PagedFileManager::instance();

    RC initializeScanIterator(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *lowKey,
                              const void *highKey, bool lowKeyInclusive, bool highKeyInclusive, PageNum nodeNum,
                              IX_ScanIterator &ix_ScanIterator);

    unsigned findFirstQualifiedEntry(const byte *node, const Attribute &attribute, const void *lowKey,
                                     const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
                                     bool &isQualifiedEntryExist);

    RC insertEntry(IXFileHandle &ixfileHandle, PageNum nodeNum,
                   const Attribute &attribute, const void *key, const RID &rid,
                   bool &isSplit, void *newChildKey, RID &newChildRid, PageNum &newChildNum);

    RC insertDataEntry(byte *node, unsigned entryLength, const Attribute &attribute, const void *key, const RID &rid);

    // return the offset of the child pointer in a non-leaf node for the given composite key
    unsigned findChildNumOffset(const byte *node, const Attribute &attribute, const void *key, const RID &rid) const;

    // return the offset of the child pointer in a non-leaf node for the given key
    unsigned
    findChildNumOffset(const byte *node, const Attribute &attribute, const void *key, bool lowKeyInclusive) const;

    void printBtree(IXFileHandle &ixfileHandle, PageNum nodeNum, const Attribute &attribute, unsigned level) const;

    // print the key and return its length (including the length part of varchar)
    unsigned printKey(const Attribute &attribute, const void *key) const;

    // compare two composite keys (key, rid)
    int compareKey(const Attribute &attribute,
                   const void *key1, const RID &rid1,
                   const void *key2, const RID &rid2) const;

    // compare two keys
    int compareKey(const Attribute &attribute, const void *key1, const void *key2) const;

    // return the length of the key (including the length part of varchar)
    unsigned getKeyLength(Attribute attribute, const void *key) const;

    // read RID from the given node and store it in rid argument
    void loadRid(const byte *node, unsigned offset, RID &rid) const;

    // write rid to the given position of the given node
    void writeRid(byte *node, unsigned offset, const RID &rid);

    PageNum getRoot(IXFileHandle &ixfileHandle) const;

    RC setRoot(IXFileHandle &ixfileHandle, PageNum rootNum);

    unsigned getFreeSpace(const byte *node) const;

    void setFreeSpace(byte *node, unsigned freeBytes);

    bool isLeaf(const byte *node) const;

    void setLeaf(byte *node);

    bool hasPrev(const byte *node) const;

    PageNum getPrevNum(const byte *node) const;

    void setPrevNum(byte *node, PageNum prevNum);

    bool hasNext(const byte *node) const;

    PageNum getNextNum(const byte *node) const;

    void setNextNum(byte *node, PageNum nextNum);
};

inline
unsigned IndexManager::getKeyLength(Attribute attribute, const void *key) const {
    switch (attribute.type) {
        case TypeInt:
        case TypeReal:
            return attribute.length;
        case TypeVarChar:
            return *((const uint32_t *) key) + 4;
    }
}

inline
void IndexManager::loadRid(const byte *node, unsigned offset, RID &rid) const {
    rid.pageNum = *((PageNum *) (node + offset));
    rid.slotNum = *((SlotNum *) (node + offset + PAGE_NUM_SZ));
}

inline
void IndexManager::writeRid(byte *node, unsigned offset, const RID &rid) {
    *((PageNum *) (node + offset)) = rid.pageNum;
    *((SlotNum *) (node + offset + PAGE_NUM_SZ)) = rid.slotNum;
}

inline
unsigned IndexManager::getFreeSpace(const byte *node) const {
    return *((uint16_t *) node);
}

inline
void IndexManager::setFreeSpace(byte *node, unsigned freeBytes) {
    *((uint16_t *) node) = freeBytes;
}

inline
bool IndexManager::isLeaf(const byte *node) const {
    return (*((uint8_t *) (node + FREE_SPACE_SZ)) & 0x1) != 0;
}

inline
void IndexManager::setLeaf(byte *node) {
    *((uint8_t *) (node + FREE_SPACE_SZ)) |= 0x1;
}

inline
bool IndexManager::hasPrev(const byte *node) const {
    assert(isLeaf(node) && "This node is not a leaf node");
    return (*((uint8_t *) (node + FREE_SPACE_SZ)) & 0x4) != 0;
}

inline
PageNum IndexManager::getPrevNum(const byte *node) const {
    assert(hasPrev(node) && "This node does not have previous sibling!");
    return *((PageNum *) (node + FREE_SPACE_SZ + LEAF_FLAG_SZ));
}

inline
void IndexManager::setPrevNum(byte *node, PageNum prevNum) {
    *((uint8_t *) (node + FREE_SPACE_SZ)) |= 0x4;
    *((PageNum *) (node + FREE_SPACE_SZ + LEAF_FLAG_SZ)) = prevNum;
}

inline
bool IndexManager::hasNext(const byte *node) const {
    assert(isLeaf(node) && "This node is not a leaf node");
    return (*((uint8_t *) (node + FREE_SPACE_SZ)) & 0x2) != 0;
}

inline
PageNum IndexManager::getNextNum(const byte *node) const {
    assert(hasNext(node) && "This node does not have next sibling!");
    return *((PageNum *) (node + FREE_SPACE_SZ + LEAF_FLAG_SZ + NODE_PTR_SZ));
}

inline
void IndexManager::setNextNum(byte *node, PageNum nextNum) {
    *((uint8_t *) (node + FREE_SPACE_SZ)) |= 0x2;
    *((PageNum *) (node + FREE_SPACE_SZ + LEAF_FLAG_SZ + NODE_PTR_SZ)) = nextNum;
}

class IX_ScanIterator {
    friend class IndexManager;
public:

    // Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Terminate index scan
    RC close();

private:
    IndexManager *indexManager = IndexManager::instance();
    IXFileHandle *ixFileHandle = nullptr;
    byte node[PAGE_SIZE];
    unsigned offset;
    const void *highKey;
    bool highKeyInclusive;
    Attribute attribute;
};


class IXFileHandle {
    friend class IndexManager;

public:
    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    RC readPage(PageNum pageNum, void *data) {
        return fileHandle.readPage(pageNum, data);
    }

    RC writePage(PageNum pageNum, const void *data) {
        return fileHandle.writePage(pageNum, data);
    }

    RC appendPage(const void *data) {
        return fileHandle.appendPage(data);
    }

    unsigned getNumberOfPages() {
        return fileHandle.getNumberOfPages();
    }

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    RC readHeaderPage(void *data) {
        return fileHandle.readHeaderPage(data);
    }

    RC writeHeaderPage(const void *data) {
        return fileHandle.writeHeaderPage(data);
    }

private:
    FileHandle fileHandle;
};

#endif
