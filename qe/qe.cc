#include <cassert>
#include <cstring>
#include <unordered_map>
#include "qe.h"

using namespace std;

/**  Filter Related Functions  **/

Filter::Filter(Iterator *input, const Condition &condition) : iter(input), condition(condition) {
    assert(!condition.bRhsIsAttr);
    // Get Attributes from input iterator
    input->getAttributes(attrs);
    for (attrNo = 0; attrNo < attrs.size(); ++attrNo) {
        if (attrs[attrNo].name == condition.lhsAttr) {
            break;
        }
    }
    assert(attrNo != attrs.size());
    assert(attrs[attrNo].type == condition.rhsValue.type);
}

RC Filter::getNextTuple(void *data) {
    Value lhsValue;

    if (condition.bRhsIsAttr) {
        return QE_EOF;
    }

    do {
        if (iter->getNextTuple(data) == QE_EOF) {
            return QE_EOF;
        }
        if (getLhsValue(attrs, condition.lhsAttr, data, lhsValue) == FAIL) {
            return FAIL;
        }
    } while (!isQualifiedTuple(lhsValue, condition.op, condition.rhsValue));

    return SUCCESS;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
    attrs = this->attrs;
}

RC Filter::getLhsValue(const vector<Attribute> attrs, const string attrName, const void *data, Value &value) {
    int offset = getBytesOfNullIndicator(attrs.size());
    for (Attribute attr : attrs) {
        if (attr.name == attrName) {
            value.type = attr.type;
            value.data = (char *) data + offset;
            return SUCCESS;
        } else {
            switch (attr.type) {
                case TypeInt:
                case TypeReal:
                    offset += 4;
                    break;
                case TypeVarChar:
                    uint32_t length = *((uint32_t *) ((char *) data + offset));
                    offset += (4 + length);
                    break;
            }
        }
    }
    return FAIL;
}

bool Filter::isQualifiedTuple(const Value lhsValue, const CompOp op, const Value rhsValue) {
    if (lhsValue.type != rhsValue.type) {
        return false;
    }
    switch (lhsValue.type) {
        case TypeInt: {
            int lhsInt = *((int *) lhsValue.data);
            int rhsInt = *((int *) rhsValue.data);
            return compare(op, lhsInt, rhsInt);
        }
        case TypeReal: {
            float lhsReal = *((float *) lhsValue.data);
            float rhsReal = *((float *) rhsValue.data);
            return compare(op, lhsReal, rhsReal);
        }
        case TypeVarChar: {
            int lhsLength = *((int *) lhsValue.data);
            int rhsLength = *((int *) rhsValue.data);
            string lhsString((byte *) lhsValue.data + 4, lhsLength);
            string rhsString((byte *) rhsValue.data + 4, rhsLength);
            return compare(op, lhsString, rhsString);
        }
    }
}

/**  Project Related Functions  **/

RC Project::getNextTuple(void *data) {
    void *originalData = malloc(PAGE_SIZE);
    void *attributeData = malloc(PAGE_SIZE);
    if (iter->getNextTuple(originalData) == QE_EOF) { return FAIL; }

    prepareNullsIndicator(data);
    int offset = getBytesOfNullIndicator(attrs.size());
    for (int i = 0; i < attrs.size(); i++) {
        Attribute targetAttr = attrs[i];
        if (getAttributeData(originalData, originalAttrs, targetAttr, attributeData) == false) { // attribute is null
            int nullOffset = i / 8;
            uint8_t flag = 0x80 >> (i % 8);
            *((uint8_t *) data + nullOffset) = *((uint8_t *) data + nullOffset) | flag;
        } else {
            switch (targetAttr.type) {
                case TypeInt:
                case TypeReal:
                    memcpy((byte *) data + offset, attributeData, 4);
                    offset += 4;
                    break;
                case TypeVarChar:
                    int lenVarChar = *((int *) attributeData);
                    memcpy((byte *) data + offset, attributeData, 4 + lenVarChar);
                    offset += (4 + lenVarChar);
                    break;
            }
        }
    }

    free(originalData);
    free(attributeData);
    return SUCCESS;
};

void Project::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    attrs = this->attrs;
};

void Project::prepareNameAttributeMap(const vector<Attribute> attrs) {
    for (Attribute attr : attrs) {
        nameAttributeMap[attr.name] = attr;
    }
};

void Project::prepareAttrs(const vector<string> attrNames) {
    attrs.clear();
    for (string attrName : attrNames) {
        auto it = nameAttributeMap.find(attrName);
        if (it == nameAttributeMap.end()) {
            cerr << "Invalid input attrNames！" << endl;
        } else {
            attrs.push_back(nameAttributeMap[attrName]);
        }
    }
}

void Project::prepareNullsIndicator(void *data) {
    int length = getBytesOfNullIndicator(attrs.size());
    memset(data, 0, length);
}

bool Project::getAttributeData(const void *data, const vector<Attribute> attrs, const Attribute targetAttr,
                               void *attributeData) {
    int offset = getBytesOfNullIndicator(attrs.size());
    for (int i = 0; i < attrs.size(); i++) {
        Attribute currentAttribute = attrs[i];
        if (currentAttribute.name == targetAttr.name) {
            if (isAttributeNull(i, data)) {
                return false;
            }
            switch (currentAttribute.type) {
                case TypeInt:
                case TypeReal:
                    memcpy(attributeData, (char *) data + offset, 4);
                    break;
                case TypeVarChar:
                    uint32_t length = *((uint32_t *) ((char *) data + offset));
                    memcpy(attributeData, (char *) data + offset, 4 + length);
                    break;
            }
            return true;
        } else {
            switch (currentAttribute.type) {
                case TypeInt:
                case TypeReal:
                    offset += 4;
                    break;
                case TypeVarChar:
                    uint32_t length = *((uint32_t *) ((char *) data + offset));
                    offset += (4 + length);
                    break;
            }
        }
    }
    cerr << "Didn't find corresponding attribute, which should be impossible!" << endl;
    return false;
}

/**  Join Related Functions  **/

BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages)
        : leftIn(leftIn), rightIn(rightIn), condition(condition), numOfBufferPages(numPages) {
    assert(condition.op == EQ_OP);  // should be equijoin
    leftBuffer = new byte[numPages * PAGE_SIZE];
    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);
//    rightIn->setIterator();
    attrs = leftAttrs;
    attrs.insert(attrs.end(), rightAttrs.begin(), rightAttrs.end());

    for (leftAttrNo = 0; leftAttrNo < leftAttrs.size(); ++leftAttrNo) {
        if (leftAttrs[leftAttrNo].name == condition.lhsAttr) {
            attrType = leftAttrs[leftAttrNo].type;
            break;
        }
    }
    assert(leftAttrNo != leftAttrs.size());     // left attribute should exist in left relation

    for (rightAttrNo = 0; rightAttrNo < rightAttrs.size(); ++rightAttrNo) {
        if (rightAttrs[rightAttrNo].name == condition.rhsAttr) {
            assert(rightAttrs[rightAttrNo].type == attrType);   // condition attributes should have the same type
            break;
        }
    }
    assert(rightAttrNo != rightAttrs.size());   // right attribute should exist in right relation

    switch (attrType) {
        case TypeInt:
            hashTable = new unordered_map<int32_t, vector<unsigned>>();
            break;
        case TypeReal:
            hashTable = new unordered_map<float, vector<unsigned>>();
            break;
        case TypeVarChar:
            hashTable = new unordered_map<string, vector<unsigned>>();
            break;
    }
}

BNLJoin::~BNLJoin() {
    delete[] leftBuffer;
    switch (attrType) {
        case TypeInt:
            delete (unordered_map<int32_t, vector<unsigned>> *) hashTable;
            break;
        case TypeReal:
            delete (unordered_map<float, vector<unsigned>> *) hashTable;
            break;
        case TypeVarChar:
            delete (unordered_map<string, vector<unsigned>> *) hashTable;
            break;
    }
}

RC BNLJoin::getNextTuple(void *data) {
    while (true) {
        if (leftBufferSize == 0) {
            while (true) {  // read the next block of tuples from left relation to leftBuffer
                unsigned leftTupleLength;
                if (lastLeftTupleLength != 0) {
                    memcpy(leftBuffer, leftTuple, lastLeftTupleLength);
                    leftTupleLength = lastLeftTupleLength;
                    lastLeftTupleLength = 0;
                } else {
                    RC rcLeft = leftIn->getNextTuple(leftTuple);
                    if (rcLeft == QE_EOF) {
                        break;
                    }
                    leftTupleLength = computeTupleLength(leftAttrs, leftTuple);
                    if (leftBufferSize + leftTupleLength > numOfBufferPages * PAGE_SIZE) {
                        lastLeftTupleLength = leftTupleLength;
                        break;
                    }
                    memcpy(leftBuffer + leftBufferSize, leftTuple, leftTupleLength);
                }

                unsigned attrOffset = getAttributeOffset(leftAttrs, leftTuple, leftAttrNo);
                insertToHashTable(hashTable, attrType, leftTuple + attrOffset, leftBufferSize);
                leftBufferSize += leftTupleLength;
            }
            if (leftBufferSize == 0) {
                break;  // return QE_EOF;
            }
            rightIn->setIterator();
        }

        while (true) {  // scan right relation
            if (leftIdx == leftOffsets.size()) {
                RC rcRight = rightIn->getNextTuple(rightTuple);
                if (rcRight == QE_EOF) {
                    break;
                }
                unsigned attrOffset = getAttributeOffset(rightAttrs, rightTuple, rightAttrNo);
                leftOffsets = getOffsetsFromHashTable(hashTable, attrType, rightTuple + attrOffset);
                leftIdx = 0;
            }
            if (leftIdx != leftOffsets.size()) {
                joinTuples(leftAttrs, leftBuffer + leftOffsets[leftIdx], rightAttrs, rightTuple, data);
                ++leftIdx;
                return SUCCESS;
            }
        }
        leftBufferSize = 0;
        clearHashTable(hashTable, attrType);
    }

    return QE_EOF;
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const {
    attrs = this->attrs;
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
    assert(condition.op == EQ_OP);  // should be equijoin
    this->leftIn = leftIn;
    this->rightIn = rightIn;
    this->condition = condition;

    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);
    attrs = leftAttrs;
    attrs.insert(attrs.end(), rightAttrs.begin(), rightAttrs.end());

    for (leftAttrNo = 0; leftAttrNo < leftAttrs.size(); ++leftAttrNo) {
        if (leftAttrs[leftAttrNo].name == condition.lhsAttr) {
            attrType = leftAttrs[leftAttrNo].type;
            break;
        }
    }
    assert(leftAttrNo != leftAttrs.size());     // left attribute should exist in left relation

    for (rightAttrNo = 0; rightAttrNo < rightAttrs.size(); ++rightAttrNo) {
        if (rightAttrs[rightAttrNo].name == condition.rhsAttr) {
            assert(rightAttrs[rightAttrNo].type == attrType);   // condition attributes should have the same type
            break;
        }
    }
    assert(rightAttrNo != rightAttrs.size());   // right attribute should exist in right relation
}

INLJoin::~INLJoin() {

}

RC INLJoin::getNextTuple(void *data) {
    while (true) {
        if (isLeftTupleEmpty) {
            RC rcLeft = leftIn->getNextTuple(leftTuple);
            if (rcLeft == QE_EOF) {
                break;  // return QE_EOF;
            }
            isLeftTupleEmpty = false;
            unsigned attrOffset = getAttributeOffset(leftAttrs, leftTuple, leftAttrNo);
            rightIn->setIterator(leftTuple + attrOffset, leftTuple + attrOffset, true, true);
        }

        byte rightTuple[PAGE_SIZE];
        RC rcRight = rightIn->getNextTuple(rightTuple);
        if (rcRight != QE_EOF) {
            joinTuples(leftAttrs, leftTuple, rightAttrs, rightTuple, data);
            return SUCCESS;
        }
        isLeftTupleEmpty = true;
    }

    return QE_EOF;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
    attrs = this->attrs;
}

GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned numPartitions)
        : leftIn(leftIn), rightIn(rightIn), condition(condition), numOfPartitions(numPartitions) {
    assert(condition.op == EQ_OP);  // should be equijoin
    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);
    attrs = leftAttrs;
    attrs.insert(attrs.end(), rightAttrs.begin(), rightAttrs.end());

    for (leftAttrNo = 0; leftAttrNo < leftAttrs.size(); ++leftAttrNo) {
        if (leftAttrs[leftAttrNo].name == condition.lhsAttr) {
            attrType = leftAttrs[leftAttrNo].type;
            break;
        }
    }
    assert(leftAttrNo != leftAttrs.size());     // left attribute should exist in left relation

    for (rightAttrNo = 0; rightAttrNo < rightAttrs.size(); ++rightAttrNo) {
        if (rightAttrs[rightAttrNo].name == condition.rhsAttr) {
            assert(rightAttrs[rightAttrNo].type == attrType);   // condition attributes should have the same type
            break;
        }
    }
    assert(rightAttrNo != rightAttrs.size());   // right attribute should exist in right relation

    for (const Attribute &attr : leftAttrs) {
        leftAttrNames.push_back(attr.name);
    }
    for (const Attribute &attr : rightAttrs) {
        rightAttrNames.push_back(attr.name);
    }

    leftRelName = leftAttrs[0].name.substr(0, leftAttrs[0].name.find_first_of('.'));
    rightRelName = rightAttrs[0].name.substr(0, rightAttrs[0].name.find_first_of('.'));
    FileHandle *leftFileHandles = new FileHandle[numOfPartitions];
    FileHandle *rightFileHandles = new FileHandle[numOfPartitions];
    for (unsigned i = 0; i < numOfPartitions; ++i) {
        string suffix = to_string(i);
        rbfm->createFile("left_join_" + leftRelName + suffix);
        rbfm->openFile("left_join_" + leftRelName + suffix, leftFileHandles[i]);
        rbfm->createFile("right_join_" + rightRelName + suffix);
        rbfm->openFile("right_join_" + rightRelName + suffix, rightFileHandles[i]);
    }
    byte tuple[PAGE_SIZE];
    while (true) {
        RC rcLeft = leftIn->getNextTuple(tuple);
        if (rcLeft == QE_EOF) {
            break;
        }
        unsigned attrOffset = getAttributeOffset(leftAttrs, tuple, leftAttrNo);
        unsigned partition = getPartitionNum(attrType, tuple + attrOffset, numOfPartitions);
        RID rid;
        rbfm->insertRecord(leftFileHandles[partition], leftAttrs, tuple, rid);
    }
    while (true) {
        RC rcRight = rightIn->getNextTuple(tuple);
        if (rcRight == QE_EOF) {
            break;
        }
        unsigned attrOffset = getAttributeOffset(rightAttrs, tuple, rightAttrNo);
        unsigned partition = getPartitionNum(attrType, tuple + attrOffset, numOfPartitions);
        RID rid;
        rbfm->insertRecord(rightFileHandles[partition], rightAttrs, tuple, rid);
    }

    switch (attrType) {
        case TypeInt:
            hashTable = new unordered_map<int32_t, vector<unsigned>>();
            break;
        case TypeReal:
            hashTable = new unordered_map<float, vector<unsigned>>();
            break;
        case TypeVarChar:
            hashTable = new unordered_map<string, vector<unsigned>>();
            break;
    }

    delete[] leftFileHandles;
    delete[] rightFileHandles;
}

GHJoin::~GHJoin() {
    for (unsigned i = 0; i < numOfPartitions; ++i) {
        string suffix = to_string(i);
        rbfm->destroyFile("left_join_" + leftRelName + suffix);
        rbfm->destroyFile("right_join_" + rightRelName + suffix);
    }
    switch (attrType) {
        case TypeInt:
            delete (unordered_map<int32_t, vector<unsigned>> *) hashTable;
            break;
        case TypeReal:
            delete (unordered_map<float, vector<unsigned>> *) hashTable;
            break;
        case TypeVarChar:
            delete (unordered_map<string, vector<unsigned>> *) hashTable;
            break;
    }
}

RC GHJoin::getNextTuple(void *data) {
    for (; curPartitionNum < numOfPartitions; ++curPartitionNum) {
        if (leftBufferSize == 0) {
            FileHandle leftFileHandle;
            rbfm->openFile("left_join_" + leftRelName + to_string(curPartitionNum), leftFileHandle);
            rbfm->scan(leftFileHandle, leftAttrs, "", NO_OP, nullptr, leftAttrNames, leftIterator);
            unsigned numOfLeftPages = leftFileHandle.getNumberOfPages();
            if (numOfLeftPages == 0) {
                continue;
            }
            leftBuffer = new byte[numOfLeftPages * PAGE_SIZE];
            RID rid;
            while (leftIterator.getNextRecord(rid, leftBuffer + leftBufferSize) != RBFM_EOF) {
                unsigned leftTupleLength = computeTupleLength(leftAttrs, leftBuffer + leftBufferSize);
                unsigned attrOffset = getAttributeOffset(leftAttrs, leftBuffer + leftBufferSize, leftAttrNo);
                insertToHashTable(hashTable, attrType, leftBuffer + leftBufferSize + attrOffset, leftBufferSize);
                leftBufferSize += leftTupleLength;
            }
            leftIterator.close();   // the leftIterator will never be used again

            if (leftBufferSize == 0) {
                delete[] leftBuffer;
                continue;
            }
            FileHandle rightFileHandle;
            rbfm->openFile("right_join_" + rightRelName + to_string(curPartitionNum), rightFileHandle);
            rbfm->scan(rightFileHandle, rightAttrs, "", NO_OP, nullptr, rightAttrNames, rightIterator);
        }
        RID rid;
        while (true) {
            if (leftIdx == leftOffsets.size()) {
                RC rcRight = rightIterator.getNextRecord(rid, rightTuple);
                if (rcRight == RBFM_EOF) {
                    break;
                }
                unsigned attrOffset = getAttributeOffset(rightAttrs, rightTuple, rightAttrNo);
                leftOffsets = getOffsetsFromHashTable(hashTable, attrType, rightTuple + attrOffset);
                leftIdx = 0;
            }
            if (leftIdx != leftOffsets.size()) {
                joinTuples(leftAttrs, leftBuffer + leftOffsets[leftIdx], rightAttrs, rightTuple, data);
                ++leftIdx;
                return SUCCESS;
            }
        }

        rightIterator.close();  // the rightIterator will never be used again
        delete[] leftBuffer;
        leftBufferSize = 0;
        clearHashTable(hashTable, attrType);
    }

    return QE_EOF;
}

void GHJoin::getAttributes(vector<Attribute> &attrs) const {
    attrs = this->attrs;
}

unsigned computeTupleLength(const vector<Attribute> &attrs, const void *tuple) {
    auto numOfFields = attrs.size();
    unsigned tupleLength = getBytesOfNullIndicator(numOfFields);
    const byte *pFlag = (const byte *) tuple;         // pointer to null flags
    const byte *pData = pFlag + getBytesOfNullIndicator(numOfFields);  // pointer to actual field data
    uint8_t flagMask = 0x80;     // cannot use (signed) byte

    for (const Attribute &attr : attrs) {
        if (!(*pFlag & flagMask)) {
            switch (attr.type) {
                case TypeInt:
                case TypeReal:
                    tupleLength += attr.length;
                    pData += attr.length;
                    break;
                case TypeVarChar:
                    uint32_t length = *((const uint32_t *) pData);
                    tupleLength += 4 + length;
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

    return tupleLength;
}

unsigned getAttributeOffset(const vector<Attribute> &attrs, const byte *tuple, unsigned attrNo) {
    const byte *pFlag = tuple;
    unsigned attrOffset = getBytesOfNullIndicator(attrs.size());
    uint8_t flagMask = 0x80;
    for (unsigned i = 0; i < attrNo; ++i) {
        if (!(*pFlag & flagMask)) {
            switch (attrs[i].type) {
                case TypeInt:
                case TypeReal:
                    attrOffset += attrs[i].length;
                    break;
                case TypeVarChar:
                    uint32_t length = *((const uint32_t *) (tuple + attrOffset));
                    attrOffset += 4 + length;
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
    return attrOffset;
}

void clearHashTable(void *hashTable, AttrType type) {
    switch (type) {
        case TypeInt: {
            auto &intHashTable = *(unordered_map<int32_t, vector<unsigned>> *) hashTable;
            intHashTable.clear();
            break;
        }
        case TypeReal: {
            auto &floatHashTable = *(unordered_map<float, vector<unsigned>> *) hashTable;
            floatHashTable.clear();
            break;
        }
        case TypeVarChar: {
            auto &strHashTable = *(unordered_map<string, vector<unsigned>> *) hashTable;
            strHashTable.clear();
            break;
        }
    }
}

void insertToHashTable(void *hashTable, AttrType type, const byte *key, unsigned value) {
    switch (type) {
        case TypeInt: {
            auto &intHashTable = *(unordered_map<int32_t, vector<unsigned>> *) hashTable;
            intHashTable[*(const int32_t *) key].push_back(value);
            break;
        }
        case TypeReal: {
            auto &floatHashTable = *(unordered_map<float, vector<unsigned>> *) hashTable;
            floatHashTable[*(const float *) key].push_back(value);
            break;
        }
        case TypeVarChar: {
            auto &strHashTable = *(unordered_map<string, vector<unsigned>> *) hashTable;
            uint32_t length = *((const uint32_t *) key);
            strHashTable[string(key + 4, length)].push_back(value);
            break;
        }
    }
}

vector<unsigned> getOffsetsFromHashTable(void *hashTable, AttrType type, const byte *key) {
    switch (type) {
        case TypeInt: {
            auto &intHashTable = *(unordered_map<int32_t, vector<unsigned>> *) hashTable;
            return intHashTable[*(const int32_t *) key];
        }
        case TypeReal: {
            auto &floatHashTable = *(unordered_map<float, vector<unsigned>> *) hashTable;
            return floatHashTable[*(const float *) key];
        }
        case TypeVarChar: {
            auto &strHashTable = *(unordered_map<string, vector<unsigned>> *) hashTable;
            uint32_t length = *((const uint32_t *) key);
            return strHashTable[string(key + 4, length)];
        }
    }
}

void joinTuples(const vector<Attribute> &leftAttrs, const byte *leftTuple,
                const vector<Attribute> &rightAttrs, const byte *rightTuple,
                void *data) {
    unsigned bytesOfNullFlagsLeft = getBytesOfNullIndicator(leftAttrs.size());
    unsigned bytesOfNullFlagsRight = getBytesOfNullIndicator(rightAttrs.size());
    unsigned bytesOfNullFlags = getBytesOfNullIndicator(leftAttrs.size() + rightAttrs.size());
    memset(data, 0, bytesOfNullFlags);
    memcpy(data, leftTuple, bytesOfNullFlagsLeft);
    byte *pFlag = (byte*) data + leftAttrs.size() / 8;
    byte *pData = (byte*) data + bytesOfNullFlags;
    uint8_t flagMask = 0x80 >> (leftAttrs.size() % 8);
    const byte *pFlagRight = rightTuple;
    uint8_t flagMaskRight = 0x80;
    for (unsigned i = 0; i < rightAttrs.size(); ++i) {
        if (*pFlagRight & flagMaskRight) {
            *pFlag = *pFlag | flagMask;
        }

        if (flagMask == 0x01) {
            flagMask = 0x80;
            ++pFlag;
        } else {
            flagMask = flagMask >> 1;
        }
        if (flagMaskRight == 0x01) {
            flagMaskRight = 0x80;
            ++pFlagRight;
        } else {
            flagMaskRight = flagMaskRight >> 1;
        }
    }
    unsigned leftTupleLength = computeTupleLength(leftAttrs, leftTuple);
    unsigned rightTupleLength = computeTupleLength(rightAttrs, rightTuple);
    memcpy(pData,
           leftTuple + bytesOfNullFlagsLeft,
           leftTupleLength - bytesOfNullFlagsLeft);
    memcpy(pData + leftTupleLength - bytesOfNullFlagsLeft,
           rightTuple + bytesOfNullFlagsRight,
           rightTupleLength - bytesOfNullFlagsRight);
}

unsigned getPartitionNum(AttrType type, const byte *key, unsigned numOfPartitions) {
    switch (type) {
        case TypeInt: {
            hash<int32_t> intHash;
            return intHash(*(const int32_t *) (key)) % numOfPartitions;
        }
        case TypeReal: {
            hash<float> floatHash;
            return floatHash(*(const float *) (key)) % numOfPartitions;
        }
        case TypeVarChar: {
            hash<string> strHash;
            uint32_t length = *((const uint32_t *) (key));
            return strHash(string(key + 4, length)) % numOfPartitions;
        }
    }
}

