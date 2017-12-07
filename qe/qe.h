#ifndef _qe_h_
#define _qe_h_

#include <limits>
#include <vector>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum {
    MIN = 0, MAX, COUNT, SUM, AVG
} AggregateOp;

// The following functions use the following
// format for the pasAgsed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void *data;         // value
};


struct Condition {
    string lhsAttr;        // left-hand side attribute
    CompOp op;             // comparison operator
    bool bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};

struct AggregateInfo {
    float sum = 0, count = 0, min = numeric_limits<float>::max(), max = numeric_limits<float>::min();

    void update(float inputValue) {
        sum += inputValue;
        count += 1;
        min = inputValue < min ? inputValue : min;
        max = inputValue > max ? inputValue : max;
    }

    float getAggreteValue(AggregateOp aggOp) {
        switch (aggOp) {
            case MIN:
                return min;
            case MAX:
                return max;
            case COUNT:
                return count;
            case SUM:
                return sum;
            case AVG:
                return sum / count;
        }
    }
};

class Iterator {
    // All the relational operators and access methods are iterators.
public:
    virtual RC getNextTuple(void *data) = 0;

    virtual void getAttributes(vector<Attribute> &attrs) const = 0;

    virtual ~Iterator() {};
};


class TableScan : public Iterator {
    // A wrapper inheriting Iterator over RM_ScanIterator
public:
    RelationManager &rm;
    RM_ScanIterator *iter;
    string tableName;
    vector<Attribute> attrs;
    vector<string> attrNames;
    RID rid;

    TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL) : rm(rm) {
        //Set members
        this->tableName = tableName;

        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Get Attribute Names from RM
        unsigned i;
        for (i = 0; i < attrs.size(); ++i) {
            // convert to char *
            attrNames.push_back(attrs.at(i).name);
        }

        // Call RM scan to get an iterator
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

        // Set alias
        if (alias) this->tableName = alias;
    };

    // Start a new iterator given the new compOp and value
    void setIterator() {
        iter->close();
        delete iter;
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
    };

    RC getNextTuple(void *data) {
        return iter->getNextTuple(rid, data);
    };

    void getAttributes(vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = this->attrs;
        unsigned i;

        // For attribute in vector<Attribute>, name it as rel.attr
        for (i = 0; i < attrs.size(); ++i) {
            string tmp = tableName;
            tmp += ".";
            tmp += attrs.at(i).name;
            attrs.at(i).name = tmp;
        }
    };

    ~TableScan() {
        iter->close();
    };
};


class IndexScan : public Iterator {
    // A wrapper inheriting Iterator over IX_IndexScan
public:
    RelationManager &rm;
    RM_IndexScanIterator *iter;
    string tableName;
    string attrName;
    vector<Attribute> attrs;
    char key[PAGE_SIZE];
    RID rid;

    IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL) : rm(rm) {
        // Set members
        this->tableName = tableName;
        this->attrName = attrName;


        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Call rm indexScan to get iterator
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

        // Set alias
        if (alias) this->tableName = alias;
    };

    // Start a new iterator given the new key range
    void setIterator(void *lowKey,
                     void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive) {
        iter->close();
        delete iter;
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                     highKeyInclusive, *iter);
    };

    RC getNextTuple(void *data) {
        int rc = iter->getNextEntry(rid, key);
        if (rc == 0) {
            rc = rm.readTuple(tableName.c_str(), rid, data);
        }
        return rc;
    };

    void getAttributes(vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = this->attrs;
        unsigned i;

        // For attribute in vector<Attribute>, name it as rel.attr
        for (i = 0; i < attrs.size(); ++i) {
            string tmp = tableName;
            tmp += ".";
            tmp += attrs.at(i).name;
            attrs.at(i).name = tmp;
        }
    };

    ~IndexScan() {
        iter->close();
    };
};


class Filter : public Iterator {
    // Filter operator
public:
    Filter(Iterator *input, const Condition &condition);

    ~Filter() {}

    RC getNextTuple(void *data);

    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;

private:
    Iterator *iter;
    const Condition condition;
    vector<Attribute> attrs;
    unsigned attrNo;

    RC getLhsValue(const vector<Attribute> attrs, const string attrName, const void *data, Value &value);

    bool isQualifiedTuple(const Value lhsValue, const CompOp op, const Value rhsValue);
};


class Project : public Iterator {
    // Projection operator
public:
    Project(Iterator *input, const vector<string> &attrNames);

    ~Project() {};

    RC getNextTuple(void *data);

    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;

private:
    Iterator *iter;
    vector<Attribute> attrs;
    vector<Attribute> originalAttrs;
    unordered_map<string, Attribute> nameAttributeMap;

    void prepareNameAttributeMap(const vector<Attribute> attrs);

    void prepareAttrs(const vector<string> attrNames);

    void prepareNullsIndicator(void *data);
};

class BNLJoin : public Iterator {
// Block nested-loop join operator
public:
    BNLJoin(Iterator *leftIn,            // Iterator of input R
            TableScan *rightIn,           // TableScan Iterator of input S
            const Condition &condition,   // Join condition
            const unsigned numPages       // # of pages that can be loaded into memory,
            //   i.e., memory block size (decided by the optimizer)
    );

    ~BNLJoin();

    RC getNextTuple(void *data);

    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;

private:
    Iterator *leftIn;
    TableScan *rightIn;
    Condition condition;
    vector<Attribute> leftAttrs;
    vector<Attribute> rightAttrs;
    vector<Attribute> attrs;
    unsigned numOfBufferPages;          // number of buffer pages for left relation
    unsigned leftAttrNo;                // no of condition attribute in left relation
    unsigned rightAttrNo;               // no of condition attribute in right relation
    AttrType attrType;                  // type of condition attribute

    byte *leftBuffer = nullptr;         // memory buffer for tuples from left relation
    unsigned leftBufferSize = 0;
    void *hashTable = nullptr;          // hash table for tuples in leftBuffer
    vector<unsigned> leftOffsets;
    unsigned leftIdx = 0;
    byte leftTuple[PAGE_SIZE];          // the last tuple from left relation
    unsigned lastLeftTupleLength = 0;   // length of the last left tuple that is not loaded into leftBuffer
    byte rightTuple[PAGE_SIZE];         // current tuple from right relation
};


class INLJoin : public Iterator {
// Index nested-loop join operator
public:
    INLJoin(Iterator *leftIn,           // Iterator of input R
            IndexScan *rightIn,          // IndexScan Iterator of input S
            const Condition &condition   // Join condition
    );

    ~INLJoin();

    RC getNextTuple(void *data);

    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;

private:
    Iterator *leftIn;
    IndexScan *rightIn;
    Condition condition;
    vector<Attribute> leftAttrs;
    vector<Attribute> rightAttrs;
    vector<Attribute> attrs;
    unsigned leftAttrNo;            // no of condition attribute in left relation
    unsigned rightAttrNo;           // no of condition attribute in right relation
    AttrType attrType;              // type of condition attribute

    byte leftTuple[PAGE_SIZE];
    bool isLeftTupleEmpty = true;
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
public:
    GHJoin(Iterator *leftIn,               // Iterator of input R
           Iterator *rightIn,               // Iterator of input S
           const Condition &condition,      // Join condition (CompOp is always EQ)
           const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
    );

    ~GHJoin();

    RC getNextTuple(void *data);

    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;

private:
    static int relNo;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    Iterator *leftIn;
    Iterator *rightIn;
    Condition condition;
    unsigned numOfPartitions;
    unsigned curPartitionNum = 0;

    int leftRelNo = relNo++;
    int rightRelNo = relNo++;
    vector<Attribute> leftAttrs;
    vector<Attribute> rightAttrs;
    vector<string> leftAttrNames;
    vector<string> rightAttrNames;
    vector<Attribute> attrs;
    unsigned leftAttrNo;            // no of condition attribute in left relation
    unsigned rightAttrNo;           // no of condition attribute in right relation
    AttrType attrType;              // type of condition attribute

    RBFM_ScanIterator leftIterator;
    RBFM_ScanIterator rightIterator;
    byte *leftBuffer = nullptr;     // memory buffer for tuples from left relation
    unsigned leftBufferSize = 0;    // size of tuples in leftBuffer
    void *hashTable = nullptr;
    vector<unsigned> leftOffsets;
    unsigned leftIdx = 0;
    byte rightTuple[PAGE_SIZE];         // current tuple from right relation
};

class Aggregate : public Iterator {
    // Aggregation operator
public:
    // Mandatory
    // Basic aggregation
    Aggregate(Iterator *input,          // Iterator of input R
              Attribute aggAttr,        // The attribute over which we are computing an aggregate
              AggregateOp op            // Aggregate operation
    );

    // Optional for everyone: 5 extra-credit points
    // Group-based hash aggregation
    Aggregate(Iterator *input,             // Iterator of input R
              Attribute aggAttr,           // The attribute over which we are computing an aggregate
              Attribute groupAttr,         // The attribute over which we are grouping the tuples
              AggregateOp op              // Aggregate operation
    );

    ~Aggregate();

    RC getNextTuple(void *data);

    // Please name the output attribute as aggregateOp(aggAttr)
    // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
    // output attrname = "MAX(rel.attr)"
    void getAttributes(vector<Attribute> &attrs) const;

private:
    AggregateInfo aggregateInfo;
    Iterator *iter;
    vector<Attribute> attrs;
    vector<Attribute> originalAttrs;
    Attribute aggAttr;
    Attribute groupAttr;
    AggregateOp aggOp;
    void *groupMapPtr = nullptr;
    bool isGroupingRequired;
    bool reachEOF = false;
    bool scanned = false;

    void prepareUnGroupedAttrs();
    void prepareGroupedAttrs();
    string getAggregateOpName(AggregateOp aggOp) const;
    RC getNextUngroupedTuple(void *data);
    RC getNextGroupedTuple(void *data);
    RC prepareUngroupedTuple(void *data);
    RC prepareGroupedTuple(void *groupAttrValuePtr, void *data);
    RC findAggregateIfoFromGroupMap(const byte *key);
    RC putAggreateInfoToGroupMap(const byte *key, const AggregateInfo &value);
    AggregateInfo getAggregateIfoFromGroupMap(const byte *keyPtr);
    RC prepareNextKeyValueFromGroupMap(void *groupAttrValuePtr, AggregateInfo &aggregateInfo);
};

unsigned computeTupleLength(const vector<Attribute> &attrs, const void *tuple);

unsigned getAttributeOffset(const vector<Attribute> &attrs, const byte *tuple, unsigned attrNo);

void clearHashTable(void *hashTable, AttrType type);

void insertToHashTable(void *hashTable, AttrType type, const byte *key, unsigned value);

vector<unsigned> getOffsetsFromHashTable(void *hashTable, AttrType type, const byte *key);

void joinTuples(const vector<Attribute> &leftAttrs, const byte *leftTuple,
                const vector<Attribute> &rightAttrs, const byte *rightTuple,
                void *data);

// used in Grace Hash Join
unsigned getPartitionNum(AttrType type, const byte *key, unsigned numOfPartitions);

// get key(data of target attribute) with judging whether the key is null or not
inline bool
getAttributeData(const void *data, const vector<Attribute> attrs, const Attribute targetAttr, void *attributeData) {
    int offset = getBytesOfNullIndicator(attrs.size());
    const byte *pFlag = (const byte*) data;
    uint8_t flagMask = 0x80;

    for (int i = 0; i < attrs.size(); i++) {
        Attribute currentAttribute = attrs[i];
        if (currentAttribute.name == targetAttr.name) {
            if (*pFlag & flagMask) {
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
            if (!(*pFlag & flagMask)) {
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
            if (flagMask == 0x01) {
                flagMask = 0x80;
                ++pFlag;
            } else {
                flagMask = flagMask >> 1;
            }
        }
    }
    cerr << "Didn't find corresponding attribute, which should be impossible!" << endl;
    return false;
}

#endif
