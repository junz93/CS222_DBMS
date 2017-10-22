#ifndef CS222_UTIL_H
#define CS222_UTIL_H

#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <stdexcept>
#include <stdio.h>
#include <math.h>

using namespace std;

// Calculate actual bytes for nulls-indicator for the given field counts
int getByteOfNullsIndicator(int fieldCount) {
    return ceil((double) fieldCount / CHAR_BIT);
}

// prepare tuple that would be written to "Tables" table
void prepareTupleForTables(int attributeCount, const int tableID, const string &name,
                           const int isSystemInfo, void *tuple) {
    int offset = 0;
    int nullAttributesIndicatorActualSize = getByteOfNullsIndicator(attributeCount);
    int nameLength = name.size();

    // write Null-indicator to tuple record
    memset((char *) tuple + offset, 0, nullAttributesIndicatorActualSize);
    offset += nullAttributesIndicatorActualSize;

    // write tableID to tuple record
    memcpy((char *) tuple + offset, &tableID, sizeof(int));
    offset += sizeof(int);

    // write tableName to tuple record
    memcpy((char *) tuple + offset, &nameLength, sizeof(uint32_t));
    offset += sizeof(int);
    memcpy((char *) tuple + offset, name.c_str(), nameLength);
    offset += nameLength;

    // write fileName to tuple record
    memcpy((char *) tuple + offset, &nameLength, sizeof(uint32_t));
    offset += sizeof(int);
    memcpy((char *) tuple + offset, name.c_str(), nameLength);
    offset += nameLength;

    // write isSystemInfo to tuple record
    memcpy((char *) tuple + offset, &isSystemInfo, sizeof(int));
    offset += sizeof(int);
}

// prepare tuple that would be written to "Columns" table
void prepareTupleForColumns(int attributeCount, const int tableID, const string &columnName, const int columnType,
                            const int columnLength, const int columnPosition, const int isSystemInfo, void *tuple) {
    int offset = 0;
    int nullAttributesIndicatorActualSize = getByteOfNullsIndicator(attributeCount);
    int nameLength = columnName.size();

    // write Null-indicator to tuple record
    memset((char *) tuple + offset, 0, nullAttributesIndicatorActualSize);
    offset += nullAttributesIndicatorActualSize;

    // write tableID to tuple record
    memcpy((char *) tuple + offset, &tableID, sizeof(int));
    offset += sizeof(int);

    // write columnName to tuple record
    memcpy((char *) tuple + offset, &nameLength, sizeof(uint32_t));
    offset += sizeof(int);
    memcpy((char *) tuple + offset, columnName.c_str(), nameLength);
    offset += nameLength;

    // write columnType to tuple record
    memcpy((char *) tuple + offset, &columnType, sizeof(int));
    offset += sizeof(int);

    // write columnLength to tuple record
    memcpy((char *) tuple + offset, &columnLength, sizeof(int));
    offset += sizeof(int);

    // write columnPosition to tuple record
    memcpy((char *) tuple + offset, &columnPosition, sizeof(int));
    offset += sizeof(int);

    // write isSystemInfo to tuple record
    memcpy((char *) tuple + offset, &isSystemInfo, sizeof(int));
    offset += sizeof(int);
}


#endif //CS222_UTIL_H
