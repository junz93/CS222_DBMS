
#include "qe.h"

/**  Filter Related Functions  **/

RC Filter::getNextTuple(void *data) {
    Value lhsValue;

    if (condition.bRhsIsAttr == true) {
        return QE_EOF;
    }

    do {
        if (iter->getNextTuple(data) == QE_EOF) {
            return QE_EOF;
        }
//        RC rc = getLhsValue(attrs, condition.lhsAttr, data, lhsValue);
//        if (rc == FAIL) {
//            return QE_EOF;
//        }
        getLhsValue(attrs, condition.lhsAttr, data, lhsValue);
    } while (!isQulifiedTuple(lhsValue, condition.op, condition.rhsValue));

    return SUCCESS;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    attrs = this->attrs;
}

RC Filter::getLhsValue(const vector<Attribute> attrs, const string attrName, const void *data, Value &value) {
    int offset = getByteOfNullsIndicator(attrs.size());
    for (Attribute attr : attrs) {
        if (attr.name == attrName) {
            value.type = attr.type;
            switch (attr.type) {
                case TypeInt:
                case TypeReal:
                    memcpy(value.data, (char *) data + offset, 4);
                    break;
                case TypeVarChar:
                    uint32_t length = *((uint32_t *) ((char *) data + offset));
                    memcpy(value.data, (char *) data + offset, 4 + length);
                    break;
            }
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

bool Filter::isQulifiedTuple(const Value lhsValue, const CompOp op, const Value rhsValue) {
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


