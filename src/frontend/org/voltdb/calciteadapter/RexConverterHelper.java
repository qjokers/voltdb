/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.calciteadapter;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.hsqldb_voltpatches.FunctionSQL;
import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.hsqldb_voltpatches.FunctionForVoltDB.FunctionDescriptor;
import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.InComparisonExpression;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.VectorValueExpression;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.QuantifierType;

public class RexConverterHelper {

    public static AbstractExpression createFunctionExpression(
            RelDataType relDataType,
            String funcName,
            List<AbstractExpression> operands,
            String impliedArg) {
        FunctionDescriptor functionId = FunctionForVoltDB.newVoltDBFunctionId(funcName);
        if (functionId == null) {
            throw new CalcitePlanningException("Unsupported function:" + funcName);
        }
        return createFunctionExpression(relDataType, funcName, functionId.getId(), operands, impliedArg);
    }

    public static AbstractExpression createFunctionExpression(
            RelDataType relDataType,
            String funcName,
            int funcId,
            List<AbstractExpression> operands,
            String impliedArg) {
        FunctionExpression fe = new FunctionExpression();
        fe.setAttributes(funcName, impliedArg, funcId);
        fe.setArgs(operands);
        TypeConverter.setType(fe, relDataType);
        return fe;
    }

    public static AbstractExpression createFunctionExpression(
            VoltType voltType,
            String funcName,
            int funcId,
            List<AbstractExpression> operands,
            String impliedArg) {
        FunctionExpression fe = new FunctionExpression();
        fe.setAttributes(funcName, impliedArg, funcId);
        fe.setArgs(operands);
        TypeConverter.setType(fe, voltType, voltType.getMaxLengthInBytes());
        return fe;
    }

    public static AbstractExpression createToTimestampFunctionExpression(
            RelDataType relDataType,
            ExpressionType intervalOperatorType,
            List<AbstractExpression> aeOperands) {
        // There must be two operands
        assert(2 == aeOperands.size());
        // One of them is timestamp and another one is interval (BIGINT) in microseconds
        AbstractExpression timestamp = null;
        AbstractExpression interval = null;
        if (aeOperands.get(0).getValueType() == VoltType.TIMESTAMP) {
            timestamp = aeOperands.get(0);
        } else if (aeOperands.get(0).getValueType() == VoltType.BIGINT) {
            interval = aeOperands.get(0);
        }
        if (aeOperands.get(1).getValueType() == VoltType.TIMESTAMP) {
            timestamp = aeOperands.get(1);
        } else if (aeOperands.get(1).getValueType() == VoltType.BIGINT) {
            interval = aeOperands.get(1);
        }
        if (timestamp == null || interval == null) {
            throw new CalcitePlanningException("Invalid arguments for VoltDB TO_TIMESTAMP function");
        }

        // SINCE_EPOCH
        List<AbstractExpression> epochOperands = new ArrayList<>();
        epochOperands.add(timestamp);
        String impliedArgMicrosecond = "MICROSECOND";
        AbstractExpression sinceEpochExpr = createFunctionExpression(
                VoltType.BIGINT,
                "since_epoch",
                FunctionSQL.voltGetSinceEpochId(impliedArgMicrosecond),
                epochOperands,
                impliedArgMicrosecond);

        // Plus/Minus interval
        AbstractExpression plusExpr = new OperatorExpression(intervalOperatorType, sinceEpochExpr, interval);

        // TO_TIMESTAMP
        List<AbstractExpression> timestampOperands = new ArrayList<>();
        timestampOperands.add(plusExpr);
        AbstractExpression timestampExpr = createFunctionExpression(
                relDataType,
                "to_timestamp",
                FunctionSQL.voltGetToTimestampId(impliedArgMicrosecond),
                timestampOperands,
                impliedArgMicrosecond);

        return timestampExpr;
    }

    public static AbstractExpression createInComparisonExpression(
            List<AbstractExpression> aeOperands) {
        //
        assert (aeOperands.size() > 0);
        // The left expression should be the same for all operands because it is IN expression
        AbstractExpression leftInExpr = aeOperands.get(0).getLeft();
        assert(leftInExpr != null);
        AbstractExpression rightInExpr = new VectorValueExpression();
        List<AbstractExpression> inArgs = new ArrayList<>();
        for (AbstractExpression expr : aeOperands) {
            assert(expr.getRight() != null);
            inArgs.add(expr.getRight());
        }
        rightInExpr.setArgs(inArgs);

        ComparisonExpression inExpr = new InComparisonExpression();
        inExpr.setLeft(leftInExpr);
        inExpr.setRight(rightInExpr);
        inExpr.setQuantifier(QuantifierType.ANY);
        return inExpr;
    }
}
